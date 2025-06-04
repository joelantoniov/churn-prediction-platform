#! /usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from src.streaming.spark_streaming import ChurnStreamingProcessor
from src.data_ingestion.kafka_consumer import CustomerEventConsumer
from src.utils.logging_config import setup_logging
from src.utils.health_check import HealthChecker
import structlog

setup_logging()
logger = structlog.get_logger()

class ChurnPredictionApp:
    def __init__(self):
        self.streaming_processor = ChurnStreamingProcessor()
        self.health_checker = HealthChecker()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.running = True

    async def start(self):
        logger.info("Starting Churn Prediction Platform")

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        tasks = [
            self._run_streaming_processor(),
            self._run_health_checker(),
            self._run_metrics_collector()
        ]

        await asyncio.gather(*tasks)

    async def _run_streaming_processor(self):
        try:
            events_stream = self.streaming_processor.create_kafka_stream()
            predictions_stream = self.streaming_processor.process_churn_predictions(events_stream)

            await self.streaming_processor.write_to_delta(
                predictions_stream,
                "/data/churn_predictions",
                "/checkpoints/predictions"
            ).awaitTermination()
        except Exception as e:
            logger.error("Streaming processor failed", error=str(e))
            raise

    async def _run_health_checker(self):
        while self.running:
            await asyncio.sleep(30)
            health_status = await self.health_checker.check_all_systems()
            if not health_status['healthy']:
                logger.warning("System health check failed", status=health_status)
    
    async def _run_metrics_collector(self):
        from src.monitoring.metrics_collector import MetricsCollector
        metrics_collector = MetricsCollector()
        
        while self.running:
            await asyncio.sleep(60)
            await metrics_collector.collect_and_export_metrics()
    
    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        self.running = False
        self.streaming_processor.spark.stop()
        self.executor.shutdown(wait=True)
        sys.exit(0)

if __name__ == "__main__":
    app = ChurnPredictionApp()
    asyncio.run(app.start())
