import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from typing import Dict, Any
import structlog

logger = structlog.get_logger()

class DataQualityValidator:
    def __init__(self, data_context_path: str = "/opt/great_expectations"):
        self.context = gx.get_context(context_root_dir=data_context_path)
        
    def create_customer_events_suite(self) -> ExpectationSuite:
        suite = self.context.add_or_update_expectation_suite("customer_events_suite")
        
        expectations = [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "customer_id"}
            },
            {
                "expectation_type": "expect_column_to_exist", 
                "kwargs": {"column": "timestamp"}
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "event_type"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "timestamp"}
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "event_type",
                    "value_set": ["purchase", "page_view", "support_ticket", "login", "logout"]
                }
            },
            {
                "expectation_type": "expect_column_value_lengths_to_be_between",
                "kwargs": {
                    "column": "customer_id",
                    "min_value": 1,
                    "max_value": 50
                }
            }
        ]
        
        for exp_config in expectations:
            expectation = ExpectationConfiguration(**exp_config)
            suite.add_expectation(expectation)
        
        return suite
    
    def validate_dataframe(self, df, suite_name: str) -> Dict[str, Any]:
        try:
            suite = self.context.get_expectation_suite(suite_name)
            validator = self.context.get_validator(
                batch_request={
                    "datasource_name": "pandas_datasource",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "validation_data"
                },
                expectation_suite=suite
            )
            
            validation_result = validator.validate(df)
            
            logger.info("Data validation completed", 
                       success=validation_result.success,
                       statistics=validation_result.statistics)
            
            return {
                "success": validation_result.success,
                "statistics": validation_result.statistics,
                "results": validation_result.results
            }
            
        except Exception as e:
            logger.error("Data validation failed", error=str(e))
            return {"success": False, "error": str(e)}
