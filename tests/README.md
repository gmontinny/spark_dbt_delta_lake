# Tests for Spark + DBT + Delta Lake Pipeline

This directory contains tests for the Spark + DBT + Delta Lake pipeline. The tests verify that the application is working correctly after the recent fixes to address Java gateway issues and Delta Lake compatibility.

## Test Structure

The tests are organized as follows:

- `test_pipeline.py`: Contains unit tests for the pipeline components and integration tests for the full pipeline.

## Test Cases

The following test cases are included:

1. **Spark Session Creation Test**: Verifies that a Spark session can be created successfully with the correct configuration for Delta Lake.

2. **Pipeline Execution Test**: Tests the individual components of the pipeline (ingestion and transformation) and verifies that the data is processed correctly.

3. **Full Pipeline Test**: Tests the full pipeline execution from start to finish.

## Running the Tests

To run the tests, execute the following command from the project root directory:

```bash
python run_tests.py
```

This will discover and run all tests in the `tests` directory.

## Expected Results

When the tests run successfully, you should see output similar to the following:

```
test_full_pipeline (tests.test_pipeline.TestPipeline) ... ok
test_pipeline_execution (tests.test_pipeline.TestPipeline) ... ok
test_spark_session_creation (tests.test_pipeline.TestPipeline) ... ok

----------------------------------------------------------------------
Ran 3 tests in XX.XXXs

OK
```

If any tests fail, detailed error messages will be displayed to help diagnose the issue.

## Troubleshooting

If you encounter issues running the tests:

1. Make sure all dependencies are installed:
   ```bash
   pip install -r requirements.txt
   ```

2. Check the log files in the `logs` directory for detailed error messages.

3. Verify that your Java and Spark versions are compatible with Delta Lake 3.2.0.