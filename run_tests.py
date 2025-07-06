"""
Script to run the tests for the Spark + DBT + Delta Lake pipeline.
"""

import unittest
import sys
import os

def main():
    """Run the tests."""
    # Add the project root to the Python path
    sys.path.insert(0, os.getcwd())
    
    # Discover and run tests
    test_suite = unittest.defaultTestLoader.discover('tests')
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Return non-zero exit code if tests failed
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    sys.exit(main())