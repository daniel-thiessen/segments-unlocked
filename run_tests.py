#!/usr/bin/env python
"""
Test runner script for segments-unlocked.
"""
import unittest
import sys
import os

if __name__ == "__main__":
    # Add the parent directory to the Python path
    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
    
    # Discover and run all tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(os.path.join(os.path.dirname(__file__), 'tests'))
    
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Exit with non-zero code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)
