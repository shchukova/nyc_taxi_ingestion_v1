#!/usr/bin/env python3
"""
Test runner for SnowflakeLoader tests
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_tests(test_type="all", verbose=False, coverage=False):
    """
    Run SnowflakeLoader tests
    
    Args:
        test_type: Type of tests to run ("all", "unit", "integration", "edge_cases")
        verbose: Enable verbose output
        coverage: Generate coverage report
    """
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test files based on type
    test_files = {
        "unit": ["tests/unit/test_snowflake_loader.py"],
        "integration": ["tests/unit/test_snowflake_loader_integration.py"],
        "edge_cases": ["tests/unit/test_snowflake_loader_edge_cases.py"],
        "all": [
            "tests/unit/test_snowflake_loader.py",
            "tests/unit/test_snowflake_loader_integration.py", 
            "tests/unit/test_snowflake_loader_edge_cases.py"
        ]
    }
    
    if test_type not in test_files:
        print(f"Invalid test type: {test_type}")
        print(f"Available types: {', '.join(test_files.keys())}")
        return 1
    
    cmd.extend(test_files[test_type])
    
    # Add options
    if verbose:
        cmd.append("-v")
    
    if coverage:
        cmd.extend([
            "--cov=src.loaders.snowflake_loader",
            "--cov-report=html",
            "--cov-report=term-missing"
        ])
    
    # Add additional useful options
    cmd.extend([
        "--tb=short",  # Shorter traceback format
        "--strict-markers",  # Strict marker checking
        "-ra"  # Show all test outcome summaries
    ])
    
    print(f"Running command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        return 130
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1


def main():
    parser = argparse.ArgumentParser(description="Run SnowflakeLoader tests")
    parser.add_argument(
        "--type", "-t",
        choices=["all", "unit", "integration", "edge_cases"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Generate coverage report"
    )
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path("src/loaders/snowflake_loader.py").exists():
        print("Error: snowflake_loader.py not found. Run this script from the project root.")
        return 1
    
    return run_tests(args.type, args.verbose, args.coverage)


if __name__ == "__main__":
    sys.exit(main())