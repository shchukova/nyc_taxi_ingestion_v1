# Run all tests script
# tests/run_tlc_tests.py
"""
Script to run all TLC Data Source tests with detailed reporting.
"""

import subprocess
import sys
from pathlib import Path


def run_test_suite():
    """Run all TLC data source tests."""
    
    print("🚕 NYC TLC Data Source Test Suite")
    print("=" * 50)
    
    test_files = [
        ("TLC Data File Tests", "tests/unit/test_tlc_data_file.py"),
        ("TLC Data Source Basic", "tests/unit/test_tlc_data_source_basic.py"),
        ("Date Validation", "tests/unit/test_tlc_data_source_date_validation.py"),
        ("File Operations", "tests/unit/test_tlc_data_source_file_operations.py"),
        ("Schema Validation", "tests/unit/test_tlc_data_source_schema_validation.py"),
        ("Estimates & Processing", "tests/unit/test_tlc_data_source_estimates.py"),
        ("Integration Tests", "tests/unit/test_tlc_data_source_integration.py"),
        ("Edge Cases", "tests/unit/test_tlc_data_source_edge_cases.py")
    ]
    
    results = []
    
    for test_name, test_file in test_files:
        print(f"\n🔍 Running: {test_name}")
        print(f"File: {test_file}")
        print("-" * 40)
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", test_file, "-v"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            if result.returncode == 0:
                print(f"✅ {test_name} - ALL PASSED")
                results.append((test_name, True, ""))
            else:
                print(f"❌ {test_name} - SOME FAILED")
                results.append((test_name, False, result.stdout + result.stderr))
                
        except Exception as e:
            print(f"💥 {test_name} - ERROR: {e}")
            results.append((test_name, False, str(e)))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    
    for test_name, success, error in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
        if not success and error:
            print(f"   Error: {error[:100]}...")
    
    print(f"\nResults: {passed}/{total} test suites passed")
    
    if passed == total:
        print("\n🎉 ALL TLC DATA SOURCE TESTS PASSED!")
        print("\nYour TLC data source classes are working correctly:")
        print("✅ TLCDataFile - dataclass with properties")
        print("✅ URL generation - correct formatting")
        print("✅ Date validation - handles boundaries and TLC delays")
        print("✅ File operations - date ranges and recent files")
        print("✅ Schema validation - yellow and green taxi schemas")
        print("✅ Processing estimates - realistic time calculations")
        print("✅ Integration - all components work together")
        print("✅ Edge cases - boundary conditions handled")
        
        return True
    else:
        print(f"\n❌ {total - passed} test suite(s) failed")
        print("\nCheck the errors above and fix the issues.")
        return False


if __name__ == "__main__":
    success = run_test_suite()
    sys.exit(0 if success else 1)