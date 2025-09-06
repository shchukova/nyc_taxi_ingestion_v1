# Create a test runner for all FileExtractor tests
# tests/run_file_extractor_tests.py
"""
Test runner for FileExtractor tests.
"""

import subprocess
import sys
from pathlib import Path


def main():
    """Run all FileExtractor tests."""
    
    print("🔧 FileExtractor Test Suite")
    print("=" * 50)
    
    test_files = [
        "test_file_extractor_basic.py",
        "test_file_extractor_download.py", 
        "test_file_extractor_download_with_progress.py",
        "test_file_extractor_validation.py",
        "test_file_extractor_metadata.py",
        "test_file_extractor_utilities.py",
        "test_file_extractor_context_manager.py",
        "test_file_extractor_integration.py",
        "test_file_extractor_edge_cases.py"
    ]
    
    all_passed = True
    
    for test_file in test_files:
        print(f"\n🔍 Running: {test_file}")
        print("-" * 40)
        
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            f"tests/unit/{test_file}", "-v"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {test_file} - PASSED")
        else:
            print(f"❌ {test_file} - FAILED")
            print("STDOUT:", result.stdout[-500:])  # Last 500 chars
            print("STDERR:", result.stderr[-500:])
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 ALL FILE EXTRACTOR TESTS PASSED!")
        print("\nYour FileExtractor class is working correctly:")
        print("✅ Basic initialization and configuration")
        print("✅ HTTP session setup with retry strategy")
        print("✅ File download with progress tracking")
        print("✅ File validation and integrity checks")
        print("✅ Metadata extraction and MD5 calculation")
        print("✅ Utility functions and cleanup")
        print("✅ Context manager support")
        print("✅ Error handling and edge cases")
        print("✅ Integration scenarios")
    else:
        print("❌ Some tests failed. Check the output above.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)