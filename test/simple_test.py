import requests
import json
import time
import argparse
import sys
from typing import Dict, Any, Callable

# --- Color constants for terminal output ---
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

# --- Configuration (BASE_URL will be set by main function) ---
BASE_URL = "http://127.0.0.1:8000"

class TestRunner:
    """
    Test runner class to manage test execution and results tracking
    """
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total_tests = 0
    
    def print_header(self, title: str):
        """Print a formatted header for test sections"""
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{title:^70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}\n")
    
    def print_section(self, title: str):
        """Print a formatted section header"""
        print(f"\n{Colors.BOLD}{Colors.CYAN}► {title}{Colors.RESET}")
        print(f"{Colors.CYAN}{'-' * 70}{Colors.RESET}")
    
    def assert_test(self, condition: bool, test_name: str, details: str = ""):
        """
        Evaluate a test condition and track results
        """
        self.total_tests += 1
        if condition:
            self.passed += 1
            print(f"{Colors.GREEN}✓ PASS{Colors.RESET} | {test_name}")
            if details:
                print(f"  {Colors.CYAN}→ {details}{Colors.RESET}")
        else:
            self.failed += 1
            print(f"{Colors.RED}✗ FAIL{Colors.RESET} | {test_name}")
            if details:
                print(f"  {Colors.RED}→ {details}{Colors.RESET}")
    
    def print_summary(self):
        """Print final test results summary"""
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"{Colors.BOLD}Test Summary{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"Total Tests: {self.total_tests}")
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.RESET}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.RESET}")
        
        if self.failed == 0:
            print(f"\n{Colors.GREEN}{Colors.BOLD} All tests passed!{Colors.RESET}\n")
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}⚠ Some tests failed!{Colors.RESET}\n")

def check_system_health():
    """Check if the storage system is running and healthy"""
    print(f"{Colors.YELLOW}Checking system health (Target: {BASE_URL})...{Colors.RESET}")
    try:
        # Call the /health endpoint from api/main.go
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"{Colors.GREEN}✓ System is healthy (API Gateway Hostname: {response.json().get('hostname')}){Colors.RESET}")
            return True
        else:
            print(f"{Colors.RED}✗ System status abnormal ({response.status_code}){Colors.RESET}")
            return False
    except Exception as e:
        print(f"{Colors.RED}✗ Cannot connect to system: {e}{Colors.RESET}")
        return False

def cleanup_test_keys(runner: TestRunner):
    """Clean up test keys before starting tests to ensure clean environment"""
    runner.print_section("Environment Cleanup (Pre-Test Cleanup)")
    keys_to_clean = ["test_replication", "test_ec", "invalid_key"]
    
    for key in keys_to_clean:
        try:
            requests.delete(f"{BASE_URL}/delete/{key}", timeout=2)
        except Exception:
            pass 
    print(f"{Colors.CYAN}✓ Cleanup completed{Colors.RESET}")

def perform_storage_usage_check(runner: TestRunner, test_name: str, assertion_details: str, condition_func: Callable[[int], bool]):
    try:
        response = requests.get(f"{BASE_URL}/storage_usage")
        data = response.json()
        total_size = data.get('total_system_size', 0)
        
        condition = condition_func(total_size)
        details = f"Total storage: {total_size} bytes ({assertion_details})"
        
        runner.assert_test(
            response.status_code == 200 and condition,
            test_name,
            details
        )
    except Exception as e:
        runner.assert_test(False, test_name, f"Error: {e}")

def test_replication_strategy(runner: TestRunner):
    """Test Strategy A: Replication"""
    runner.print_section("Strategy A: Replication")
    
    key = "test_replication"
    value = {"data": "Hello, Replication Strategy!"}
    
    # 1. Write test
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": key, "strategy": "replication"},
            json=value 
        )
        runner.assert_test(
            response.status_code == 200,
            "Write data (JSON object)",
            f"Nodes written: {len(response.json().get('nodes_written', []))}"
        )
    except Exception as e:
        runner.assert_test(False, "Write data (JSON object)", f"Error: {e}")
    
    # 2. Read test
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        data_json = response.json() 
        runner.assert_test(
            response.status_code == 200 and data_json == value,
            "Read data (JSON object)",
            f"Data matches: {data_json == value}"
        )
    except Exception as e:
        runner.assert_test(False, "Read data (JSON object)", f"Error: {e}")
    
    # 3. Delete test
    try:
        response = requests.delete(f"{BASE_URL}/delete/{key}")
        runner.assert_test(
            response.status_code == 200,
            "Delete data",
            f"Deletion strategy: {response.json().get('strategy', 'N/A')}"
        )
    except Exception as e:
        runner.assert_test(False, "Delete data", f"Error: {e}")
    
    # 4. Verify deletion
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        runner.assert_test(
            response.status_code == 404,
            "Verify deletion (should be 404)",
            f"Data successfully deleted (received {response.status_code})"
        )
    except Exception as e:
        runner.assert_test(False, "Verify deletion (should be 404)", f"Error: {e}")

def test_ec_strategy(runner: TestRunner):
    """Test Strategy B: Erasure Coding"""
    runner.print_section("Strategy B: Erasure Coding")
    
    key = "test_ec"
    value = {"data": "EC Strategy Test Data! " * 100}
    
    # 1. Write test
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": key, "strategy": "ec"},
            json=value
        )
        data = response.json()
        runner.assert_test(
            response.status_code == 200,
            "Write data (EC)",
            f"Chunks: {data.get('chunks_written')}/{data.get('total_chunks')}"
        )
    except Exception as e:
        runner.assert_test(False, "Write data (EC)", f"Error: {e}")
    
    # 2. Read test
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        data_json = response.json()
        runner.assert_test(
            response.status_code == 200 and data_json == value,
            "Read data (EC)",
            "Data integrity: ✓"
        )
    except Exception as e:
        runner.assert_test(False, "Read data (EC)", f"Error: {e}")

    # 3. Delete test
    try:
        response = requests.delete(f"{BASE_URL}/delete/{key}")
        runner.assert_test(
            response.status_code == 200,
            "Delete data (EC)",
            f"Deletion strategy: {response.json().get('strategy', 'N/A')}"
        )
    except Exception as e:
        runner.assert_test(False, "Delete data (EC)", f"Error: {e}")

def test_system_monitoring(runner: TestRunner):
    """Test monitoring features"""
    runner.print_section("System Monitoring and Cleanup Verification")
    
    # 1. Node status check
    try:
        response = requests.get(f"{BASE_URL}/node_status")
        data = response.json()
        expected_nodes = 6 
        healthy_nodes = sum(1 for status in data.values() if status == "healthy")
        
        runner.assert_test(
            response.status_code == 200 and healthy_nodes == expected_nodes,
            "Node status check",
            f"Healthy nodes: {healthy_nodes}/{expected_nodes}"
        )
    except Exception as e:
        runner.assert_test(False, "Node status check", f"Error: {e}")
    
    # 2. Verify all data has been deleted
    perform_storage_usage_check(
        runner, 
        "Storage usage check (after deletion)", 
        "Should be == 0", 
        lambda size: size == 0
    )

def test_invalid_inputs(runner: TestRunner):
    """Test invalid inputs and expected errors"""
    runner.print_section("Negative Testing (Error Handling)")
    
    # 1. Read a key that definitely doesn't exist
    try:
        key = "key_that_never_existed"
        response = requests.get(f"{BASE_URL}/read/{key}")
        runner.assert_test(
            response.status_code == 404,
            "Read non-existent key (should be 404)",
            f"Received status: {response.status_code}"
        )
    except Exception as e:
        runner.assert_test(False, "Read non-existent key", f"Error: {e}")
        
    # 2. Test "invalid strategy"
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": "invalid_key", "strategy": "invalid_strategy_name"},
            json={"data": "test"}
        )
        runner.assert_test(
            response.status_code == 422,
            "Use invalid strategy (should be 422)",
            f"Received status: {response.status_code}"
        )
    except Exception as e:
        runner.assert_test(False, "Use invalid strategy", f"Error: {e}")

    # 3. Test deprecated field_hybrid strategy
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": "invalid_key", "strategy": "field_hybrid"},
            json={"data": "test"}
        )
        runner.assert_test(
            response.status_code == 422,
            "Use deprecated field_hybrid (should be 422)",
            f"Received status: {response.status_code}"
        )
    except Exception as e:
        runner.assert_test(False, "Use deprecated field_hybrid", f"Error: {e}")
    
    # 4. Test "invalid JSON Body" (sending raw string instead of JSON)
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": "invalid_key", "strategy": "replication"},
            data="This is not a JSON object"
        )
        runner.assert_test(
            response.status_code == 415, 
            "Use invalid Content-Type (should be 415)",
            f"Received status: {response.status_code}"
        )
    except Exception as e:
        runner.assert_test(False, "Use invalid Content-Type", f"Error: {e}")

def main():
    """Main test function"""
    global BASE_URL
    
    parser = argparse.ArgumentParser(description="Distributed Storage System - Functional Test Script")
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://127.0.0.1:8000",
        help="Specify the API base URL of the storage system"
    )
    args = parser.parse_args()
    BASE_URL = args.base_url 
    
    runner = TestRunner()
    
    runner.print_header(f"Distributed Storage System - Functional Tests\nTarget: {BASE_URL}")
    
    if not check_system_health():
        print(f"\n{Colors.RED}Error: System not running, please start your server first{Colors.RESET}\n")
        sys.exit(1)
    
    cleanup_test_keys(runner)
    
    print(f"\n{Colors.GREEN}Starting main test execution...{Colors.RESET}")
    
    test_replication_strategy(runner)
    test_ec_strategy(runner)
    test_system_monitoring(runner)
    test_invalid_inputs(runner)
    
    runner.print_summary()
    
    if runner.failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
