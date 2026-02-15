#!/usr/bin/env python3
"""
NiftyAlgo Trading Bot - Backend API Testing
Tests all backend endpoints for the options trading bot
"""

import requests
import sys
import json
from datetime import datetime
from typing import Dict, Any

class NiftyAlgoAPITester:
    def __init__(self, base_url="https://market-bot-api.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []

    def log_test(self, name: str, success: bool, details: str = "", response_data: Any = None):
        """Log test result"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
        
        result = {
            "test_name": name,
            "success": success,
            "details": details,
            "response_data": response_data,
            "timestamp": datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {name}")
        if details:
            print(f"    Details: {details}")
        if not success and response_data:
            print(f"    Response: {response_data}")
        print()

    def test_api_endpoint(self, method: str, endpoint: str, expected_status: int = 200, 
                         data: Dict = None, description: str = "") -> tuple:
        """Test a single API endpoint"""
        url = f"{self.api_url}/{endpoint}"
        headers = {'Content-Type': 'application/json'}
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=10)
            elif method.upper() == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=10)
            else:
                return False, f"Unsupported method: {method}", None

            success = response.status_code == expected_status
            
            if success:
                try:
                    response_data = response.json()
                except:
                    response_data = response.text
            else:
                response_data = {
                    "status_code": response.status_code,
                    "text": response.text[:200] + "..." if len(response.text) > 200 else response.text
                }
            
            details = f"Status: {response.status_code} (expected {expected_status})"
            if description:
                details = f"{description} - {details}"
                
            return success, details, response_data

        except requests.exceptions.RequestException as e:
            return False, f"Request failed: {str(e)}", None
        except Exception as e:
            return False, f"Unexpected error: {str(e)}", None

    def test_status_endpoint(self):
        """Test GET /api/status"""
        success, details, data = self.test_api_endpoint(
            'GET', 'status', 200, 
            description="Bot status endpoint"
        )
        
        if success and data:
            required_fields = ['is_running', 'mode', 'market_status', 'connection_status', 'selected_index', 'candle_interval']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                success = False
                details += f" - Missing fields: {missing_fields}"
        
        self.log_test("GET /api/status", success, details, data)
        return success

    def test_config_endpoint(self):
        """Test GET /api/config - Should include target_points field"""
        success, details, data = self.test_api_endpoint(
            'GET', 'config', 200,
            description="Configuration endpoint"
        )
        
        if success and data:
            required_fields = ['order_qty', 'max_trades_per_day', 'daily_max_loss', 'has_credentials', 'selected_index', 'candle_interval', 'lot_size', 'strike_interval', 'target_points']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                success = False
                details += f" - Missing fields: {missing_fields}"
            
            # Specifically check target_points field
            if 'target_points' not in data:
                success = False
                details += " - target_points field is missing"
        
        self.log_test("GET /api/config", success, details, data)
        return success

    def test_target_points_update(self):
        """Test POST /api/config/update - Test updating target_points to 25"""
        test_config = {"target_points": 25}
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Update target_points to 25"
        )
        self.log_test("POST /api/config/update (target_points=25)", success, details, data)
        
        if success:
            # Verify config shows updated target_points
            success, details, config_data = self.test_api_endpoint(
                'GET', 'config', 200,
                description="Verify target_points updated to 25"
            )
            if success and config_data:
                if config_data.get('target_points') != 25:
                    success = False
                    details += f" - Expected target_points=25, got {config_data.get('target_points')}"
            
            self.log_test("Verify target_points=25", success, details, config_data)
        
        return success

    def test_market_nifty_endpoint(self):
        """Test GET /api/market/nifty"""
        success, details, data = self.test_api_endpoint(
            'GET', 'market/nifty', 200,
            description="Market data endpoint"
        )
        
        if success and data:
            required_fields = ['ltp', 'mds_score', 'mds_direction', 'selected_index']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                success = False
                details += f" - Missing fields: {missing_fields}"
        
        self.log_test("GET /api/market/nifty", success, details, data)
        return success

    def test_position_endpoint(self):
        """Test GET /api/position"""
        success, details, data = self.test_api_endpoint(
            'GET', 'position', 200,
            description="Position endpoint"
        )
        
        if success and data:
            # Should have has_position field
            if 'has_position' not in data:
                success = False
                details += " - Missing 'has_position' field"
        
        self.log_test("GET /api/position", success, details, data)
        return success

    def test_trades_endpoint(self):
        """Test GET /api/trades"""
        success, details, data = self.test_api_endpoint(
            'GET', 'trades', 200,
            description="Trades endpoint"
        )
        
        if success and data:
            if not isinstance(data, list):
                success = False
                details += " - Response should be a list"
        
        self.log_test("GET /api/trades", success, details, data)
        return success

    def test_summary_endpoint(self):
        """Test GET /api/summary"""
        success, details, data = self.test_api_endpoint(
            'GET', 'summary', 200,
            description="Daily summary endpoint"
        )
        
        if success and data:
            required_fields = ['total_trades', 'total_pnl', 'max_drawdown', 'daily_stop_triggered']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                success = False
                details += f" - Missing fields: {missing_fields}"
        
        self.log_test("GET /api/summary", success, details, data)
        return success

    def test_logs_endpoint(self):
        """Test GET /api/logs"""
        success, details, data = self.test_api_endpoint(
            'GET', 'logs', 200,
            description="Logs endpoint"
        )
        
        if success and data:
            if not isinstance(data, list):
                success = False
                details += " - Response should be a list"
        
        self.log_test("GET /api/logs", success, details, data)
        return success

    def test_bot_control_endpoints(self):
        """Test bot control endpoints (start/stop/squareoff)"""
        # Test start bot
        success, details, data = self.test_api_endpoint(
            'POST', 'bot/start', 200,
            description="Start bot endpoint"
        )
        self.log_test("POST /api/bot/start", success, details, data)
        
        # Test stop bot
        success, details, data = self.test_api_endpoint(
            'POST', 'bot/stop', 200,
            description="Stop bot endpoint"
        )
        self.log_test("POST /api/bot/stop", success, details, data)
        
        # Test square off (might fail if no position)
        success, details, data = self.test_api_endpoint(
            'POST', 'bot/squareoff', 200,
            description="Square off endpoint"
        )
        # Don't fail the test if no position exists
        if not success and data and "No open position" in str(data):
            success = True
            details = "No position to square off (expected)"
        
        self.log_test("POST /api/bot/squareoff", success, details, data)

    def test_mode_endpoint(self):
        """Test POST /api/config/mode"""
        # Test paper mode
        success, details, data = self.test_api_endpoint(
            'POST', 'config/mode?mode=paper', 200,
            description="Set paper mode"
        )
        self.log_test("POST /api/config/mode (paper)", success, details, data)
        
        # Test live mode
        success, details, data = self.test_api_endpoint(
            'POST', 'config/mode?mode=live', 200,
            description="Set live mode"
        )
        self.log_test("POST /api/config/mode (live)", success, details, data)

    def test_indices_endpoint(self):
        """Test GET /api/indices - Verify correct lot sizes and expiry info"""
        success, details, data = self.test_api_endpoint(
            'GET', 'indices', 200,
            description="Available indices endpoint"
        )
        
        if success and data:
            if not isinstance(data, list):
                success = False
                details += " - Response should be a list"
            else:
                # Expected indices with correct lot sizes and expiry info
                expected_indices = {
                    'NIFTY': {'lot_size': 65, 'expiry_type': 'weekly', 'expiry_day': 1},
                    'BANKNIFTY': {'lot_size': 30, 'expiry_type': 'monthly', 'expiry_day': 1},
                    'SENSEX': {'lot_size': 20, 'expiry_type': 'weekly', 'expiry_day': 3},
                    'FINNIFTY': {'lot_size': 60, 'expiry_type': 'monthly', 'expiry_day': 1}
                }
                
                found_indices = {item.get('name'): item for item in data if isinstance(item, dict)}
                
                # Check MIDCPNIFTY is NOT present
                if 'MIDCPNIFTY' in found_indices:
                    success = False
                    details += " - MIDCPNIFTY should be removed but is still present"
                
                # Verify each expected index
                for idx_name, expected_config in expected_indices.items():
                    if idx_name not in found_indices:
                        success = False
                        details += f" - Missing index: {idx_name}"
                    else:
                        idx_data = found_indices[idx_name]
                        
                        # Check lot size
                        if idx_data.get('lot_size') != expected_config['lot_size']:
                            success = False
                            details += f" - {idx_name}: Expected lot_size={expected_config['lot_size']}, got {idx_data.get('lot_size')}"
                        
                        # Check expiry type
                        if idx_data.get('expiry_type') != expected_config['expiry_type']:
                            success = False
                            details += f" - {idx_name}: Expected expiry_type={expected_config['expiry_type']}, got {idx_data.get('expiry_type')}"
                        
                        # Check expiry day
                        if idx_data.get('expiry_day') != expected_config['expiry_day']:
                            success = False
                            details += f" - {idx_name}: Expected expiry_day={expected_config['expiry_day']}, got {idx_data.get('expiry_day')}"
                
                # Check structure of first item
                if data and isinstance(data[0], dict):
                    required_fields = ['name', 'display_name', 'lot_size', 'strike_interval', 'expiry_type', 'expiry_day']
                    missing_fields = [field for field in required_fields if field not in data[0]]
                    if missing_fields:
                        success = False
                        details += f" - Missing fields in index item: {missing_fields}"
        
        self.log_test("GET /api/indices", success, details, data)
        return success

    def test_timeframes_endpoint(self):
        """Test GET /api/timeframes"""
        success, details, data = self.test_api_endpoint(
            'GET', 'timeframes', 200,
            description="Available timeframes endpoint"
        )
        
        if success and data:
            if not isinstance(data, list):
                success = False
                details += " - Response should be a list"
            else:
                expected_values = [5, 15, 30, 60, 300, 900]
                found_values = [item.get('value') for item in data if isinstance(item, dict)]
                missing_values = [val for val in expected_values if val not in found_values]
                if missing_values:
                    success = False
                    details += f" - Missing timeframe values: {missing_values}"
                
                # Check structure of first item
                if data and isinstance(data[0], dict):
                    required_fields = ['value', 'label']
                    missing_fields = [field for field in required_fields if field not in data[0]]
                    if missing_fields:
                        success = False
                        details += f" - Missing fields in timeframe item: {missing_fields}"
        
        self.log_test("GET /api/timeframes", success, details, data)
        return success

    def test_index_selection_and_lot_size(self):
        """Test index selection and verify lot size changes - Focus on BANKNIFTY=30"""
        # Test BANKNIFTY selection
        test_config = {"selected_index": "BANKNIFTY"}
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Update to BANKNIFTY index"
        )
        self.log_test("POST /api/config/update (BANKNIFTY)", success, details, data)
        
        if success:
            # Verify config shows BANKNIFTY with lot_size=30
            success, details, config_data = self.test_api_endpoint(
                'GET', 'config', 200,
                description="Verify BANKNIFTY config"
            )
            if success and config_data:
                if config_data.get('selected_index') != 'BANKNIFTY':
                    success = False
                    details += " - Index not updated to BANKNIFTY"
                elif config_data.get('lot_size') != 30:
                    success = False
                    details += f" - Expected lot_size=30 for BANKNIFTY, got {config_data.get('lot_size')}"
            
            self.log_test("Verify BANKNIFTY lot_size=30", success, details, config_data)
        
        # Test NIFTY selection
        test_config = {"selected_index": "NIFTY"}
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Update to NIFTY index"
        )
        self.log_test("POST /api/config/update (NIFTY)", success, details, data)
        
        if success:
            # Verify config shows NIFTY with lot_size=65
            success, details, config_data = self.test_api_endpoint(
                'GET', 'config', 200,
                description="Verify NIFTY config"
            )
            if success and config_data:
                if config_data.get('selected_index') != 'NIFTY':
                    success = False
                    details += " - Index not updated to NIFTY"
                elif config_data.get('lot_size') != 65:
                    success = False
                    details += f" - Expected lot_size=65 for NIFTY, got {config_data.get('lot_size')}"
            
            self.log_test("Verify NIFTY lot_size=65", success, details, config_data)

    def test_timeframe_selection(self):
        """Test timeframe selection"""
        # Test valid timeframe (60 seconds = 1 minute)
        test_config = {"candle_interval": 60}
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Update candle interval to 60s"
        )
        self.log_test("POST /api/config/update (candle_interval=60)", success, details, data)
        
        if success:
            # Verify config shows updated interval
            success, details, config_data = self.test_api_endpoint(
                'GET', 'config', 200,
                description="Verify candle interval updated"
            )
            if success and config_data:
                if config_data.get('candle_interval') != 60:
                    success = False
                    details += f" - Expected candle_interval=60, got {config_data.get('candle_interval')}"
            
            self.log_test("Verify candle_interval=60", success, details, config_data)

    def test_invalid_inputs(self):
        """Test invalid index and timeframe inputs"""
        # Test invalid index
        test_config = {"selected_index": "INVALID_INDEX"}
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Test invalid index rejection"
        )
        # Should succeed but not update the index
        self.log_test("POST /api/config/update (invalid index)", success, details, data)
        
        # Test invalid timeframe
        test_config = {"candle_interval": 7}  # Invalid timeframe
        success, details, data = self.test_api_endpoint(
            'POST', 'config/update', 200, test_config,
            description="Test invalid timeframe rejection"
        )
        # Should succeed but not update the interval
        self.log_test("POST /api/config/update (invalid timeframe)", success, details, data)

    def run_all_tests(self):
        """Run all API tests"""
        print("🚀 Starting NiftyAlgo Trading Bot API Tests")
        print(f"📡 Testing against: {self.base_url}")
        print("=" * 60)
        
        # Test basic endpoints
        self.test_status_endpoint()
        self.test_config_endpoint()
        self.test_market_nifty_endpoint()
        self.test_position_endpoint()
        self.test_trades_endpoint()
        self.test_summary_endpoint()
        self.test_logs_endpoint()
        
        # Test new endpoints for index/timeframe selection
        self.test_indices_endpoint()
        self.test_timeframes_endpoint()
        
        # Test configuration updates - Focus on target_points and index selection
        self.test_target_points_update()
        self.test_index_selection_and_lot_size()
        self.test_timeframe_selection()
        self.test_invalid_inputs()
        
        # Test bot control and mode
        self.test_mode_endpoint()
        self.test_bot_control_endpoints()
        
        # Print summary
        print("=" * 60)
        print(f"📊 Test Results: {self.tests_passed}/{self.tests_run} passed")
        success_rate = (self.tests_passed / self.tests_run * 100) if self.tests_run > 0 else 0
        print(f"📈 Success Rate: {success_rate:.1f}%")
        
        if self.tests_passed == self.tests_run:
            print("🎉 All tests passed!")
            return 0
        else:
            print("⚠️  Some tests failed. Check the details above.")
            return 1

def main():
    """Main test runner"""
    tester = NiftyAlgoAPITester()
    return tester.run_all_tests()

if __name__ == "__main__":
    sys.exit(main())