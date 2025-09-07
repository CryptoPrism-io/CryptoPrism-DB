#!/usr/bin/env python3
"""
Simple test script to verify QA v2 notification system works.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add paths for imports
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(current_dir))

from dotenv import load_dotenv

# Load environment
load_dotenv(current_dir.parent.parent / ".env")

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
except ImportError:
    from core.config import QAConfig
    from reporting.notification_system import NotificationSystem

def test_notifications():
    """Test the notification system with a simple message."""
    print("Testing QA v2 notification system...")
    
    try:
        # Initialize configuration
        config = QAConfig()
        print("Configuration loaded")
        
        # Initialize notification system
        notification_system = NotificationSystem(config)
        print("Notification system initialized")
        
        # Test connectivity
        test_results = notification_system.test_notifications()
        print(f"Test results: {test_results}")
        
        # Create sample QA results for testing
        from core.base_qa import QAResult
        
        # Create some sample results
        sample_results = [
            QAResult(
                check_name="test_connectivity", 
                status="PASS",
                risk_level="LOW",
                message="Database connectivity test passed",
                details={"database": "dbcp"}
            ),
            QAResult(
                check_name="test_critical_issue",
                status="FAIL", 
                risk_level="CRITICAL",
                message="Sample critical issue for testing notifications",
                details={"database": "dbcp"}
            )
        ]
        
        execution_metadata = {
            'total_execution_time': 15.2,
            'databases': ['dbcp'],
            'modules_executed': ['notification_test']
        }
        
        # Send test alert
        print("\nSending test QA summary alert...")
        success = notification_system.send_qa_summary_alert(
            results=sample_results,
            execution_metadata=execution_metadata,
            force_send=True  # Force send even with minimal issues
        )
        
        if success:
            print("Test notification sent successfully!")
            print("Check your Telegram for the QA alert message.")
        else:
            print("Test notification failed")
            
        return success
        
    except Exception as e:
        print(f"Error during notification test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_notifications()