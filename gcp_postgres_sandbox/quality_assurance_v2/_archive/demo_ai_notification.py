#!/usr/bin/env python3
"""
Demonstration of AI-powered QA notification system.
Shows working OpenAI integration with Telegram notifications.
"""

import sys
from pathlib import Path

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
    from quality_assurance_v2.core.base_qa import QAResult
except ImportError:
    try:
        from core.config import QAConfig
        from reporting.notification_system import NotificationSystem
        from core.base_qa import QAResult
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)

def demo_ai_notification():
    """Demonstrate AI-powered notification system."""
    print("=== CryptoPrism-DB QA v2: AI Notification Demo ===")
    
    try:
        # Initialize configuration
        config = QAConfig()
        print("Configuration loaded successfully")
        
        # Initialize notification system
        notification_system = NotificationSystem(config)
        print(f"Notification system initialized")
        print(f"AI Provider: {notification_system.ai_provider if notification_system.ai_available else 'None'}")
        print(f"AI Available: {notification_system.ai_available}")
        
        # Test connectivity
        test_results = notification_system.test_notifications()
        print(f"Connectivity test results: {test_results}")
        
        if not test_results.get('telegram', False):
            print("Telegram notifications not working - check configuration")
            return False
        
        # Create sample QA results
        sample_results = [
            QAResult(
                check_name="database.connectivity_check", 
                status="PASS",
                risk_level="LOW",
                message="Database connection successful",
                details={"database": "dbcp", "response_time": "245ms"}
            ),
            QAResult(
                check_name="performance.slow_query_detection",
                status="WARNING", 
                risk_level="MEDIUM",
                message="Query execution time above threshold (1.2s)",
                details={"database": "dbcp", "query": "SELECT * FROM FE_DMV_ALL", "execution_time": 1.2}
            ),
            QAResult(
                check_name="integrity.null_ratio_check",
                status="FAIL", 
                risk_level="HIGH",
                message="Excessive null values detected in critical column",
                details={"database": "dbcp", "table": "FE_MOMENTUM_SIGNALS", "column": "price", "null_ratio": 15.2}
            ),
            QAResult(
                check_name="security.duplicate_detection",
                status="FAIL",
                risk_level="CRITICAL", 
                message="Critical duplicate records found in primary signal table",
                details={"database": "dbcp", "table": "FE_DMV_ALL", "duplicate_count": 234}
            )
        ]
        
        execution_metadata = {
            'total_execution_time': 42.7,
            'databases': ['dbcp'],
            'modules_executed': ['performance_monitor', 'data_integrity']
        }
        
        # Send AI-powered QA summary alert
        print("\n=== Sending AI-Enhanced QA Alert ===")
        success = notification_system.send_qa_summary_alert(
            results=sample_results,
            execution_metadata=execution_metadata,
            force_send=True  # Force send for demo
        )
        
        if success:
            print("SUCCESS: AI-powered QA notification sent to Telegram!")
            print("Check your Telegram for the comprehensive QA report with AI analysis.")
            
            # Show notification statistics
            stats = notification_system.get_notification_statistics()
            print(f"\nNotification Statistics: {stats}")
            
            return True
        else:
            print("ERROR: Failed to send notification")
            return False
            
    except Exception as e:
        print(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = demo_ai_notification()
    if success:
        print("\n🎉 QA System v2 with AI Integration is fully operational!")
        print("Features demonstrated:")
        print("✅ OpenAI-powered intelligent summaries")  
        print("✅ Multi-risk level analysis")
        print("✅ Telegram alert delivery")
        print("✅ Comprehensive QA reporting")
    else:
        print("\n❌ Demo failed - check configuration")