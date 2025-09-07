"""
Notification System for CryptoPrism-DB QA system v2.
Handles Telegram alerts, AI-powered summaries, and multi-channel notifications.
"""

import logging
import requests
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from ..core.base_qa import QAResult
from ..core.config import QAConfig
from .qa_report_logger import QAReportLogger

logger = logging.getLogger(__name__)

class NotificationSystem:
    """
    Multi-channel notification system for QA alerts and summaries.
    Supports Telegram, AI-powered analysis, and structured reporting.
    """
    
    def __init__(self, config: QAConfig):
        """
        Initialize notification system.
        
        Args:
            config: QA configuration instance
        """
        self.config = config
        self.notification_config = config.notification_config
        
        # Initialize AI if available
        self.ai_available = False
        self.ai_provider = None
        
        # Try OpenAI first
        if OPENAI_AVAILABLE and self.notification_config.get('openai_api_key'):
            try:
                self.openai_client = openai.OpenAI(api_key=self.notification_config['openai_api_key'])
                self.ai_available = True
                self.ai_provider = 'openai'
                logger.info("AI-powered notifications enabled (OpenAI)")
            except Exception as e:
                logger.warning(f"OpenAI initialization failed: {e}")
        
        # Fallback to Gemini if OpenAI not available
        elif GEMINI_AVAILABLE and self.notification_config.get('gemini_api_key'):
            try:
                genai.configure(api_key=self.notification_config['gemini_api_key'])
                self.ai_model = genai.GenerativeModel('gemini-2.0-flash-exp')
                self.ai_available = True
                self.ai_provider = 'gemini'
                logger.info("AI-powered notifications enabled (Gemini)")
            except Exception as e:
                logger.warning(f"Gemini initialization failed: {e}")
                
        if not self.ai_available:
            logger.info("AI-powered notifications disabled - no valid API keys found")
        
        # Track notification history
        self.notification_history = []
        
        # Initialize QA report logger
        self.qa_report_logger = QAReportLogger()
    
    def send_qa_summary_alert(self, 
                             results: List[QAResult],
                             execution_metadata: Dict[str, Any],
                             force_send: bool = False) -> bool:
        """
        Send comprehensive QA summary alert via available channels.
        
        Args:
            results: List of QA results
            execution_metadata: Execution metadata
            force_send: Send regardless of alert thresholds
            
        Returns:
            True if notification sent successfully
        """
        logger.info("Preparing QA summary alert")
        
        # Filter results that should trigger alerts
        alert_results = [r for r in results if r.should_alert(self.config) or force_send]
        
        if not alert_results and not force_send:
            logger.info("No alert-worthy results found")
            return True
        
        try:
            # Prepare alert data
            alert_data = self._prepare_alert_data(results, execution_metadata)
            
            # Generate AI summary if available
            ai_summary = ""
            if self.ai_available:
                ai_summary = self._generate_ai_summary(alert_data)
            
            # Send via Telegram
            telegram_success = self._send_telegram_alert(alert_data, ai_summary)
            
            # Log to QA report file for system awareness
            try:
                system_changes = self._detect_system_changes(execution_metadata)
                recommendations = self._generate_recommendations(alert_data)
                
                self.qa_report_logger.log_qa_execution(
                    results=results,
                    execution_metadata=execution_metadata,
                    ai_summary=ai_summary,
                    system_changes=system_changes,
                    recommendations=recommendations
                )
                logger.info("QA execution logged to QA_REPORT.md")
            except Exception as e:
                logger.warning(f"Failed to log QA execution to report file: {e}")
            
            # Log notification attempt
            self._log_notification_attempt('qa_summary', telegram_success, alert_data)
            
            return telegram_success
            
        except Exception as e:
            logger.error(f"❌ Failed to send QA summary alert: {e}")
            return False
    
    def send_individual_test_alert(self, 
                                 test_name: str,
                                 results: List[QAResult],
                                 test_type: str = "individual_test") -> bool:
        """
        Send Telegram alert for individual test results.
        
        Args:
            test_name: Name of the individual test
            results: List of QAResult objects from the test
            test_type: Type of test for categorization
        
        Returns:
            Success status
        """
        try:
            # Calculate test statistics
            total_checks = len(results)
            status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
            risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
            
            for result in results:
                status_counts[result.status] += 1
                risk_counts[result.risk_level] += 1
            
            # Calculate health score
            health_score = 0
            if total_checks > 0:
                health_score = (
                    status_counts['PASS'] * 1.0 +
                    status_counts['WARNING'] * 0.7 +
                    status_counts['FAIL'] * 0.3 +
                    status_counts['ERROR'] * 0.1
                ) / total_checks * 100
            
            # Generate AI summary if available
            ai_summary = ""
            if self.ai_available and (status_counts['FAIL'] > 0 or status_counts['ERROR'] > 0):
                issues_text = []
                for result in results:
                    if result.status in ['FAIL', 'ERROR']:
                        issues_text.append(f"{result.check_name}: {result.message}")
                
                if issues_text:
                    prompt = f"""Individual Test: {test_name}
                    
Issues Found:
{chr(10).join(issues_text[:5])}

Generate a concise technical summary (max 150 chars) highlighting the most critical issues that need attention."""
                    ai_summary = self._generate_ai_summary(prompt)
            
            # Create individual test alert message
            message = self._format_individual_test_message(
                test_name, health_score, status_counts, risk_counts, results, ai_summary
            )
            
            return self._send_telegram_message(message)
            
        except Exception as e:
            logger.error(f"❌ Failed to send individual test alert: {e}")
            return False
    
    def _format_individual_test_message(self, 
                                      test_name: str,
                                      health_score: float,
                                      status_counts: Dict[str, int],
                                      risk_counts: Dict[str, int],
                                      results: List[QAResult],
                                      ai_summary: str = "") -> str:
        """Format individual test results for Telegram message."""
        
        # Status emoji based on health score
        if health_score >= 95:
            status_emoji = "✅"
        elif health_score >= 80:
            status_emoji = "🟡"
        elif health_score >= 50:
            status_emoji = "🟠"
        else:
            status_emoji = "🔴"
        
        message_lines = [
            f"🔬 *{test_name}* {status_emoji}",
            f"Health Score: *{health_score:.1f}/100*",
            f"Time: {datetime.utcnow().strftime('%H:%M UTC')}",
            ""
        ]
        
        # Test summary
        message_lines.extend([
            f"📋 *Test Results*",
            f"• Total Checks: {len(results)}",
            f"• Passed: {status_counts['PASS']} | Failed: {status_counts['FAIL']}",
            f"• Warnings: {status_counts['WARNING']} | Errors: {status_counts['ERROR']}",
            ""
        ])
        
        # Risk breakdown if issues exist
        if risk_counts['CRITICAL'] > 0 or risk_counts['HIGH'] > 0:
            message_lines.extend([
                f"⚠️ *Risk Levels*",
                f"🔴 Critical: {risk_counts['CRITICAL']} | 🟠 High: {risk_counts['HIGH']}",
                f"🟡 Medium: {risk_counts['MEDIUM']} | 🟢 Low: {risk_counts['LOW']}",
                ""
            ])
        
        # Critical/Failed issues
        critical_failed = [r for r in results if r.status in ['FAIL', 'ERROR'] or r.risk_level == 'CRITICAL']
        if critical_failed:
            message_lines.extend([
                f"🚨 *Critical Issues*"
            ])
            
            for issue in critical_failed[:3]:  # Show top 3
                msg = issue.message[:60] + "..." if len(issue.message) > 60 else issue.message
                message_lines.append(f"• {msg}")
            
            if len(critical_failed) > 3:
                message_lines.append(f"• ... and {len(critical_failed) - 3} more issues")
            
            message_lines.append("")
        
        # AI summary if available
        if ai_summary:
            message_lines.extend([
                f"🤖 *AI Analysis*",
                f"{ai_summary}",
                ""
            ])
        
        # Overall status
        if status_counts['FAIL'] == 0 and status_counts['ERROR'] == 0:
            if status_counts['WARNING'] == 0:
                message_lines.append("✅ *Status: EXCELLENT*")
            else:
                message_lines.append("🟡 *Status: GOOD* - Minor issues")
        elif status_counts['FAIL'] > 0:
            message_lines.append("🔴 *Status: ISSUES* - Problems detected")
        else:
            message_lines.append("⚡ *Status: PARTIAL* - Some checks failed")
        
        return "\n".join(message_lines)
    
    def send_critical_issue_alert(self, result: QAResult) -> bool:
        """
        Send immediate alert for critical issues.
        
        Args:
            result: Critical QA result
            
        Returns:
            True if notification sent successfully
        """
        if result.risk_level != 'CRITICAL':
            return True
        
        logger.warning(f"🚨 Sending critical issue alert: {result.check_name}")
        
        try:
            message = self._format_critical_alert(result)
            success = self._send_telegram_message(message)
            
            # Log critical notification
            self._log_notification_attempt('critical_issue', success, {
                'check_name': result.check_name,
                'message': result.message,
                'database': result.details.get('database', 'unknown')
            })
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to send critical issue alert: {e}")
            return False
    
    def send_performance_degradation_alert(self, 
                                         performance_metrics: Dict[str, Any]) -> bool:
        """
        Send alert for significant performance degradation.
        
        Args:
            performance_metrics: Performance comparison data
            
        Returns:
            True if notification sent successfully
        """
        try:
            message = self._format_performance_alert(performance_metrics)
            success = self._send_telegram_message(message)
            
            self._log_notification_attempt('performance_degradation', success, performance_metrics)
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to send performance alert: {e}")
            return False
    
    def _prepare_alert_data(self, 
                           results: List[QAResult],
                           execution_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare structured alert data."""
        
        # Categorize results
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        
        critical_issues = []
        high_issues = []
        error_issues = []
        
        for result in results:
            risk_counts[result.risk_level] += 1
            status_counts[result.status] += 1
            
            if result.risk_level == 'CRITICAL':
                critical_issues.append({
                    'check': result.check_name.split('.')[-1],  # Simplified name
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
            elif result.risk_level == 'HIGH':
                high_issues.append({
                    'check': result.check_name.split('.')[-1],
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
            elif result.status == 'ERROR':
                error_issues.append({
                    'check': result.check_name.split('.')[-1],
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
        
        # Calculate health score
        total_checks = len(results)
        health_score = 0
        if total_checks > 0:
            health_score = (
                status_counts['PASS'] * 1.0 +
                status_counts['WARNING'] * 0.7 +
                status_counts['FAIL'] * 0.3 +
                status_counts['ERROR'] * 0.1
            ) / total_checks * 100
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'total_checks': total_checks,
            'health_score': round(health_score, 1),
            'execution_time': execution_metadata.get('total_execution_time', 0),
            'databases_checked': execution_metadata.get('databases', []),
            'modules_executed': execution_metadata.get('modules_executed', []),
            'risk_distribution': risk_counts,
            'status_distribution': status_counts,
            'critical_issues': critical_issues,
            'high_issues': high_issues,
            'error_issues': error_issues
        }
    
    def _generate_ai_summary(self, alert_data: Dict[str, Any]) -> str:
        """Generate AI-powered summary of QA results."""
        if not self.ai_available:
            return ""
        
        # Prepare prompt for AI analysis
        prompt = f"""Analyze this CryptoPrism-DB quality assurance report and provide a concise technical summary for database administrators.

QA EXECUTION SUMMARY:
- Health Score: {alert_data['health_score']}/100
- Total Checks: {alert_data['total_checks']}
- Databases: {', '.join(alert_data['databases_checked'])}
- Execution Time: {alert_data['execution_time']:.1f}s

RISK DISTRIBUTION:
- Critical: {alert_data['risk_distribution']['CRITICAL']}
- High: {alert_data['risk_distribution']['HIGH']} 
- Medium: {alert_data['risk_distribution']['MEDIUM']}
- Low: {alert_data['risk_distribution']['LOW']}

STATUS DISTRIBUTION:
- Pass: {alert_data['status_distribution']['PASS']}
- Warning: {alert_data['status_distribution']['WARNING']}
- Fail: {alert_data['status_distribution']['FAIL']}
- Error: {alert_data['status_distribution']['ERROR']}

CRITICAL ISSUES: {len(alert_data['critical_issues'])}
{json.dumps(alert_data['critical_issues'][:3], indent=2) if alert_data['critical_issues'] else 'None'}

HIGH RISK ISSUES: {len(alert_data['high_issues'])}
{json.dumps(alert_data['high_issues'][:2], indent=2) if alert_data['high_issues'] else 'None'}

Provide a concise summary focusing on:
1. Overall system health assessment
2. Most critical issues requiring immediate attention  
3. Brief recommendations for next steps
4. Risk level classification (LOW/MEDIUM/HIGH/CRITICAL)

Keep response under 200 words and suitable for Telegram notification."""
        
        try:
            if self.ai_provider == 'openai':
                response = self.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a database QA analyst. Provide concise technical summaries."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=200,
                    temperature=0.7
                )
                
                if response and response.choices:
                    return response.choices[0].message.content.strip()
                    
            elif self.ai_provider == 'gemini':
                response = self.ai_model.generate_content(prompt)
                if response and response.text:
                    return response.text.strip()
            
        except Exception as e:
            logger.warning(f"AI summary generation failed ({self.ai_provider}): {e}")
        
        return ""
    
    def _send_telegram_alert(self, 
                           alert_data: Dict[str, Any],
                           ai_summary: str = "") -> bool:
        """Send formatted alert via Telegram."""
        if not self.notification_config.get('telegram_bot_token'):
            logger.info("Telegram notifications not configured")
            return True
        
        try:
            message = self._format_telegram_message(alert_data, ai_summary)
            return self._send_telegram_message(message)
            
        except Exception as e:
            logger.error(f"❌ Telegram alert failed: {e}")
            return False
    
    def _format_telegram_message(self, 
                                alert_data: Dict[str, Any],
                                ai_summary: str = "") -> str:
        """Format alert data for Telegram message."""
        
        # Header with health score
        health_score = alert_data['health_score']
        if health_score >= 90:
            status_emoji = "✅"
        elif health_score >= 70:
            status_emoji = "🟡"
        else:
            status_emoji = "🔴"
        
        message_lines = [
            f"🔍 *CryptoPrism-DB QA Report* {status_emoji}",
            f"Health Score: *{health_score}/100*",
            f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            ""
        ]
        
        # Execution summary
        message_lines.extend([
            f"📊 *Execution Summary*",
            f"• Total Checks: {alert_data['total_checks']}",
            f"• Databases: {', '.join(alert_data['databases_checked'])}",
            f"• Duration: {alert_data['execution_time']:.1f}s",
            ""
        ])
        
        # Risk overview
        risk_dist = alert_data['risk_distribution']
        if risk_dist['CRITICAL'] > 0 or risk_dist['HIGH'] > 0:
            message_lines.extend([
                f"⚠️ *Risk Overview*",
                f"• Critical: {risk_dist['CRITICAL']}",
                f"• High: {risk_dist['HIGH']}",
                f"• Medium: {risk_dist['MEDIUM']}",
                ""
            ])
        
        # Critical issues
        critical_issues = alert_data['critical_issues']
        if critical_issues:
            message_lines.extend([
                f"🚨 *Critical Issues ({len(critical_issues)})*"
            ])
            
            for issue in critical_issues[:3]:  # Limit to top 3
                database = issue['database']
                check = issue['check']
                msg = issue['message'][:80] + "..." if len(issue['message']) > 80 else issue['message']
                message_lines.append(f"• {check} [{database}]: {msg}")
            
            if len(critical_issues) > 3:
                message_lines.append(f"• ... and {len(critical_issues) - 3} more critical issues")
            
            message_lines.append("")
        
        # High-risk issues summary
        high_issues = alert_data['high_issues']
        if high_issues:
            message_lines.extend([
                f"🟠 *High-Risk Issues*: {len(high_issues)} found",
                ""
            ])
        
        # AI summary if available
        if ai_summary:
            message_lines.extend([
                f"🤖 *AI Analysis*",
                ai_summary,
                ""
            ])
        
        # Footer
        message_lines.extend([
            f"📈 Status: {alert_data['status_distribution']['PASS']}✅ "
                    f"{alert_data['status_distribution']['WARNING']}⚠️ "
                    f"{alert_data['status_distribution']['FAIL']}❌ "
                    f"{alert_data['status_distribution']['ERROR']}🔥"
        ])
        
        return "\n".join(message_lines)
    
    def _format_critical_alert(self, result: QAResult) -> str:
        """Format critical issue for immediate alert."""
        database = result.details.get('database', 'unknown')
        check_name = result.check_name.split('.')[-1]  # Simplified name
        
        return f"""🚨 *CRITICAL QA ALERT* 🚨

*Check*: {check_name}
*Database*: {database}
*Issue*: {result.message}

*Timestamp*: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
*Session*: Immediate Alert

⚡ *Action Required*: Investigate immediately"""
    
    def _format_performance_alert(self, metrics: Dict[str, Any]) -> str:
        """Format performance degradation alert."""
        return f"""
📉 **Performance Degradation Alert**

**Database**: {metrics.get('database', 'unknown')}
**Metric**: {metrics.get('metric_type', 'unknown')}
**Current Value**: {metrics.get('current_value', 'N/A')}
**Previous Value**: {metrics.get('previous_value', 'N/A')}
**Degradation**: {metrics.get('degradation_percentage', 'N/A')}%

**Timestamp**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

🔍 **Recommended**: Review database performance and optimization
"""
    
    def _send_telegram_message(self, message: str) -> bool:
        """Send message via Telegram Bot API."""
        bot_token = self.notification_config.get('telegram_bot_token')
        chat_id = self.notification_config.get('telegram_chat_id')
        
        if not bot_token or not chat_id:
            logger.warning("Telegram configuration incomplete")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("✅ Telegram notification sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Telegram API error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Telegram notification failed: {e}")
            return False
    
    def _log_notification_attempt(self, 
                                notification_type: str,
                                success: bool,
                                data: Dict[str, Any]):
        """Log notification attempt for tracking."""
        attempt = {
            'timestamp': datetime.utcnow().isoformat(),
            'type': notification_type,
            'success': success,
            'data_summary': {
                'message_length': len(str(data)),
                'channels': []
            }
        }
        
        if self.notification_config.get('telegram_bot_token'):
            attempt['data_summary']['channels'].append('telegram')
        
        if self.ai_available:
            attempt['data_summary']['channels'].append('ai_summary')
        
        self.notification_history.append(attempt)
        
        # Keep only last 50 notifications in memory
        if len(self.notification_history) > 50:
            self.notification_history = self.notification_history[-50:]
    
    def test_notifications(self) -> Dict[str, bool]:
        """Test notification channels to ensure they're working."""
        logger.info("Testing notification channels")
        
        results = {}
        
        # Test Telegram
        if self.notification_config.get('telegram_bot_token'):
            try:
                test_message = f"""
🔧 **CryptoPrism-DB QA System Test**

Notification system test at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

✅ Telegram integration working
"""
                results['telegram'] = self._send_telegram_message(test_message)
            except Exception as e:
                logger.error(f"Telegram test failed: {e}")
                results['telegram'] = False
        else:
            results['telegram'] = False
        
        # Test AI
        if self.ai_available:
            try:
                test_data = {
                    'health_score': 85.0,
                    'total_checks': 10,
                    'execution_time': 5.2,
                    'databases_checked': ['test_db'],
                    'risk_distribution': {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 7},
                    'status_distribution': {'PASS': 8, 'WARNING': 1, 'FAIL': 1, 'ERROR': 0},
                    'critical_issues': [],
                    'high_issues': [{'check': 'test_check', 'message': 'test issue', 'database': 'test_db'}]
                }
                ai_response = self._generate_ai_summary(test_data)
                results['ai_summary'] = bool(ai_response)
            except Exception as e:
                logger.error(f"AI test failed: {e}")
                results['ai_summary'] = False
        else:
            results['ai_summary'] = False
        
        logger.info(f"Notification test results: {results}")
        return results
    
    def _detect_system_changes(self, execution_metadata: Dict[str, Any]) -> List[str]:
        """Detect system changes since last QA execution."""
        changes = []
        
        try:
            # Check if this is first run by looking for previous health scores
            databases = execution_metadata.get('databases', [])
            for db in databases:
                last_score = self.qa_report_logger.get_last_health_score(db)
                if last_score is None:
                    changes.append(f"First QA execution for {db} database")
            
            # Check for new modules or configurations
            modules_executed = execution_metadata.get('modules_executed', [])
            if 'ai_integration' in modules_executed or self.ai_available:
                changes.append("AI-powered analysis integration active")
            
            if self.notification_config.get('telegram_bot_token'):
                changes.append("Telegram notification system active")
                
            return changes
            
        except Exception as e:
            logger.warning(f"Failed to detect system changes: {e}")
            return ["System change detection unavailable"]
    
    def _generate_recommendations(self, alert_data: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on QA results."""
        recommendations = []
        
        try:
            health_score = alert_data['health_score']
            critical_count = alert_data['risk_distribution']['CRITICAL']
            high_count = alert_data['risk_distribution']['HIGH']
            error_count = alert_data['status_distribution']['ERROR']
            
            # Health-based recommendations
            if health_score < 30:
                recommendations.append("**URGENT**: System health critical - immediate intervention required")
            elif health_score < 50:
                recommendations.append("**HIGH PRIORITY**: System health poor - schedule maintenance window")
            elif health_score < 70:
                recommendations.append("**MONITOR**: System health fair - investigate warnings")
            
            # Issue-based recommendations
            if critical_count > 0:
                recommendations.append(f"**IMMEDIATE**: Address {critical_count} critical issue(s) - system stability at risk")
            
            if high_count > 0:
                recommendations.append(f"**HIGH PRIORITY**: Resolve {high_count} high-risk issue(s) within 24 hours")
            
            if error_count > 0:
                recommendations.append(f"**TECHNICAL**: Fix {error_count} system error(s) - may indicate infrastructure problems")
            
            # Specific recommendations based on issue patterns
            critical_issues = alert_data.get('critical_issues', [])
            for issue in critical_issues:
                if 'connectivity' in issue['check'].lower():
                    recommendations.append("**DATABASE**: Verify database connectivity and credentials")
                elif 'table' in issue['check'].lower() or 'schema' in issue['check'].lower():
                    recommendations.append("**PIPELINE**: Execute technical analysis pipeline to populate missing tables")
            
            # Performance recommendations
            execution_time = alert_data.get('execution_time', 0)
            if execution_time > 120:
                recommendations.append("**PERFORMANCE**: QA execution time excessive - consider optimization")
            
            # Default recommendation if no issues
            if not recommendations and health_score >= 90:
                recommendations.append("**MAINTAIN**: System health excellent - continue current monitoring schedule")
            
            return recommendations[:5]  # Limit to 5 recommendations
            
        except Exception as e:
            logger.warning(f"Failed to generate recommendations: {e}")
            return ["Recommendation generation unavailable"]
    
    def get_notification_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent notification history."""
        return self.notification_history[-limit:] if self.notification_history else []
    
    def get_notification_statistics(self) -> Dict[str, Any]:
        """Get notification system statistics."""
        if not self.notification_history:
            return {'total_notifications': 0}
        
        successful_notifications = len([n for n in self.notification_history if n['success']])
        
        notification_types = {}
        for notification in self.notification_history:
            notif_type = notification['type']
            notification_types[notif_type] = notification_types.get(notif_type, 0) + 1
        
        return {
            'total_notifications': len(self.notification_history),
            'successful_notifications': successful_notifications,
            'success_rate': successful_notifications / len(self.notification_history) * 100,
            'notification_types': notification_types,
            'channels_configured': {
                'telegram': bool(self.notification_config.get('telegram_bot_token')),
                'ai_summary': self.ai_available
            }
        }