"""
QA Report Logger for CryptoPrism-DB QA system v2.
Automatically logs QA execution results to QA_REPORT.md for system awareness.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
import re

from ..core.base_qa import QAResult

logger = logging.getLogger(__name__)

class QAReportLogger:
    """
    Logs QA execution results to QA_REPORT.md file for persistent tracking.
    Maintains system awareness of historical QA results and trends.
    """
    
    def __init__(self, report_file_path: Optional[str] = None):
        """
        Initialize QA report logger.
        
        Args:
            report_file_path: Optional path to QA_REPORT.md file
        """
        if report_file_path:
            self.report_file_path = Path(report_file_path)
        else:
            # Default to project root (4 levels up from this file)
            current_dir = Path(__file__).parent
            self.report_file_path = current_dir.parent.parent.parent / "QA_REPORT.md"
        
        self.qa_version = "v2.0.0"
    
    def log_qa_execution(self, 
                        results: List[QAResult],
                        execution_metadata: Dict[str, Any],
                        ai_summary: str = "",
                        system_changes: List[str] = None,
                        recommendations: List[str] = None) -> bool:
        """
        Log QA execution results to the report file.
        
        Args:
            results: List of QA results
            execution_metadata: Execution metadata
            ai_summary: AI-generated analysis summary
            system_changes: List of detected system changes
            recommendations: List of recommendations
            
        Returns:
            True if logging successful
        """
        try:
            # Prepare report data
            report_data = self._prepare_report_data(
                results, execution_metadata, ai_summary, system_changes, recommendations
            )
            
            # Generate report entry
            report_entry = self._generate_report_entry(report_data)
            
            # Update report file
            success = self._update_report_file(report_entry, report_data)
            
            if success:
                logger.info(f"QA execution logged to {self.report_file_path}")
            else:
                logger.error(f"Failed to log QA execution to {self.report_file_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"QA report logging failed: {e}")
            return False
    
    def _prepare_report_data(self, 
                           results: List[QAResult],
                           execution_metadata: Dict[str, Any],
                           ai_summary: str,
                           system_changes: List[str],
                           recommendations: List[str]) -> Dict[str, Any]:
        """Prepare structured report data."""
        
        # Calculate status and risk distributions
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        critical_issues = []
        high_issues = []
        medium_issues = []
        
        for result in results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
            
            issue_data = {
                'check': result.check_name,
                'message': result.message,
                'status': result.status,
                'database': result.details.get('database', 'unknown')
            }
            
            if result.risk_level == 'CRITICAL':
                critical_issues.append(issue_data)
            elif result.risk_level == 'HIGH':
                high_issues.append(issue_data)
            elif result.risk_level == 'MEDIUM':
                medium_issues.append(issue_data)
        
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
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'qa_version': self.qa_version,
            'databases': execution_metadata.get('databases', []),
            'execution_time': execution_metadata.get('total_execution_time', 0),
            'total_checks': total_checks,
            'health_score': round(health_score, 1),
            'status_distribution': status_counts,
            'risk_distribution': risk_counts,
            'critical_issues': critical_issues,
            'high_issues': high_issues,
            'medium_issues': medium_issues,
            'ai_summary': ai_summary,
            'system_changes': system_changes or [],
            'recommendations': recommendations or []
        }
    
    def _generate_report_entry(self, report_data: Dict[str, Any]) -> str:
        """Generate formatted report entry."""
        
        # Header
        entry_lines = [
            f"### [{report_data['timestamp']}] - QA {report_data['qa_version']} - {', '.join(report_data['databases']).upper()} Database Analysis",
            "",
            "**Execution Details:**",
            f"- Databases Tested: {', '.join(report_data['databases'])}",
            f"- Execution Time: {report_data['execution_time']:.1f}s",
            f"- Health Score: {report_data['health_score']}/100",
            f"- QA Version: {report_data['qa_version']} (Enhanced with AI integration)",
            "",
            "**Status Distribution:**",
            f"- ✅ PASS: {report_data['status_distribution']['PASS']}",
            f"- ⚠️ WARNING: {report_data['status_distribution']['WARNING']}",
            f"- ❌ FAIL: {report_data['status_distribution']['FAIL']}",
            f"- 🔥 ERROR: {report_data['status_distribution']['ERROR']}",
            "",
            "**Risk Distribution:**",
            f"- 🟢 LOW: {report_data['risk_distribution']['LOW']}",
            f"- 🟡 MEDIUM: {report_data['risk_distribution']['MEDIUM']}",
            f"- 🟠 HIGH: {report_data['risk_distribution']['HIGH']}",
            f"- 🔴 CRITICAL: {report_data['risk_distribution']['CRITICAL']}",
            "",
            "**Key Findings:**",
            ""
        ]
        
        # Critical issues
        issue_counter = 1
        for issue in report_data['critical_issues']:
            entry_lines.extend([
                f"{issue_counter}. **🔴 CRITICAL: {self._format_issue_title(issue)}**",
                f"   - Check: `{issue['check']}`",
                f"   - Issue: {issue['message']}",
                f"   - Impact: Critical system functionality affected",
                f"   - Status: {issue['status']}",
                ""
            ])
            issue_counter += 1
        
        # High issues
        for issue in report_data['high_issues']:
            entry_lines.extend([
                f"{issue_counter}. **🟠 HIGH: {self._format_issue_title(issue)}**",
                f"   - Check: `{issue['check']}`",
                f"   - Issue: {issue['message']}",
                f"   - Impact: Significant system degradation",
                f"   - Status: {issue['status']}",
                ""
            ])
            issue_counter += 1
        
        # Medium issues (limit to 3)
        for issue in report_data['medium_issues'][:3]:
            entry_lines.extend([
                f"{issue_counter}. **🟡 MEDIUM: {self._format_issue_title(issue)}**",
                f"   - Check: `{issue['check']}`",
                f"   - Issue: {issue['message']}",
                f"   - Impact: Moderate system impact",
                f"   - Status: {issue['status']}",
                ""
            ])
            issue_counter += 1
        
        if len(report_data['medium_issues']) > 3:
            entry_lines.append(f"   - ... and {len(report_data['medium_issues']) - 3} more medium-risk issues")
            entry_lines.append("")
        
        # AI Analysis
        if report_data['ai_summary']:
            entry_lines.extend([
                "**AI Analysis:**",
                f"> {report_data['ai_summary']}",
                ""
            ])
        
        # System Changes
        if report_data['system_changes']:
            entry_lines.extend([
                "**System Changes Detected:**"
            ])
            for change in report_data['system_changes']:
                entry_lines.append(f"- {change}")
            entry_lines.append("")
        
        # Recommendations
        if report_data['recommendations']:
            entry_lines.extend([
                "**Recommendations:**"
            ])
            for i, rec in enumerate(report_data['recommendations'], 1):
                entry_lines.append(f"{i}. {rec}")
            entry_lines.append("")
        
        entry_lines.extend([
            "**Follow-up Actions:**",
            "- [ ] Review and address critical issues",
            "- [ ] Monitor system health trends", 
            "- [ ] Schedule next QA execution",
            "",
            "---",
            ""
        ])
        
        return "\n".join(entry_lines)
    
    def _format_issue_title(self, issue: Dict[str, Any]) -> str:
        """Format issue title for readability."""
        check_parts = issue['check'].split('.')
        if len(check_parts) >= 2:
            category = check_parts[0].replace('_', ' ').title()
            specific = check_parts[-1].replace('_', ' ').title()
            return f"{category} {specific}"
        return issue['check'].replace('_', ' ').title()
    
    def _update_report_file(self, report_entry: str, report_data: Dict[str, Any]) -> bool:
        """Update QA_REPORT.md file with new entry."""
        try:
            if not self.report_file_path.exists():
                logger.warning(f"QA report file does not exist: {self.report_file_path}")
                return False
            
            # Read current content
            with open(self.report_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find insertion point after "## QA Execution History"
            history_marker = "## QA Execution History"
            history_pos = content.find(history_marker)
            
            if history_pos == -1:
                logger.error("Could not find QA Execution History section in report file")
                return False
            
            # Find the end of the history header (after the next line)
            next_newline = content.find('\n', history_pos)
            if next_newline == -1:
                return False
            
            # Insert new entry after the header
            insertion_point = next_newline + 1
            new_content = (
                content[:insertion_point] + 
                "\n" + report_entry + 
                content[insertion_point:]
            )
            
            # Update health trends and performance metrics
            new_content = self._update_trends_section(new_content, report_data)
            
            # Write updated content
            with open(self.report_file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            # Update last modified timestamp
            new_content = self._update_timestamp(new_content)
            
            with open(self.report_file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update report file: {e}")
            return False
    
    def _update_trends_section(self, content: str, report_data: Dict[str, Any]) -> str:
        """Update system health trends section."""
        try:
            # Update database health trend
            health_pattern = r"(### Database Health Over Time\n(?:- .*\n)*)"
            database_names = ", ".join([db.upper() for db in report_data['databases']])
            health_status = self._get_health_status_text(report_data['health_score'])
            
            new_health_entry = f"- {report_data['timestamp'][:10]}: {database_names} Health Score {report_data['health_score']}/100 ({health_status})\n"
            
            match = re.search(health_pattern, content)
            if match:
                existing_entries = match.group(1)
                updated_entries = existing_entries.rstrip() + "\n" + new_health_entry
                content = content.replace(existing_entries, updated_entries)
            
            # Update performance metrics
            perf_pattern = r"(### Performance Metrics\n(?:- .*\n)*)"
            new_perf_entry = f"- {report_data['timestamp'][:10]}: QA execution time {report_data['execution_time']:.1f}s ({report_data['total_checks']} checks)\n"
            
            match = re.search(perf_pattern, content)
            if match:
                existing_entries = match.group(1)
                updated_entries = existing_entries.rstrip() + "\n" + new_perf_entry
                content = content.replace(existing_entries, updated_entries)
            
            return content
            
        except Exception as e:
            logger.error(f"Failed to update trends section: {e}")
            return content
    
    def _get_health_status_text(self, health_score: float) -> str:
        """Get health status text based on score."""
        if health_score >= 90:
            return "EXCELLENT"
        elif health_score >= 70:
            return "GOOD"
        elif health_score >= 50:
            return "FAIR"
        elif health_score >= 30:
            return "POOR"
        else:
            return "CRITICAL"
    
    def _update_timestamp(self, content: str) -> str:
        """Update last modified timestamp at bottom of file."""
        timestamp_pattern = r"\*Last Updated: .*\*"
        new_timestamp = f"*Last Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*"
        
        return re.sub(timestamp_pattern, new_timestamp, content)
    
    def get_last_health_score(self, database: str) -> Optional[float]:
        """Get the last health score for a specific database."""
        try:
            if not self.report_file_path.exists():
                return None
            
            with open(self.report_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Look for health score pattern
            pattern = rf"- .*: {database.upper()} Health Score (\d+\.?\d*)/100"
            matches = re.findall(pattern, content)
            
            if matches:
                return float(matches[-1])  # Return most recent
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last health score: {e}")
            return None