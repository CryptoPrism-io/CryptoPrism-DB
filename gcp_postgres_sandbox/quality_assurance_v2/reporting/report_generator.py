"""
Report Generator for CryptoPrism-DB QA system v2.
Generates comprehensive JSON and CSV reports with historical tracking.
"""

import json
import csv
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from pathlib import Path
import os

from ..core.base_qa import QAResult
from ..core.config import QAConfig

logger = logging.getLogger(__name__)

class ReportGenerator:
    """
    Generates comprehensive QA reports in multiple formats.
    Supports JSON, CSV export and historical report management.
    """
    
    def __init__(self, config: QAConfig, reports_dir: Optional[str] = None):
        """
        Initialize report generator.
        
        Args:
            config: QA configuration instance
            reports_dir: Directory for storing reports (defaults to quality_assurance_v2/reports)
        """
        self.config = config
        
        # Set up reports directory
        if reports_dir:
            self.reports_dir = Path(reports_dir)
        else:
            # Default to reports directory relative to this module
            module_dir = Path(__file__).parent.parent
            self.reports_dir = module_dir / "reports"
        
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Report metadata
        self.report_timestamp = datetime.now(timezone.utc)
        self.report_id = self.report_timestamp.strftime("%Y%m%d_%H%M%S")
    
    def generate_comprehensive_report(self, 
                                     all_results: List[QAResult],
                                     module_summaries: Dict[str, Dict[str, Any]],
                                     execution_metadata: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate comprehensive QA report in multiple formats.
        
        Args:
            all_results: All QA results from all modules
            module_summaries: Summary statistics for each module
            execution_metadata: Metadata about QA execution
            
        Returns:
            Dictionary with paths to generated reports
        """
        logger.info(f"Generating comprehensive QA report with {len(all_results)} results")
        
        # Prepare consolidated report data
        report_data = self._prepare_report_data(all_results, module_summaries, execution_metadata)
        
        # Generate different report formats
        report_paths = {}
        
        try:
            # JSON detailed report
            json_path = self._generate_json_report(report_data)
            report_paths['json'] = str(json_path)
            
            # CSV summary report
            csv_path = self._generate_csv_report(all_results)
            report_paths['csv'] = str(csv_path)
            
            # Executive summary report
            summary_path = self._generate_executive_summary(report_data)
            report_paths['summary'] = str(summary_path)
            
            # Historical comparison
            historical_path = self._update_historical_tracking(report_data)
            if historical_path:
                report_paths['historical'] = str(historical_path)
            
            logger.info(f"✅ Generated {len(report_paths)} report files in {self.reports_dir}")
            
        except Exception as e:
            logger.error(f"❌ Failed to generate reports: {e}")
            raise
        
        return report_paths
    
    def _prepare_report_data(self, 
                            all_results: List[QAResult],
                            module_summaries: Dict[str, Dict[str, Any]],
                            execution_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare consolidated report data structure."""
        
        # Categorize results by risk level and status
        risk_distribution = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        status_distribution = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        
        results_by_database = {}
        results_by_module = {}
        critical_issues = []
        high_issues = []
        
        for result in all_results:
            # Update distributions
            risk_distribution[result.risk_level] += 1
            status_distribution[result.status] += 1
            
            # Group by database
            if 'database' in result.details:
                db_name = result.details['database']
                if db_name not in results_by_database:
                    results_by_database[db_name] = []
                results_by_database[db_name].append(result.to_dict())
            
            # Group by module
            module_name = result.check_name.split('.')[0]
            if module_name not in results_by_module:
                results_by_module[module_name] = []
            results_by_module[module_name].append(result.to_dict())
            
            # Collect critical and high-risk issues
            if result.risk_level == 'CRITICAL':
                critical_issues.append({
                    'check_name': result.check_name,
                    'message': result.message,
                    'database': result.details.get('database', 'unknown'),
                    'module': module_name
                })
            elif result.risk_level == 'HIGH':
                high_issues.append({
                    'check_name': result.check_name,
                    'message': result.message,
                    'database': result.details.get('database', 'unknown'),
                    'module': module_name
                })
        
        # Calculate overall health score (0-100)
        total_checks = len(all_results)
        if total_checks > 0:
            # Weight different outcomes
            score = (
                status_distribution['PASS'] * 1.0 +
                status_distribution['WARNING'] * 0.7 +
                status_distribution['FAIL'] * 0.3 +
                status_distribution['ERROR'] * 0.1
            ) / total_checks * 100
        else:
            score = 0
        
        return {
            'report_metadata': {
                'report_id': self.report_id,
                'generated_at': self.report_timestamp.isoformat(),
                'qa_system_version': '2.0.0',
                'total_checks': total_checks,
                'overall_health_score': round(score, 1)
            },
            'execution_summary': execution_metadata,
            'results_overview': {
                'risk_distribution': risk_distribution,
                'status_distribution': status_distribution,
                'databases_analyzed': list(results_by_database.keys()),
                'modules_executed': list(module_summaries.keys())
            },
            'critical_issues': critical_issues,
            'high_risk_issues': high_issues,
            'module_summaries': module_summaries,
            'results_by_database': results_by_database,
            'results_by_module': results_by_module,
            'detailed_results': [result.to_dict() for result in all_results]
        }
    
    def _generate_json_report(self, report_data: Dict[str, Any]) -> Path:
        """Generate comprehensive JSON report."""
        json_filename = f"qa_report_{self.report_id}.json"
        json_path = self.reports_dir / json_filename
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str, ensure_ascii=False)
        
        logger.debug(f"Generated JSON report: {json_path}")
        return json_path
    
    def _generate_csv_report(self, all_results: List[QAResult]) -> Path:
        """Generate CSV summary report for spreadsheet analysis."""
        csv_filename = f"qa_summary_{self.report_id}.csv"
        csv_path = self.reports_dir / csv_filename
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Headers
            headers = [
                'Timestamp', 'Check Name', 'Module', 'Database', 'Status', 'Risk Level',
                'Message', 'Execution Time', 'Row Count', 'Error Details'
            ]
            writer.writerow(headers)
            
            # Data rows
            for result in all_results:
                module_name = result.check_name.split('.')[0]
                database = result.details.get('database', '')
                execution_time = result.metrics.get('execution_time_seconds', '')
                row_count = result.metrics.get('row_count', '')
                error_details = result.details.get('error', '') if result.status == 'ERROR' else ''
                
                row = [
                    result.timestamp,
                    result.check_name,
                    module_name,
                    database,
                    result.status,
                    result.risk_level,
                    result.message,
                    execution_time,
                    row_count,
                    error_details
                ]
                writer.writerow(row)
        
        logger.debug(f"Generated CSV report: {csv_path}")
        return csv_path
    
    def _generate_executive_summary(self, report_data: Dict[str, Any]) -> Path:
        """Generate executive summary report in plain text format."""
        summary_filename = f"qa_executive_summary_{self.report_id}.txt"
        summary_path = self.reports_dir / summary_filename
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("CryptoPrism-DB Quality Assurance Executive Summary\\n")
            f.write("=" * 50 + "\\n\\n")
            
            # Basic metrics
            metadata = report_data['report_metadata']
            overview = report_data['results_overview']
            
            f.write(f"Report Generated: {metadata['generated_at']}\\n")
            f.write(f"Report ID: {metadata['report_id']}\\n")
            f.write(f"Overall Health Score: {metadata['overall_health_score']}/100\\n\\n")
            
            f.write(f"Total Checks Performed: {metadata['total_checks']}\\n")
            f.write(f"Databases Analyzed: {', '.join(overview['databases_analyzed'])}\\n")
            f.write(f"QA Modules Executed: {', '.join(overview['modules_executed'])}\\n\\n")
            
            # Status distribution
            f.write("Status Distribution:\\n")
            for status, count in overview['status_distribution'].items():
                percentage = count / metadata['total_checks'] * 100 if metadata['total_checks'] > 0 else 0
                f.write(f"  {status}: {count} ({percentage:.1f}%)\\n")
            f.write("\\n")
            
            # Risk distribution
            f.write("Risk Level Distribution:\\n")
            for risk, count in overview['risk_distribution'].items():
                percentage = count / metadata['total_checks'] * 100 if metadata['total_checks'] > 0 else 0
                f.write(f"  {risk}: {count} ({percentage:.1f}%)\\n")
            f.write("\\n")
            
            # Critical issues
            critical_issues = report_data['critical_issues']
            if critical_issues:
                f.write(f"CRITICAL ISSUES ({len(critical_issues)}):\\n")
                f.write("-" * 30 + "\\n")
                for issue in critical_issues[:10]:  # Limit to top 10
                    f.write(f"• {issue['message']} [{issue['database']}]\\n")
                    f.write(f"  Check: {issue['check_name']}\\n\\n")
            
            # High-risk issues summary
            high_issues = report_data['high_risk_issues']
            if high_issues:
                f.write(f"HIGH-RISK ISSUES ({len(high_issues)}):\\n")
                f.write("-" * 30 + "\\n")
                for issue in high_issues[:5]:  # Limit to top 5
                    f.write(f"• {issue['message']} [{issue['database']}]\\n")
            
            # Module performance summary
            f.write("\\nModule Performance Summary:\\n")
            f.write("-" * 30 + "\\n")
            for module, summary in report_data['module_summaries'].items():
                execution_time = summary.get('execution_time_seconds', 'N/A')
                total_checks = summary.get('total_checks', 0)
                alerts = summary.get('alerts_triggered', 0)
                f.write(f"{module}: {total_checks} checks, {alerts} alerts, {execution_time}s\\n")
            
            # Recommendations
            f.write("\\nRecommendations:\\n")
            f.write("-" * 30 + "\\n")
            
            if critical_issues:
                f.write(f"🔴 IMMEDIATE ACTION: Address {len(critical_issues)} critical issues\\n")
            
            if high_issues:
                f.write(f"🟠 HIGH PRIORITY: Review {len(high_issues)} high-risk findings\\n")
            
            health_score = metadata['overall_health_score']
            if health_score < 70:
                f.write("🟡 SYSTEM HEALTH: Overall health score below 70% - comprehensive review needed\\n")
            elif health_score < 90:
                f.write("🟢 SYSTEM HEALTH: Good overall health - monitor ongoing trends\\n")
            else:
                f.write("✅ SYSTEM HEALTH: Excellent system health - maintain current practices\\n")
        
        logger.debug(f"Generated executive summary: {summary_path}")
        return summary_path
    
    def _update_historical_tracking(self, report_data: Dict[str, Any]) -> Optional[Path]:
        """Update historical tracking with current report data."""
        historical_filename = "qa_historical_trends.json"
        historical_path = self.reports_dir / historical_filename
        
        try:
            # Load existing historical data
            historical_data = []
            if historical_path.exists():
                with open(historical_path, 'r', encoding='utf-8') as f:
                    historical_data = json.load(f)
            
            # Prepare current report summary for historical tracking
            current_summary = {
                'report_id': report_data['report_metadata']['report_id'],
                'timestamp': report_data['report_metadata']['generated_at'],
                'health_score': report_data['report_metadata']['overall_health_score'],
                'total_checks': report_data['report_metadata']['total_checks'],
                'critical_count': report_data['results_overview']['risk_distribution']['CRITICAL'],
                'high_count': report_data['results_overview']['risk_distribution']['HIGH'],
                'failed_count': report_data['results_overview']['status_distribution']['FAIL'],
                'databases': report_data['results_overview']['databases_analyzed'],
                'modules': list(report_data['module_summaries'].keys())
            }
            
            # Add to historical data
            historical_data.append(current_summary)
            
            # Keep only last 50 reports to prevent file growth
            if len(historical_data) > 50:
                historical_data = historical_data[-50:]
            
            # Save updated historical data
            with open(historical_path, 'w', encoding='utf-8') as f:
                json.dump(historical_data, f, indent=2, default=str)
            
            logger.debug(f"Updated historical tracking: {historical_path}")
            return historical_path
            
        except Exception as e:
            logger.warning(f"Failed to update historical tracking: {e}")
            return None
    
    def get_trend_analysis(self, lookback_days: int = 7) -> Dict[str, Any]:
        """
        Analyze QA trends over specified time period.
        
        Args:
            lookback_days: Number of days to analyze
            
        Returns:
            Trend analysis data
        """
        historical_path = self.reports_dir / "qa_historical_trends.json"
        
        if not historical_path.exists():
            return {'error': 'No historical data available'}
        
        try:
            with open(historical_path, 'r', encoding='utf-8') as f:
                historical_data = json.load(f)
            
            # Filter data within lookback period
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            recent_data = []
            
            for entry in historical_data:
                entry_date = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                if entry_date >= cutoff_date:
                    recent_data.append(entry)
            
            if len(recent_data) < 2:
                return {'error': 'Insufficient data for trend analysis'}
            
            # Calculate trends
            health_scores = [entry['health_score'] for entry in recent_data]
            critical_counts = [entry['critical_count'] for entry in recent_data]
            
            trends = {
                'period_days': lookback_days,
                'reports_analyzed': len(recent_data),
                'health_score_trend': {
                    'current': health_scores[-1],
                    'previous': health_scores[0],
                    'change': health_scores[-1] - health_scores[0],
                    'average': sum(health_scores) / len(health_scores)
                },
                'critical_issues_trend': {
                    'current': critical_counts[-1],
                    'previous': critical_counts[0],
                    'change': critical_counts[-1] - critical_counts[0],
                    'average': sum(critical_counts) / len(critical_counts)
                }
            }
            
            return trends
            
        except Exception as e:
            logger.error(f"Failed to generate trend analysis: {e}")
            return {'error': str(e)}
    
    def cleanup_old_reports(self, retention_days: int = 30):
        """Clean up old report files to manage disk space."""
        logger.info(f"Cleaning up reports older than {retention_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0
        
        for file_path in self.reports_dir.glob("qa_*"):
            try:
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                if file_mtime < cutoff_date and file_path.name != "qa_historical_trends.json":
                    file_path.unlink()
                    deleted_count += 1
                    
            except Exception as e:
                logger.warning(f"Failed to delete {file_path}: {e}")
        
        logger.info(f"Cleaned up {deleted_count} old report files")