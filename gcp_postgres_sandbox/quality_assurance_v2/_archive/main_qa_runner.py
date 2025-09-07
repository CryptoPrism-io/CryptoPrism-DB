"""
Main QA Runner for CryptoPrism-DB Quality Assurance system v2.
Orchestrates comprehensive database quality assurance across all modules.
"""

import argparse
import sys
import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

# Add the quality_assurance_v2 directory to Python path
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(current_dir))

# Import with proper module paths
try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.core.database import DatabaseManager
    from quality_assurance_v2.modules.performance_monitor import PerformanceMonitor
    from quality_assurance_v2.modules.data_integrity import DataIntegrityChecker
    from quality_assurance_v2.modules.index_analyzer import IndexAnalyzer
    from quality_assurance_v2.modules.pipeline_validator import PipelineValidator
    from quality_assurance_v2.reporting.report_generator import ReportGenerator
    from quality_assurance_v2.reporting.logging_system import QALoggingSystem
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
except ImportError:
    # Fallback to local imports if running from within the directory
    from core.config import QAConfig
    from core.database import DatabaseManager
    from modules.performance_monitor import PerformanceMonitor
    from modules.data_integrity import DataIntegrityChecker
    from modules.index_analyzer import IndexAnalyzer
    from modules.pipeline_validator import PipelineValidator
    from reporting.report_generator import ReportGenerator
    from reporting.logging_system import QALoggingSystem
    from reporting.notification_system import NotificationSystem

class QAOrchestrator:
    """
    Main orchestrator for the CryptoPrism-DB Quality Assurance system.
    Coordinates execution of all QA modules and generates comprehensive reports.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize QA orchestrator.
        
        Args:
            config_file: Path to custom configuration file
        """
        try:
            # Initialize core components
            self.config = QAConfig(config_file)
            self.db_manager = DatabaseManager(self.config)
            self.logging_system = QALoggingSystem()
            self.report_generator = ReportGenerator(self.config)
            self.notification_system = NotificationSystem(self.config)
            
            # Initialize QA modules
            self.modules = {
                'performance_monitor': PerformanceMonitor(self.config, self.db_manager),
                'data_integrity': DataIntegrityChecker(self.config, self.db_manager),
                'index_analyzer': IndexAnalyzer(self.config, self.db_manager),
                'pipeline_validator': PipelineValidator(self.config, self.db_manager)
            }
            
            # Execution metadata
            self.execution_metadata = {
                'start_time': None,
                'end_time': None,
                'total_execution_time': 0,
                'databases': [],
                'modules_executed': [],
                'version': '2.0.0'
            }
            
            self.logger = logging.getLogger('qa_orchestrator')
            self.logger.info("✅ QA Orchestrator initialized successfully")
            
        except Exception as e:
            print(f"❌ Failed to initialize QA Orchestrator: {e}")
            raise
    
    def run_comprehensive_qa(self, 
                           databases: Optional[List[str]] = None,
                           modules: Optional[List[str]] = None,
                           generate_reports: bool = True,
                           send_notifications: bool = True) -> Dict[str, Any]:
        """
        Run comprehensive quality assurance across specified databases and modules.
        
        Args:
            databases: List of databases to check (None for all configured)
            modules: List of modules to run (None for all modules)
            generate_reports: Whether to generate reports
            send_notifications: Whether to send notifications
            
        Returns:
            Comprehensive QA execution results
        """
        self.execution_metadata['start_time'] = datetime.utcnow()
        start_time = time.time()
        
        # Default to all configured databases and modules
        target_databases = databases or list(self.config.database_configs.keys())
        target_modules = modules or list(self.modules.keys())
        
        self.execution_metadata['databases'] = target_databases
        self.execution_metadata['modules_executed'] = target_modules
        
        # Start logging
        self.logging_system.log_qa_start(target_databases, target_modules)
        
        try:
            # Database health check
            self.logger.info("🔍 Performing database health checks")
            health_results = self._perform_health_checks(target_databases)
            
            # Run QA modules
            all_results = []
            module_summaries = {}
            
            for module_name in target_modules:
                if module_name not in self.modules:
                    self.logger.warning(f"⚠️ Unknown module: {module_name}")
                    continue
                
                self.logger.info(f"🚀 Executing module: {module_name}")
                module_start_time = time.time()
                
                try:
                    module = self.modules[module_name]
                    module_results = module.run_checks(target_databases)
                    
                    # Log module results to logging system
                    for result in module_results:
                        self.logging_system.log_check_result(
                            check_name=result.check_name,
                            status=result.status,
                            risk_level=result.risk_level,
                            message=result.message,
                            database=result.details.get('database', ''),
                            execution_time=result.metrics.get('execution_time_seconds'),
                            details=result.details
                        )
                        
                        # Send critical alerts immediately
                        if result.risk_level == 'CRITICAL':
                            if send_notifications:
                                self.notification_system.send_critical_issue_alert(result)
                    
                    all_results.extend(module_results)
                    
                    # Get module summary
                    module_summary = module.get_summary_statistics()
                    module_summaries[module_name] = module_summary
                    
                    module_execution_time = time.time() - module_start_time
                    issues_found = len([r for r in module_results if r.status in ['FAIL', 'WARNING']])
                    
                    self.logging_system.log_module_end(
                        module_name=module_name,
                        database=', '.join(target_databases),
                        checks_run=len(module_results),
                        execution_time=module_execution_time,
                        issues_found=issues_found
                    )
                    
                except Exception as e:
                    self.logger.error(f"❌ Module {module_name} failed: {e}")
                    # Continue with other modules
            
            # Finalize execution metadata
            end_time = time.time()
            self.execution_metadata['end_time'] = datetime.utcnow()
            self.execution_metadata['total_execution_time'] = end_time - start_time
            
            # Generate summary statistics
            results_summary = self._generate_results_summary(all_results)
            
            # End logging
            self.logging_system.log_qa_end(
                total_checks=len(all_results),
                execution_time=self.execution_metadata['total_execution_time'],
                results_summary=results_summary['status_distribution']
            )
            
            # Generate reports
            report_paths = {}
            if generate_reports:
                try:
                    report_paths = self.report_generator.generate_comprehensive_report(
                        all_results=all_results,
                        module_summaries=module_summaries,
                        execution_metadata=self.execution_metadata
                    )
                    self.logger.info(f"📋 Generated {len(report_paths)} report files")
                except Exception as e:
                    self.logger.error(f"❌ Report generation failed: {e}")
            
            # Send notifications
            notification_success = True
            if send_notifications:
                try:
                    notification_success = self.notification_system.send_qa_summary_alert(
                        results=all_results,
                        execution_metadata=self.execution_metadata
                    )
                except Exception as e:
                    self.logger.error(f"❌ Notification failed: {e}")
                    notification_success = False
            
            # Prepare final results
            final_results = {
                'execution_metadata': self.execution_metadata,
                'health_checks': health_results,
                'results_summary': results_summary,
                'module_summaries': module_summaries,
                'detailed_results': [result.to_dict() for result in all_results],
                'report_paths': report_paths,
                'notification_sent': notification_success,
                'logging_statistics': self.logging_system.get_log_statistics()
            }
            
            self.logger.info(f"✅ QA execution completed successfully in {self.execution_metadata['total_execution_time']:.2f}s")
            return final_results
            
        except Exception as e:
            self.logger.error(f"❌ QA execution failed: {e}")
            raise
        finally:
            # Clean up database connections
            self.db_manager.close_all_connections()
    
    def _perform_health_checks(self, databases: List[str]) -> Dict[str, Any]:
        """Perform basic health checks on target databases."""
        health_results = {}
        
        for database in databases:
            self.logger.debug(f"Health checking database: {database}")
            start_time = time.time()
            
            try:
                health_result = self.db_manager.health_check(database)
                success = health_result.get(database, {}).get('status') == 'healthy'
                response_time = time.time() - start_time
                
                health_results[database] = health_result.get(database, {})
                
                # Log connection attempt
                self.logging_system.log_database_connection(
                    database=database,
                    success=success,
                    response_time=response_time
                )
                
            except Exception as e:
                health_results[database] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'response_time_ms': (time.time() - start_time) * 1000
                }
                
                self.logging_system.log_database_connection(
                    database=database,
                    success=False,
                    response_time=time.time() - start_time
                )
        
        return health_results
    
    def _generate_results_summary(self, results: List) -> Dict[str, Any]:
        """Generate summary statistics from QA results."""
        if not results:
            return {'total_checks': 0}
        
        # Count by status and risk level
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        # Collect issues by severity
        critical_issues = []
        high_issues = []
        failed_checks = []
        
        for result in results:
            # Update counters
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
            
            # Collect significant issues
            if result.risk_level == 'CRITICAL':
                critical_issues.append({
                    'check_name': result.check_name,
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
            elif result.risk_level == 'HIGH':
                high_issues.append({
                    'check_name': result.check_name,
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
            elif result.status == 'FAIL':
                failed_checks.append({
                    'check_name': result.check_name,
                    'message': result.message,
                    'database': result.details.get('database', 'unknown')
                })
        
        # Calculate health score
        total_checks = len(results)
        health_score = (
            status_counts['PASS'] * 1.0 +
            status_counts['WARNING'] * 0.7 +
            status_counts['FAIL'] * 0.3 +
            status_counts['ERROR'] * 0.1
        ) / total_checks * 100 if total_checks > 0 else 0
        
        return {
            'total_checks': total_checks,
            'health_score': round(health_score, 1),
            'status_distribution': status_counts,
            'risk_distribution': risk_counts,
            'critical_issues_count': len(critical_issues),
            'high_issues_count': len(high_issues),
            'failed_checks_count': len(failed_checks),
            'critical_issues': critical_issues[:10],  # Limit for readability
            'high_issues': high_issues[:10],
            'failed_checks': failed_checks[:10]
        }
    
    def test_system_connectivity(self) -> Dict[str, Any]:
        """Test all system components for connectivity and functionality."""
        self.logger.info("🧪 Testing system connectivity")
        
        test_results = {
            'timestamp': datetime.utcnow().isoformat(),
            'databases': {},
            'notifications': {},
            'reporting': {'status': 'unknown'},
            'overall_status': 'unknown'
        }
        
        try:
            # Test database connections
            for database in self.config.database_configs.keys():
                try:
                    health_result = self.db_manager.health_check(database)
                    test_results['databases'][database] = {
                        'status': health_result.get(database, {}).get('status', 'unknown'),
                        'response_time': health_result.get(database, {}).get('response_time_ms', 0)
                    }
                except Exception as e:
                    test_results['databases'][database] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            # Test notification system
            try:
                notification_tests = self.notification_system.test_notifications()
                test_results['notifications'] = notification_tests
            except Exception as e:
                test_results['notifications'] = {'error': str(e)}
            
            # Test reporting system
            try:
                # Simple test by checking if reports directory is writable
                test_file = self.report_generator.reports_dir / "test_connectivity.tmp"
                test_file.write_text("test")
                test_file.unlink()
                test_results['reporting'] = {'status': 'working'}
            except Exception as e:
                test_results['reporting'] = {'status': 'failed', 'error': str(e)}
            
            # Determine overall status
            db_failures = len([r for r in test_results['databases'].values() if r['status'] != 'healthy'])
            notification_failures = len([v for v in test_results['notifications'].values() if not v])
            
            if db_failures == 0 and notification_failures == 0 and test_results['reporting']['status'] == 'working':
                test_results['overall_status'] = 'all_systems_operational'
            elif db_failures > 0:
                test_results['overall_status'] = 'database_issues'
            else:
                test_results['overall_status'] = 'partial_functionality'
            
            self.logger.info(f"System test completed: {test_results['overall_status']}")
            
        except Exception as e:
            test_results['overall_status'] = 'system_failure'
            test_results['error'] = str(e)
            self.logger.error(f"System test failed: {e}")
        
        return test_results
    
    def run_quick_health_check(self) -> Dict[str, Any]:
        """Run a quick health check focusing on critical systems only."""
        self.logger.info("⚡ Running quick health check")
        
        start_time = time.time()
        
        # Run minimal checks from each module
        quick_results = []
        
        # Performance check - basic connectivity only
        for database in self.config.database_configs.keys():
            health_result = self.db_manager.health_check(database)
            
            if health_result.get(database, {}).get('status') == 'healthy':
                quick_results.append({
                    'check': f'connectivity_{database}',
                    'status': 'PASS',
                    'risk_level': 'LOW',
                    'message': f'Database {database} is accessible'
                })
            else:
                quick_results.append({
                    'check': f'connectivity_{database}',
                    'status': 'FAIL',
                    'risk_level': 'CRITICAL',
                    'message': f'Database {database} is not accessible'
                })
        
        # Data integrity - check key tables exist
        for database in self.config.database_configs.keys():
            missing_tables = 0
            for table in self.config.key_tables:
                if not self._table_exists_quick(database, table):
                    missing_tables += 1
            
            if missing_tables == 0:
                quick_results.append({
                    'check': f'key_tables_{database}',
                    'status': 'PASS',
                    'risk_level': 'LOW',
                    'message': f'All key tables exist in {database}'
                })
            else:
                quick_results.append({
                    'check': f'key_tables_{database}',
                    'status': 'FAIL',
                    'risk_level': 'HIGH',
                    'message': f'{missing_tables} key tables missing in {database}'
                })
        
        execution_time = time.time() - start_time
        
        # Generate summary
        status_counts = {'PASS': 0, 'FAIL': 0}
        risk_counts = {'LOW': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in quick_results:
            status_counts[result['status']] = status_counts.get(result['status'], 0) + 1
            risk_counts[result['risk_level']] = risk_counts.get(result['risk_level'], 0) + 1
        
        health_score = (status_counts.get('PASS', 0) / len(quick_results) * 100) if quick_results else 0
        
        return {
            'execution_time': round(execution_time, 2),
            'health_score': round(health_score, 1),
            'total_checks': len(quick_results),
            'status_distribution': status_counts,
            'risk_distribution': risk_counts,
            'results': quick_results,
            'recommendation': self._get_quick_health_recommendation(health_score, risk_counts)
        }
    
    def _table_exists_quick(self, database: str, table_name: str) -> bool:
        """Quick table existence check without detailed validation."""
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                result = conn.execute(text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)"
                ), {'table_name': table_name}).scalar()
                return bool(result)
        except:
            return False
    
    def _get_quick_health_recommendation(self, health_score: float, risk_counts: Dict[str, int]) -> str:
        """Get recommendation based on quick health check results."""
        if health_score >= 95:
            return "✅ System is healthy - no immediate action needed"
        elif health_score >= 80:
            return "🟡 System is mostly healthy - monitor any warnings"
        elif risk_counts.get('CRITICAL', 0) > 0:
            return "🔴 CRITICAL issues detected - run full QA immediately"
        else:
            return "🟠 System issues detected - run comprehensive QA for detailed analysis"

def main():
    """Main entry point for QA system execution."""
    parser = argparse.ArgumentParser(description='CryptoPrism-DB Quality Assurance System v2')
    
    parser.add_argument('--config', '-c', 
                       help='Path to custom configuration file')
    parser.add_argument('--databases', '-d', nargs='+',
                       help='Databases to check (default: all configured)')
    parser.add_argument('--modules', '-m', nargs='+',
                       choices=['performance_monitor', 'data_integrity', 'index_analyzer', 'pipeline_validator'],
                       help='QA modules to run (default: all modules)')
    parser.add_argument('--no-reports', action='store_true',
                       help='Skip report generation')
    parser.add_argument('--no-notifications', action='store_true',
                       help='Skip notifications')
    parser.add_argument('--quick-check', action='store_true',
                       help='Run quick health check only')
    parser.add_argument('--test-connectivity', action='store_true',
                       help='Test system connectivity only')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize orchestrator
        qa_orchestrator = QAOrchestrator(args.config)
        
        if args.test_connectivity:
            # Test connectivity only
            print("Testing system connectivity...")
            results = qa_orchestrator.test_system_connectivity()
            print(f"Overall status: {results['overall_status']}")
            print(f"Database status: {list(results['databases'].keys())}")
            print(f"Notification status: {results['notifications']}")
            
        elif args.quick_check:
            # Quick health check only
            print("Running quick health check...")
            results = qa_orchestrator.run_quick_health_check()
            print(f"Health Score: {results['health_score']}/100")
            print(f"Execution Time: {results['execution_time']}s")
            print(f"Recommendation: {results['recommendation']}")
            
        else:
            # Full QA execution
            print("Starting comprehensive QA execution...")
            results = qa_orchestrator.run_comprehensive_qa(
                databases=args.databases,
                modules=args.modules,
                generate_reports=not args.no_reports,
                send_notifications=not args.no_notifications
            )
            
            # Print summary
            summary = results['results_summary']
            print(f"\nQA Execution Complete")
            print(f"Health Score: {summary['health_score']}/100")
            print(f"Total Checks: {summary['total_checks']}")
            print(f"Critical Issues: {summary['critical_issues_count']}")
            print(f"High-Risk Issues: {summary['high_issues_count']}")
            print(f"Execution Time: {results['execution_metadata']['total_execution_time']:.2f}s")
            
            if results['report_paths']:
                print(f"Reports generated: {len(results['report_paths'])}")
                for report_type, path in results['report_paths'].items():
                    print(f"  {report_type}: {path}")
            
            # Exit with error code if critical issues found
            if summary['critical_issues_count'] > 0:
                sys.exit(1)
        
    except KeyboardInterrupt:
        print("\nQA execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"QA execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()