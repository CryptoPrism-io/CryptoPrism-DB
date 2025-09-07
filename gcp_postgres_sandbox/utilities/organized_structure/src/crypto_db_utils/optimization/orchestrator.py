#!/usr/bin/env python3
"""
Database Optimization Orchestrator for CryptoPrism-DB

Master script that orchestrates the complete database optimization workflow:
1. Extract database schema
2. Run pre-optimization benchmarks  
3. Generate optimization scripts
4. (Manual step: Execute optimization scripts)
5. Run post-optimization benchmarks
6. Analyze performance improvements

This script coordinates all optimization tools and provides a complete workflow
for measuring and implementing database performance improvements.

Features:
- Complete end-to-end optimization workflow
- Automated benchmark comparison
- Performance improvement reporting
- Time savings calculation and ROI analysis
- Comprehensive logging and error handling

Usage:
  python db_optimization_orchestrator.py --phase [extract|benchmark|optimize|analyze]
  python db_optimization_orchestrator.py --full-workflow
"""

import os
import sys
import json
import logging
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import our optimization modules
from schema_extractor import DatabaseSchemaExtractor
from query_benchmarker import QueryBenchmarker
from optimization_generator import OptimizationGenerator
from performance_analyzer import PerformanceAnalyzer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DatabaseOptimizationOrchestrator:
    """Orchestrate complete database optimization workflow."""
    
    def __init__(self):
        """Initialize orchestrator with default settings."""
        self.base_dir = Path.cwd()
        self.output_dir = self.base_dir / "database_analysis"
        self.output_dir.mkdir(exist_ok=True)
        
        self.workflow_state_file = self.output_dir / "optimization_workflow_state.json"
        
        # Initialize optimization tools
        self.schema_extractor = DatabaseSchemaExtractor()
        self.query_benchmarker = QueryBenchmarker()
        self.optimization_generator = OptimizationGenerator()
        self.performance_analyzer = PerformanceAnalyzer()
        
        # Load or initialize workflow state
        self.workflow_state = self.load_workflow_state()
    
    def load_workflow_state(self) -> Dict[str, Any]:
        """Load workflow state from file or initialize new state."""
        if self.workflow_state_file.exists():
            try:
                with open(self.workflow_state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load workflow state: {e}")
        
        # Initialize new workflow state
        return {
            'workflow_id': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'created': datetime.now().isoformat(),
            'phases_completed': [],
            'files_generated': {},
            'current_phase': None,
            'last_updated': datetime.now().isoformat()
        }
    
    def save_workflow_state(self):
        """Save current workflow state to file."""
        self.workflow_state['last_updated'] = datetime.now().isoformat()
        
        with open(self.workflow_state_file, 'w', encoding='utf-8') as f:
            json.dump(self.workflow_state, f, indent=2, ensure_ascii=False)
    
    def phase_extract_schema(self) -> str:
        """Phase 1: Extract database schema analysis."""
        logger.info("🔍 Phase 1: Extracting database schema")
        self.workflow_state['current_phase'] = 'extract_schema'
        
        try:
            # Extract schema from all databases
            analysis = self.schema_extractor.extract_all_schemas()
            
            # Save schema analysis
            schema_filepath = self.schema_extractor.save_to_json(analysis, str(self.output_dir))
            
            # Generate summary report
            summary = self.schema_extractor.generate_summary_report(analysis)
            print("\n" + summary)
            
            # Update workflow state
            self.workflow_state['phases_completed'].append('extract_schema')
            self.workflow_state['files_generated']['schema_analysis'] = schema_filepath
            self.save_workflow_state()
            
            logger.info(f"✅ Schema extraction completed: {schema_filepath}")
            return schema_filepath
            
        except Exception as e:
            logger.error(f"❌ Schema extraction failed: {e}")
            raise
    
    def phase_pre_optimization_benchmark(self) -> str:
        """Phase 2: Run pre-optimization performance benchmarks."""
        logger.info("⏱️  Phase 2: Running pre-optimization benchmarks")
        self.workflow_state['current_phase'] = 'pre_benchmark'
        
        try:
            # Run comprehensive benchmark suite
            results = self.query_benchmarker.run_full_benchmark_suite()
            
            # Save benchmark results
            benchmark_filepath = self.query_benchmarker.save_benchmark_results(results, str(self.output_dir))
            
            # Generate benchmark report
            report = self.query_benchmarker.generate_benchmark_report(results)
            print("\n" + report)
            
            # Update workflow state
            self.workflow_state['phases_completed'].append('pre_benchmark')
            self.workflow_state['files_generated']['pre_benchmark'] = benchmark_filepath
            self.save_workflow_state()
            
            logger.info(f"✅ Pre-optimization benchmarking completed: {benchmark_filepath}")
            return benchmark_filepath
            
        except Exception as e:
            logger.error(f"❌ Pre-optimization benchmarking failed: {e}")
            raise
    
    def phase_generate_optimizations(self) -> Dict[str, str]:
        """Phase 3: Generate optimization scripts."""
        logger.info("🛠️  Phase 3: Generating optimization scripts")
        self.workflow_state['current_phase'] = 'generate_optimizations'
        
        try:
            # Get schema analysis file
            schema_file = self.workflow_state['files_generated'].get('schema_analysis')
            if not schema_file:
                raise ValueError("Schema analysis file not found. Run extract_schema phase first.")
            
            # Generate optimization scripts
            sql_output_dir = self.base_dir / "sql_optimizations"
            generated_files = self.optimization_generator.generate_all_optimization_scripts(
                schema_file, str(sql_output_dir)
            )
            
            print("\n" + "=" * 80)
            print("DATABASE OPTIMIZATION SCRIPTS GENERATED")
            print("=" * 80)
            
            for script_type, filepath in generated_files.items():
                print(f"✅ {script_type}: {filepath}")
            
            print("\n📋 NEXT STEPS:")
            print("1. Review the generated SQL scripts carefully")
            print("2. Test on a backup database first")
            print("3. Execute primary key script first")
            print("4. Execute index script second")
            print("5. Run post-optimization benchmarks")
            print("6. Use analyzer to compare performance")
            
            # Update workflow state
            self.workflow_state['phases_completed'].append('generate_optimizations')
            self.workflow_state['files_generated']['optimization_scripts'] = generated_files
            self.save_workflow_state()
            
            logger.info(f"✅ Optimization script generation completed")
            return generated_files
            
        except Exception as e:
            logger.error(f"❌ Optimization script generation failed: {e}")
            raise
    
    def phase_post_optimization_benchmark(self) -> str:
        """Phase 4: Run post-optimization performance benchmarks."""
        logger.info("⚡ Phase 4: Running post-optimization benchmarks")
        self.workflow_state['current_phase'] = 'post_benchmark'
        
        try:
            # Run comprehensive benchmark suite
            results = self.query_benchmarker.run_full_benchmark_suite()
            
            # Save benchmark results with post-optimization identifier
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_benchmark_results_post_optimization_{timestamp}.json"
            filepath = self.output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            # Generate benchmark report
            report = self.query_benchmarker.generate_benchmark_report(results)
            print("\n" + report)
            
            # Update workflow state
            self.workflow_state['phases_completed'].append('post_benchmark')
            self.workflow_state['files_generated']['post_benchmark'] = str(filepath)
            self.save_workflow_state()
            
            logger.info(f"✅ Post-optimization benchmarking completed: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ Post-optimization benchmarking failed: {e}")
            raise
    
    def phase_analyze_performance(self) -> str:
        """Phase 5: Analyze performance improvements."""
        logger.info("📊 Phase 5: Analyzing performance improvements")
        self.workflow_state['current_phase'] = 'analyze_performance'
        
        try:
            # Get benchmark files
            pre_benchmark_file = self.workflow_state['files_generated'].get('pre_benchmark')
            post_benchmark_file = self.workflow_state['files_generated'].get('post_benchmark')
            
            if not pre_benchmark_file or not post_benchmark_file:
                raise ValueError("Benchmark files not found. Run benchmark phases first.")
            
            # Generate detailed performance analysis
            analysis_filepath = self.performance_analyzer.generate_detailed_analysis_report(
                pre_benchmark_file, post_benchmark_file, str(self.output_dir)
            )
            
            # Generate and display performance report
            report = self.performance_analyzer.generate_performance_report(analysis_filepath)
            print("\n" + report)
            
            # Save report as well
            report_path = Path(analysis_filepath).parent / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
            
            # Update workflow state
            self.workflow_state['phases_completed'].append('analyze_performance')
            self.workflow_state['files_generated']['performance_analysis'] = analysis_filepath
            self.workflow_state['files_generated']['performance_report'] = str(report_path)
            self.save_workflow_state()
            
            logger.info(f"✅ Performance analysis completed: {analysis_filepath}")
            return analysis_filepath
            
        except Exception as e:
            logger.error(f"❌ Performance analysis failed: {e}")
            raise
    
    def run_full_workflow(self):
        """Run complete optimization workflow (pre-optimization phases only)."""
        logger.info("🚀 Starting complete database optimization workflow")
        
        print("=" * 80)
        print("DATABASE OPTIMIZATION WORKFLOW")
        print("=" * 80)
        print(f"Workflow ID: {self.workflow_state['workflow_id']}")
        print(f"Started: {datetime.now().isoformat()}")
        print("")
        
        try:
            # Phase 1: Extract schema
            if 'extract_schema' not in self.workflow_state['phases_completed']:
                self.phase_extract_schema()
            else:
                logger.info("✅ Schema extraction already completed")
            
            # Phase 2: Pre-optimization benchmark
            if 'pre_benchmark' not in self.workflow_state['phases_completed']:
                self.phase_pre_optimization_benchmark()
            else:
                logger.info("✅ Pre-optimization benchmark already completed")
            
            # Phase 3: Generate optimization scripts
            if 'generate_optimizations' not in self.workflow_state['phases_completed']:
                self.phase_generate_optimizations()
            else:
                logger.info("✅ Optimization scripts already generated")
            
            print("\n" + "=" * 80)
            print("PRE-OPTIMIZATION WORKFLOW COMPLETED")
            print("=" * 80)
            print("📋 MANUAL STEPS REQUIRED:")
            print("1. Review generated SQL optimization scripts")
            print("2. Test scripts on backup database")
            print("3. Execute optimization scripts on production")
            print("4. Run post-optimization phases:")
            print("   python db_optimization_orchestrator.py --phase post-benchmark")
            print("   python db_optimization_orchestrator.py --phase analyze")
            print("")
            
            logger.info("🎉 Pre-optimization workflow completed successfully!")
            
        except Exception as e:
            logger.error(f"💥 Workflow failed: {e}")
            raise
    
    def run_post_optimization_workflow(self):
        """Run post-optimization workflow phases."""
        logger.info("⚡ Starting post-optimization workflow")
        
        try:
            # Phase 4: Post-optimization benchmark
            if 'post_benchmark' not in self.workflow_state['phases_completed']:
                self.phase_post_optimization_benchmark()
            else:
                logger.info("✅ Post-optimization benchmark already completed")
            
            # Phase 5: Analyze performance
            if 'analyze_performance' not in self.workflow_state['phases_completed']:
                self.phase_analyze_performance()
            else:
                logger.info("✅ Performance analysis already completed")
            
            print("\n" + "=" * 80)
            print("COMPLETE OPTIMIZATION WORKFLOW FINISHED")
            print("=" * 80)
            logger.info("🎉 Complete optimization workflow finished successfully!")
            
        except Exception as e:
            logger.error(f"💥 Post-optimization workflow failed: {e}")
            raise
    
    def show_workflow_status(self):
        """Display current workflow status."""
        print("=" * 80)
        print("DATABASE OPTIMIZATION WORKFLOW STATUS")
        print("=" * 80)
        print(f"Workflow ID: {self.workflow_state['workflow_id']}")
        print(f"Created: {self.workflow_state['created']}")
        print(f"Last Updated: {self.workflow_state['last_updated']}")
        print(f"Current Phase: {self.workflow_state.get('current_phase', 'None')}")
        print("")
        
        print("PHASES COMPLETED:")
        if self.workflow_state['phases_completed']:
            for phase in self.workflow_state['phases_completed']:
                print(f"  ✅ {phase}")
        else:
            print("  No phases completed yet")
        print("")
        
        print("GENERATED FILES:")
        if self.workflow_state['files_generated']:
            for file_type, filepath in self.workflow_state['files_generated'].items():
                if isinstance(filepath, dict):
                    print(f"  📁 {file_type}:")
                    for script_name, script_path in filepath.items():
                        print(f"    - {script_name}: {script_path}")
                else:
                    print(f"  📄 {file_type}: {filepath}")
        else:
            print("  No files generated yet")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Database Optimization Orchestrator for CryptoPrism-DB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pre-optimization workflow
  python db_optimization_orchestrator.py --full-workflow
  
  # Run individual phases
  python db_optimization_orchestrator.py --phase extract
  python db_optimization_orchestrator.py --phase benchmark
  python db_optimization_orchestrator.py --phase optimize
  
  # After manual optimization execution
  python db_optimization_orchestrator.py --phase post-benchmark
  python db_optimization_orchestrator.py --phase analyze
  
  # Check workflow status
  python db_optimization_orchestrator.py --status
        """
    )
    
    parser.add_argument('--phase', choices=['extract', 'benchmark', 'optimize', 'post-benchmark', 'analyze'],
                       help='Run specific optimization phase')
    parser.add_argument('--full-workflow', action='store_true',
                       help='Run complete pre-optimization workflow')
    parser.add_argument('--post-workflow', action='store_true',
                       help='Run post-optimization workflow')
    parser.add_argument('--status', action='store_true',
                       help='Show current workflow status')
    
    args = parser.parse_args()
    
    try:
        orchestrator = DatabaseOptimizationOrchestrator()
        
        if args.status:
            orchestrator.show_workflow_status()
        elif args.full_workflow:
            orchestrator.run_full_workflow()
        elif args.post_workflow:
            orchestrator.run_post_optimization_workflow()
        elif args.phase:
            if args.phase == 'extract':
                orchestrator.phase_extract_schema()
            elif args.phase == 'benchmark':
                orchestrator.phase_pre_optimization_benchmark()
            elif args.phase == 'optimize':
                orchestrator.phase_generate_optimizations()
            elif args.phase == 'post-benchmark':
                orchestrator.phase_post_optimization_benchmark()
            elif args.phase == 'analyze':
                orchestrator.phase_analyze_performance()
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()