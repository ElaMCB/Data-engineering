"""
Quality Reporter
Generates quality reports and dashboards
"""

from typing import Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession
import structlog

logger = structlog.get_logger()


class QualityReporter:
    """
    Generates data quality reports and dashboards
    
    Features:
    - Validation summary reports
    - Quality trend analysis
    - HTML/PDF report generation
    - Dashboard metrics
    """
    
    def __init__(self, spark: SparkSession = None):
        """
        Initialize Quality Reporter
        
        Args:
            spark: Optional SparkSession instance
        """
        self.spark = spark
        self.logger = logger.bind(component="QualityReporter")
    
    def generate_report(self, validation_results: Dict[str, Any]) -> str:
        """
        Generate HTML quality report
        
        Args:
            validation_results: Results from QualityValidator.validate()
            
        Returns:
            HTML report as string
        """
        self.logger.info("Generating quality report")
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; }}
                .summary {{ margin: 20px 0; }}
                .metric {{ display: inline-block; margin: 10px; padding: 15px; background-color: #f0f0f0; border-radius: 5px; }}
                .passed {{ color: green; }}
                .failed {{ color: red; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Quality Report</h1>
                <p>Generated: {validation_results.get('timestamp', datetime.now().isoformat())}</p>
            </div>
            
            <div class="summary">
                <h2>Summary</h2>
                <div class="metric">
                    <strong>Overall Status:</strong>
                    <span class="{'passed' if validation_results.get('overall_passed') else 'failed'}">
                        {'PASSED' if validation_results.get('overall_passed') else 'FAILED'}
                    </span>
                </div>
                <div class="metric">
                    <strong>Total Records:</strong> {validation_results.get('total_records', 0):,}
                </div>
                <div class="metric">
                    <strong>Pass Rate:</strong> {validation_results.get('overall_pass_rate', 0):.2f}%
                </div>
                <div class="metric">
                    <strong>Rules Executed:</strong> {validation_results.get('rules_executed', 0)}
                </div>
                <div class="metric">
                    <strong>Rules Passed:</strong> {validation_results.get('rules_passed', 0)}
                </div>
                <div class="metric">
                    <strong>Rules Failed:</strong> {validation_results.get('rules_failed', 0)}
                </div>
            </div>
            
            <div class="summary">
                <h2>Validation Results</h2>
                <table>
                    <tr>
                        <th>Rule Name</th>
                        <th>Status</th>
                        <th>Failed Count</th>
                        <th>Pass Rate</th>
                        <th>Execution Time (s)</th>
                        <th>Error Message</th>
                    </tr>
        """
        
        for result in validation_results.get('results', []):
            status_class = 'passed' if result.get('passed') else 'failed'
            status_text = 'PASSED' if result.get('passed') else 'FAILED'
            
            html += f"""
                    <tr>
                        <td>{result.get('rule_name', 'N/A')}</td>
                        <td class="{status_class}">{status_text}</td>
                        <td>{result.get('failed_count', 0):,}</td>
                        <td>{result.get('pass_rate', 0):.2f}%</td>
                        <td>{result.get('execution_time', 0):.2f}</td>
                        <td>{result.get('error_message', '') or ''}</td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def save_report(self, validation_results: Dict[str, Any], output_path: str):
        """
        Save quality report to file
        
        Args:
            validation_results: Results from QualityValidator.validate()
            output_path: Path to save report
        """
        html = self.generate_report(validation_results)
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        self.logger.info("Quality report saved", path=output_path)

