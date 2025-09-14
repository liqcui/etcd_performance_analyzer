"""
Deep Drive ELT module for ETCD Analyzer Performance Deep Drive Analysis
Extract, Load, Transform module for deep drive performance data
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class deepDriveELT(utilityELT):
    """Extract, Load, Transform class for deep drive performance analysis data"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
    
    def extract_deep_drive(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deep drive performance data from JSON result"""
        try:
            structured_data = {
                'test_metadata': {},
                'general_info_metrics': [],
                'wal_fsync_metrics': [],
                'disk_io_metrics': [],
                'network_metrics': [],
                'backend_commit_metrics': [],
                'compact_defrag_metrics': [],
                'analysis_results': {},
                'summary_info': {}
            }
            
            # Extract test metadata
            structured_data['test_metadata'] = {
                'test_id': data.get('test_id', 'unknown'),
                'timestamp': data.get('timestamp', 'unknown'),
                'duration': data.get('duration', 'unknown'),
                'category': data.get('category', 'performance_deep_drive'),
                'status': data.get('status', 'unknown')
            }
            
            # Extract data sections
            data_section = data.get('data', {})
            
            # Extract general info metrics
            general_info_data = data_section.get('general_info_data', [])
            for metric in general_info_data:
                structured_data['general_info_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract WAL fsync metrics
            wal_fsync_data = data_section.get('wal_fsync_data', [])
            for metric in wal_fsync_data:
                structured_data['wal_fsync_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract disk I/O metrics
            disk_io_data = data_section.get('disk_io_data', [])
            for metric in disk_io_data:
                structured_data['disk_io_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'node_name': metric.get('node_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'devices': metric.get('devices', [])
                })
            
            # Extract network metrics
            network_data = data_section.get('network_data', {})
            
            # Pod metrics
            pod_metrics = network_data.get('pod_metrics', [])
            for metric in pod_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'node_name': None,
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'pod'
                })
            
            # Node metrics
            node_metrics = network_data.get('node_metrics', [])
            for metric in node_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': None,
                    'node_name': metric.get('node_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'node'
                })
            
            # Cluster metrics
            cluster_metrics = network_data.get('cluster_metrics', [])
            for metric in cluster_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': None,
                    'node_name': None,
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'cluster'
                })
            
            # Extract backend commit metrics
            backend_commit_data = data_section.get('backend_commit_data', [])
            for metric in backend_commit_data:
                structured_data['backend_commit_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract compact defrag metrics
            compact_defrag_data = data_section.get('compact_defrag_data', [])
            for metric in compact_defrag_data:
                structured_data['compact_defrag_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract analysis results
            analysis_section = data.get('analysis', {})
            structured_data['analysis_results'] = {
                'potential_bottlenecks': analysis_section.get('potential_bottlenecks', []),
                'latency_analysis': analysis_section.get('latency_analysis', {}),
                'recommendations': analysis_section.get('recommendations', [])
            }
            
            # Extract summary information
            summary_section = data.get('summary', {})
            structured_data['summary_info'] = {
                'total_metrics_collected': summary_section.get('total_metrics_collected', 0),
                'categories': summary_section.get('categories', {}),
                'overall_health': summary_section.get('overall_health', 'unknown'),
                'timestamp': summary_section.get('timestamp', 'unknown')
            }
            
            return structured_data
            
        except Exception as e:
            self.logger.error(f"Error extracting deep drive data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Transform test metadata
            if structured_data.get('test_metadata'):
                metadata_df = pd.DataFrame([structured_data['test_metadata']])
                dataframes['test_overview'] = self.limit_dataframe_columns(metadata_df, table_name='test_overview')
            
            # Transform general info metrics
            if structured_data.get('general_info_metrics'):
                general_df = pd.DataFrame(structured_data['general_info_metrics'])
                general_df = self._format_deep_drive_metrics_dataframe(general_df, 'general_info')
                dataframes['general_info'] = self.limit_dataframe_columns(general_df, table_name='general_info')
            
            # Transform WAL fsync metrics
            if structured_data.get('wal_fsync_metrics'):
                wal_df = pd.DataFrame(structured_data['wal_fsync_metrics'])
                wal_df = self._format_deep_drive_metrics_dataframe(wal_df, 'wal_fsync')
                dataframes['wal_fsync'] = self.limit_dataframe_columns(wal_df, table_name='wal_fsync')
            
            # Transform disk I/O metrics
            if structured_data.get('disk_io_metrics'):
                disk_df = pd.DataFrame(structured_data['disk_io_metrics'])
                disk_df = self._format_deep_drive_metrics_dataframe(disk_df, 'disk_io')
                dataframes['disk_io'] = self.limit_dataframe_columns(disk_df, table_name='disk_io')
            
            # Transform network metrics
            if structured_data.get('network_metrics'):
                network_df = pd.DataFrame(structured_data['network_metrics'])
                network_df = self._format_deep_drive_metrics_dataframe(network_df, 'network_io')
                dataframes['network_io'] = self.limit_dataframe_columns(network_df, table_name='network_io')
            
            # Transform backend commit metrics
            if structured_data.get('backend_commit_metrics'):
                backend_df = pd.DataFrame(structured_data['backend_commit_metrics'])
                backend_df = self._format_deep_drive_metrics_dataframe(backend_df, 'backend_commit')
                dataframes['backend_commit'] = self.limit_dataframe_columns(backend_df, table_name='backend_commit')
            
            # Transform compact defrag metrics
            if structured_data.get('compact_defrag_metrics'):
                compact_df = pd.DataFrame(structured_data['compact_defrag_metrics'])
                compact_df = self._format_deep_drive_metrics_dataframe(compact_df, 'compact_defrag')
                dataframes['compact_defrag'] = self.limit_dataframe_columns(compact_df, table_name='compact_defrag')
            
            # Transform analysis summary
            if structured_data.get('analysis_results') or structured_data.get('summary_info'):
                analysis_data = []
                
                # Add latency analysis
                latency_analysis = structured_data.get('analysis_results', {}).get('latency_analysis', {})
                for metric_type, analysis in latency_analysis.items():
                    if isinstance(analysis, dict):
                        analysis_data.append({
                            'Analysis Type': 'Latency Analysis',
                            'Metric': metric_type.replace('_', ' ').title(),
                            'Average (ms)': analysis.get('avg_ms', 'N/A'),
                            'Status': analysis.get('status', 'Unknown')
                        })
                
                # Add summary info
                summary_info = structured_data.get('summary_info', {})
                if summary_info:
                    analysis_data.append({
                        'Analysis Type': 'Summary',
                        'Metric': 'Total Metrics',
                        'Average (ms)': summary_info.get('total_metrics_collected', 0),
                        'Status': summary_info.get('overall_health', 'Unknown')
                    })
                
                if analysis_data:
                    analysis_df = pd.DataFrame(analysis_data)
                    dataframes['analysis_summary'] = self.limit_dataframe_columns(analysis_df, table_name='analysis_summary')
            
            return dataframes
            
        except Exception as e:
            self.logger.error(f"Error transforming deep drive data to DataFrames: {e}")
            return {}
    
    def _format_deep_drive_metrics_dataframe(self, df: pd.DataFrame, metric_type: str) -> pd.DataFrame:
        """Format metrics DataFrame with readable units and highlighting"""
        try:
            if df.empty:
                return df
            
            df_copy = df.copy()
            
            # Identify top performers for highlighting
            if 'avg_value' in df_copy.columns:
                top_indices = self.identify_top_values(df_copy.to_dict('records'), 'avg_value')
            else:
                top_indices = []
            
            # Format values based on metric type and unit
            for idx, row in df_copy.iterrows():
                unit = str(row.get('unit', ''))
                avg_val = row.get('avg_value', 0)
                max_val = row.get('max_value', 0)
                
                is_top = idx in top_indices
                
                # Format based on metric type
                if metric_type == 'general_info':
                    metric_name = str(row.get('metric_name', ''))
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_general_info_values(avg_val, metric_name, unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_general_info_values(max_val, metric_name, unit, False)
                
                elif metric_type == 'wal_fsync':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_wal_fsync_values(avg_val, 'latency', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_wal_fsync_values(max_val, 'latency', unit, False)
                
                elif metric_type == 'disk_io':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_disk_io_values(avg_val, unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_disk_io_values(max_val, unit, False)
                
                elif metric_type == 'network_io':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_network_io_values(avg_val, 'network', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_network_io_values(max_val, 'network', unit, False)
                
                elif metric_type == 'backend_commit':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_backend_commit_values(avg_val, 'latency', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_backend_commit_values(max_val, 'latency', unit, False)
                
                elif metric_type == 'compact_defrag':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_compact_defrag_values(avg_val, 'duration', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_compact_defrag_values(max_val, 'duration', unit, False)
            
            # Rename columns for better display
            column_mapping = {
                'metric_name': 'Metric Name',
                'pod_name': 'Pod Name',
                'node_name': 'Node Name',
                'avg_value': 'Avg Usage',
                'max_value': 'Max Usage',
                'unit': 'Unit'
            }
            
            df_copy = df_copy.rename(columns=column_mapping)
            
            # Select relevant columns based on available data
            display_columns = ['Metric Name']
            if 'Pod Name' in df_copy.columns and df_copy['Pod Name'].notna().any():
                display_columns.append('Pod Name')
            if 'Node Name' in df_copy.columns and df_copy['Node Name'].notna().any():
                display_columns.append('Node Name')
            display_columns.extend(['Avg Usage', 'Max Usage'])
            
            # Filter to only existing columns
            available_columns = [col for col in display_columns if col in df_copy.columns]
            df_copy = df_copy[available_columns]
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"Error formatting deep drive metrics DataFrame: {e}")
            return df
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if not df.empty:
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
        except Exception as e:
            self.logger.error(f"Error generating HTML tables for deep drive: {e}")
        
        return html_tables
    
    def summarize_deep_drive(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for deep drive analysis"""
        try:
            summary_parts = []
            
            # Test metadata summary
            metadata = structured_data.get('test_metadata', {})
            test_id = metadata.get('test_id', 'unknown')
            duration = metadata.get('duration', 'unknown')
            status = metadata.get('status', 'unknown')
            
            summary_parts.append(f"<strong>Deep Drive Performance Analysis</strong> (Test ID: {test_id})")
            summary_parts.append(f"Duration: {duration}, Status: {status}")
            
            # Metrics count summary
            metrics_counts = []
            for metric_type in ['general_info_metrics', 'wal_fsync_metrics', 'disk_io_metrics', 
                              'network_metrics', 'backend_commit_metrics', 'compact_defrag_metrics']:
                count = len(structured_data.get(metric_type, []))
                if count > 0:
                    display_name = metric_type.replace('_metrics', '').replace('_', ' ').title()
                    metrics_counts.append(f"{display_name}: {count}")
            
            if metrics_counts:
                summary_parts.append(f"Metrics collected: {', '.join(metrics_counts)}")
            
            # Analysis results summary
            analysis = structured_data.get('analysis_results', {})
            latency_analysis = analysis.get('latency_analysis', {})
            
            if latency_analysis:
                latency_summary = []
                for metric_type, data in latency_analysis.items():
                    if isinstance(data, dict) and 'status' in data:
                        status = data['status']
                        metric_display = metric_type.replace('_', ' ').title()
                        if status == 'excellent':
                            latency_summary.append(f"✅ {metric_display}: {status}")
                        elif status == 'good':
                            latency_summary.append(f"✅ {metric_display}: {status}")
                        else:
                            latency_summary.append(f"⚠️ {metric_display}: {status}")
                
                if latency_summary:
                    summary_parts.append("Latency Status: " + ", ".join(latency_summary))
            
            # Overall health
            summary_info = structured_data.get('summary_info', {})
            overall_health = summary_info.get('overall_health', 'unknown')
            total_metrics = summary_info.get('total_metrics_collected', 0)
            
            summary_parts.append(f"Overall Health: {overall_health.title()}, Total Metrics: {total_metrics}")
            
            # Recommendations
            recommendations = analysis.get('recommendations', [])
            if recommendations:
                if len(recommendations) == 1 and recommendations[0] == "No significant performance bottlenecks detected":
                    summary_parts.append("✅ No performance issues detected")
                else:
                    summary_parts.append(f"📋 {len(recommendations)} recommendations available")
            
            return "<br>".join(summary_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating deep drive summary: {e}")
            return f"Deep Drive Analysis Summary (Error: {str(e)})"