"""
ELT module for etcd compact and defrag data processing
Specialized module for extracting, transforming and loading compact/defrag metrics
"""

import logging
from typing import Dict, Any, List, Union
import pandas as pd
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class compactDefragELT(utilityELT):
    """Extract, Load, Transform class for compact defrag data"""
    
    def __init__(self):
        super().__init__()
        self.metric_categories = {
            'compaction': ['debugging_mvcc_db_compaction_duration_sum_delta', 'debugging_mvcc_db_compaction_duration_sum'],
            'defragmentation': ['disk_backend_defrag_duration_sum_rate', 'disk_backend_defrag_duration_sum'],
            'page_faults': ['vmstat_pgmajfault_rate', 'vmstat_pgmajfault_total']
        }
    
    def extract_compact_defrag(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract compact defrag data into structured format"""
        try:
            # Handle nested data structure
            if 'data' in data and 'metrics' in data['data']:
                metrics_data = data['data']['metrics']
                master_nodes = data['data'].get('master_nodes', [])
                summary_data = data['data'].get('summary', {})
            elif 'metrics' in data:
                metrics_data = data['metrics']
                master_nodes = data.get('master_nodes', [])
                summary_data = data.get('summary', {})
            else:
                return {'error': 'No metrics data found in compact defrag results'}
            
            structured = {
                'timestamp': data.get('timestamp', ''),
                'duration': data.get('duration', '1h'),
                'category': data.get('category', 'disk_compact_defrag'),
                'master_nodes': master_nodes,
                'summary': summary_data,
                'metrics_overview': [],
                'compaction_metrics': [],
                'defragmentation_metrics': [],
                'page_fault_metrics': [],
                'pod_performance': [],
                'node_performance': []
            }
            
            # Process each metric
            for metric_name, metric_info in metrics_data.items():
                if metric_info.get('status') != 'success':
                    continue
                
                # Add to metrics overview
                overview_item = {
                    'Metric': self.format_metric_name(metric_name),
                    'Category': self.categorize_compact_defrag_metric(metric_name),
                    'Unit': metric_info.get('unit', ''),
                    'Average': self.format_compact_defrag_value(metric_info.get('overall', {}).get('avg', 0), metric_info.get('unit', '')),
                    'Maximum': self.format_compact_defrag_value(metric_info.get('overall', {}).get('max', 0), metric_info.get('unit', '')),
                    'Latest': self.format_compact_defrag_value(metric_info.get('overall', {}).get('latest', 0), metric_info.get('unit', ''))
                }
                structured['metrics_overview'].append(overview_item)
                
                # Process pod/instance data
                pod_data = metric_info.get('data', {})
                
                # Handle pod-based metrics (compaction, defragmentation)
                if 'pods' in pod_data:
                    for pod_name, pod_info in pod_data['pods'].items():
                        pod_item = {
                            'Pod': self.truncate_node_name(pod_name, 30),
                            'Node': self.truncate_node_name(pod_info.get('node', 'unknown'), 25),
                            'Metric': self.format_metric_name(metric_name),
                            'Average': self.format_compact_defrag_value(pod_info.get('avg', 0), metric_info.get('unit', '')),
                            'Maximum': self.format_compact_defrag_value(pod_info.get('max', 0), metric_info.get('unit', '')),
                            'Latest': self.format_compact_defrag_value(pod_info.get('latest', 0), metric_info.get('unit', ''))
                        }
                        structured['pod_performance'].append(pod_item)
                
                # Handle instance-based metrics (vmstat)
                elif 'instances' in pod_data:
                    for instance_name, instance_info in pod_data['instances'].items():
                        instance_item = {
                            'Instance': self.truncate_node_name(instance_name, 30),
                            'Node': self.truncate_node_name(instance_info.get('node', 'unknown'), 25),
                            'Metric': self.format_metric_name(metric_name),
                            'Average': self.format_compact_defrag_value(instance_info.get('avg', 0), metric_info.get('unit', '')),
                            'Maximum': self.format_compact_defrag_value(instance_info.get('max', 0), metric_info.get('unit', '')),
                            'Latest': self.format_compact_defrag_value(instance_info.get('latest', 0), metric_info.get('unit', ''))
                        }
                        structured['node_performance'].append(instance_item)
                
                # Categorize metrics
                category = self.categorize_compact_defrag_metric(metric_name)
                if category == 'Compaction':
                    structured['compaction_metrics'].append(overview_item)
                elif category == 'Defragmentation':
                    structured['defragmentation_metrics'].append(overview_item)
                elif category == 'Page Faults':
                    structured['page_fault_metrics'].append(overview_item)
            
            return structured
            
        except Exception as e:
            logger.error(f"Error extracting compact defrag data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Create DataFrames from lists
            for key, data_list in structured_data.items():
                if isinstance(data_list, list) and data_list and key != 'master_nodes':
                    df = pd.DataFrame(data_list)
                    if not df.empty:
                        # Apply column limiting
                        df = self.limit_dataframe_columns(df, table_name=key)
                        dataframes[key] = df
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Error transforming compact defrag data: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with highlighting for compact defrag data"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Apply highlighting based on table type
                df_styled = df.copy()
                
                if table_name == 'metrics_overview':
                    # Highlight top performers in overview
                    top_avg_indices = self.identify_top_values(df.to_dict('records'), 'Average')
                    top_max_indices = self.identify_top_values(df.to_dict('records'), 'Maximum')
                    
                    for idx, row in df_styled.iterrows():
                        # Highlight average values
                        if idx in top_avg_indices:
                            df_styled.loc[idx, 'Average'] = self.highlight_compact_defrag_values(
                                self.extract_numeric_value(row['Average']), 'average', row.get('Unit', ''), is_top=True
                            )
                        else:
                            df_styled.loc[idx, 'Average'] = self.highlight_compact_defrag_values(
                                self.extract_numeric_value(row['Average']), 'average', row.get('Unit', '')
                            )
                        
                        # Highlight maximum values
                        if idx in top_max_indices:
                            df_styled.loc[idx, 'Maximum'] = self.highlight_compact_defrag_values(
                                self.extract_numeric_value(row['Maximum']), 'maximum', row.get('Unit', ''), is_top=True
                            )
                        else:
                            df_styled.loc[idx, 'Maximum'] = self.highlight_compact_defrag_values(
                                self.extract_numeric_value(row['Maximum']), 'maximum', row.get('Unit', '')
                            )
                
                elif table_name in ['pod_performance', 'node_performance']:
                    # Highlight performance metrics for pods/nodes
                    top_avg_indices = self.identify_top_values(df.to_dict('records'), 'Average')
                    
                    for idx, row in df_styled.iterrows():
                        if idx in top_avg_indices:
                            df_styled.loc[idx, 'Average'] = self.highlight_compact_defrag_values(
                                self.extract_numeric_value(row['Average']), 'performance', '', is_top=True
                            )
                
                # Generate HTML table
                html_tables[table_name] = self.create_html_table(df_styled, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Error generating HTML tables: {e}")
            return {}
    
    def summarize_compact_defrag(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for compact defrag data"""
        try:
            summary_parts = []
            
            # Basic info
            duration = structured_data.get('duration', '1h')
            master_nodes_count = len(structured_data.get('master_nodes', []))
            total_metrics = len(structured_data.get('metrics_overview', []))
            
            summary_parts.append(f"Compact & Defrag Analysis ({duration})")
            summary_parts.append(f"• {total_metrics} metrics collected across {master_nodes_count} master nodes")
            
            # Compaction summary
            compaction_metrics = structured_data.get('compaction_metrics', [])
            if compaction_metrics:
                avg_compaction_time = sum([self.extract_numeric_value(m.get('Average', '0')) for m in compaction_metrics]) / len(compaction_metrics)
                summary_parts.append(f"• Average compaction duration: {self.format_compact_defrag_value(avg_compaction_time, 'milliseconds')}")
            
            # Defragmentation summary
            defrag_metrics = structured_data.get('defragmentation_metrics', [])
            if defrag_metrics:
                total_defrag_time = sum([self.extract_numeric_value(m.get('Average', '0')) for m in defrag_metrics])
                summary_parts.append(f"• Total defragmentation activity: {self.format_compact_defrag_value(total_defrag_time, 'seconds')}")
            
            # Page faults summary
            page_fault_metrics = structured_data.get('page_fault_metrics', [])
            if page_fault_metrics:
                avg_page_faults = sum([self.extract_numeric_value(m.get('Average', '0')) for m in page_fault_metrics]) / len(page_fault_metrics)
                summary_parts.append(f"• Average page fault rate: {self.format_compact_defrag_value(avg_page_faults, 'faults/s')}")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def format_metric_name(self, metric_name: str) -> str:
        """Format metric name for display"""
        name_mapping = {
            'debugging_mvcc_db_compaction_duration_sum_delta': 'DB Compaction Rate',
            'debugging_mvcc_db_compaction_duration_sum': 'DB Compaction Duration',
            'disk_backend_defrag_duration_sum_rate': 'Defrag Rate',
            'disk_backend_defrag_duration_sum': 'Defrag Duration',
            'vmstat_pgmajfault_rate': 'Page Fault Rate',
            'vmstat_pgmajfault_total': 'Total Page Faults'
        }
        return name_mapping.get(metric_name, metric_name.replace('_', ' ').title())
    
    def categorize_compact_defrag_metric(self, metric_name: str) -> str:
        """Categorize compact defrag metric type"""
        metric_lower = metric_name.lower()
        
        if 'compaction' in metric_lower:
            return 'Compaction'
        elif 'defrag' in metric_lower:
            return 'Defragmentation'
        elif 'pgmajfault' in metric_lower or 'page' in metric_lower:
            return 'Page Faults'
        else:
            return 'Other'
    
    def format_compact_defrag_value(self, value: Union[float, int], unit: str) -> str:
        """Format compact defrag values with appropriate units"""
        try:
            if value == 0:
                return "0"
            
            if unit == 'milliseconds':
                if value < 1:
                    return f"{value*1000:.0f} μs"
                elif value < 1000:
                    return f"{value:.1f} ms"
                else:
                    return f"{value/1000:.2f} s"
            elif unit == 'seconds':
                if value < 1:
                    return f"{value*1000:.1f} ms"
                elif value < 60:
                    return f"{value:.3f} s"
                else:
                    return f"{value/60:.1f} min"
            elif unit == 'faults/s':
                if value < 0.001:
                    return f"{value*1000:.2f} mfaults/s"
                elif value < 1:
                    return f"{value:.3f} faults/s"
                else:
                    return f"{value:.1f} faults/s"
            elif unit == 'faults':
                return self.format_count_value(value)
            else:
                return f"{value:.2f}"
                
        except (ValueError, TypeError):
            return str(value)
    
    def highlight_compact_defrag_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight compact defrag values with metric-specific thresholds"""
        try:
            thresholds = self._get_compact_defrag_thresholds(metric_type)
            
            if is_top:
                formatted_value = self.format_compact_defrag_value(float(value), unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self.format_compact_defrag_value(float(value), unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self.format_compact_defrag_value(float(value), unit)
                
        except (ValueError, TypeError):
            return str(value)
    
    def _get_compact_defrag_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for compact defrag metrics"""
        thresholds_map = {
            'compaction_duration': {'warning': 50, 'critical': 100},  # milliseconds
            'defrag_duration': {'warning': 1, 'critical': 5},         # seconds
            'page_fault_rate': {'warning': 0.1, 'critical': 1.0},    # faults/s
            'page_fault_total': {'warning': 20000, 'critical': 50000} # total faults
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_type.lower():
                return threshold
        
        return {}