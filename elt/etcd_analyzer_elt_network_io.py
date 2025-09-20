"""
Network I/O ELT module for ETCD Analyzer
Extract, Load, Transform module for Network I/O Performance Data
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class networkIOELT(utilityELT):
    """Extract, Load, Transform class for Network I/O data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_network_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network I/O metrics data"""
        try:
            # Handle nested data structure
            if 'data' in data and isinstance(data['data'], dict):
                metrics_data = data['data']
            else:
                metrics_data = data
            
            # Extract different levels of metrics
            pods_metrics = metrics_data.get('pods_metrics', {})
            # Fallback to container_metrics for backward compatibility
            if not pods_metrics and 'container_metrics' in metrics_data:
                pods_metrics = metrics_data['container_metrics']
            
            node_metrics = metrics_data.get('node_metrics', {})
            cluster_metrics = metrics_data.get('cluster_metrics', {})
            
            # Extract pod-level metrics
            pod_data = []
            for metric_name, metric_info in pods_metrics.items():
                if metric_info.get('status') == 'success':
                    unit = metric_info.get('unit', 'unknown')
                    for pod_name, pod_stats in metric_info.get('pods', {}).items():
                        avg_value = pod_stats.get('avg', 0) or 0
                        max_value = pod_stats.get('max', 0) or 0
                        node_name = pod_stats.get('node', 'unknown')
                        
                        pod_data.append({
                            'metric_name': metric_name,
                            'pod_name': pod_name,
                            'node_name': node_name,
                            'avg_value': float(avg_value),
                            'max_value': float(max_value),
                            'unit': unit
                        })
            
            # Extract node-level metrics
            node_data = []
            for metric_name, metric_info in node_metrics.items():
                if metric_info.get('status') == 'success':
                    unit = metric_info.get('unit', 'unknown')
                    for node_name, node_stats in metric_info.get('nodes', {}).items():
                        avg_value = node_stats.get('avg', 0) or 0
                        max_value = node_stats.get('max', 0) or 0
                        device_count = node_stats.get('device_count', 0)
                        
                        node_data.append({
                            'metric_name': metric_name,
                            'node_name': node_name,
                            'avg_value': float(avg_value),
                            'max_value': float(max_value),
                            'device_count': device_count,
                            'unit': unit
                        })
            
            # Extract cluster-level metrics
            cluster_data = []
            for metric_name, metric_info in cluster_metrics.items():
                if metric_info.get('status') == 'success':
                    unit = metric_info.get('unit', 'unknown')
                    avg_value = metric_info.get('avg', 0) or 0
                    max_value = metric_info.get('max', 0) or 0
                    latest_value = metric_info.get('latest', 0) or 0
                    
                    cluster_data.append({
                        'metric_name': metric_name,
                        'avg_value': float(avg_value),
                        'max_value': float(max_value),
                        'latest_value': float(latest_value),
                        'unit': unit
                    })
            
            # Create network overview with top metrics
            overview_data = self._create_network_overview(pod_data, node_data, cluster_data)
            
            return {
                'pod_metrics': pod_data,
                'node_metrics': node_data,
                'cluster_metrics': cluster_data,
                'overview': overview_data,
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
                'duration': data.get('duration', 'unknown')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract network I/O data: {e}")
            return {'error': str(e)}
    
    def _create_network_overview(self, pod_data: List[Dict[str, Any]], node_data: List[Dict[str, Any]], 
                                cluster_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create network overview with top metrics summary"""
        overview_data = []
        
        # Combine all metrics for top analysis
        all_metrics = {}
        
        # Process pod metrics
        for item in pod_data:
            metric_name = item['metric_name']
            if metric_name not in all_metrics:
                all_metrics[metric_name] = {
                    'avg_values': [],
                    'max_values': [],
                    'unit': item['unit'],
                    'category': 'Pod Network'
                }
            all_metrics[metric_name]['avg_values'].append(item['avg_value'])
            all_metrics[metric_name]['max_values'].append(item['max_value'])
        
        # Process node metrics
        for item in node_data:
            metric_name = item['metric_name']
            if metric_name not in all_metrics:
                all_metrics[metric_name] = {
                    'avg_values': [],
                    'max_values': [],
                    'unit': item['unit'],
                    'category': 'Node Network'
                }
            all_metrics[metric_name]['avg_values'].append(item['avg_value'])
            all_metrics[metric_name]['max_values'].append(item['max_value'])
        
        # Process cluster metrics
        for item in cluster_data:
            metric_name = item['metric_name']
            all_metrics[metric_name] = {
                'avg_values': [item['avg_value']],
                'max_values': [item['max_value']],
                'unit': item['unit'],
                'category': 'Cluster'
            }
        
        # Create overview entries for each metric
        for metric_name, metric_data in all_metrics.items():
            avg_values = [v for v in metric_data['avg_values'] if v is not None and v > 0]
            max_values = [v for v in metric_data['max_values'] if v is not None and v > 0]
            
            if avg_values or max_values:
                # Calculate aggregated values
                total_avg = sum(avg_values) if avg_values else 0
                peak_max = max(max_values) if max_values else 0
                
                # Clean metric name for display
                display_name = metric_name.replace('network_io_', '').replace('_', ' ').title()
                if display_name.startswith('Container '):
                    display_name = display_name.replace('Container ', 'Pod ')
                elif display_name.startswith('Network Client '):
                    display_name = display_name.replace('Network Client ', 'gRPC ')
                elif display_name.startswith('Network Peer '):
                    display_name = display_name.replace('Network Peer ', 'Peer ')
                elif display_name.startswith('Grpc Active '):
                    display_name = display_name.replace('Grpc Active ', 'Active ')
                
                overview_data.append({
                    'metric_name': display_name,
                    'category': metric_data['category'],
                    'avg_usage': total_avg,
                    'max_usage': peak_max,
                    'unit': metric_data['unit'],
                    'raw_metric_name': metric_name  # Keep for threshold checking
                })
        
        # Sort by average usage (descending) to show top metrics first
        overview_data.sort(key=lambda x: x['avg_usage'], reverse=True)
        
        return overview_data
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            # Network overview table - shows top metrics with avg/max usage
            if structured_data.get('overview'):
                overview_df = pd.DataFrame(structured_data['overview'])
                if not overview_df.empty:
                    # Format values for display with highlighting
                    overview_df['avg_formatted'] = overview_df.apply(lambda row: 
                        self.highlight_network_io_values(row['avg_usage'], row['raw_metric_name'], row['unit']), axis=1)
                    overview_df['max_formatted'] = overview_df.apply(lambda row: 
                        self.highlight_network_io_values(row['max_usage'], row['raw_metric_name'], row['unit']), axis=1)
                    
                    # Identify top performers (top 3 by average usage)
                    top_indices = list(range(min(3, len(overview_df))))  # Top 3 metrics
                    for idx in top_indices:
                        if idx < len(overview_df):
                            overview_df.at[idx, 'avg_formatted'] = self.highlight_network_io_values(
                                overview_df.at[idx, 'avg_usage'], overview_df.at[idx, 'raw_metric_name'], 
                                overview_df.at[idx, 'unit'], is_top=True)
                    
                    dataframes['network_overview'] = overview_df
            
            # Pod metrics table (supports either key)
            pod_metrics_list = structured_data.get('pod_metrics') or structured_data.get('container_metrics')
            if pod_metrics_list:
                pod_df = pd.DataFrame(pod_metrics_list)
                if not pod_df.empty:
                    # Format values for display
                    pod_df['avg_formatted'] = pod_df.apply(lambda row: 
                        self.highlight_network_io_values(row['avg_value'], row['metric_name'], row['unit']), axis=1)
                    pod_df['max_formatted'] = pod_df.apply(lambda row: 
                        self.highlight_network_io_values(row['max_value'], row['metric_name'], row['unit']), axis=1)
                    
                    # Identify top performers
                    top_indices = self.identify_top_values(pod_metrics_list, 'avg_value')
                    for idx in top_indices:
                        if idx < len(pod_df):
                            pod_df.at[idx, 'avg_formatted'] = self.highlight_network_io_values(
                                pod_df.at[idx, 'avg_value'], pod_df.at[idx, 'metric_name'], 
                                pod_df.at[idx, 'unit'], is_top=True)
                    
                    dataframes['container_metrics'] = pod_df
            
            # Node metrics table
            if structured_data.get('node_metrics'):
                node_df = pd.DataFrame(structured_data['node_metrics'])
                if not node_df.empty:
                    # Format values for display
                    node_df['avg_formatted'] = node_df.apply(lambda row: 
                        self.highlight_network_io_values(row['avg_value'], row['metric_name'], row['unit']), axis=1)
                    node_df['max_formatted'] = node_df.apply(lambda row: 
                        self.highlight_network_io_values(row['max_value'], row['metric_name'], row['unit']), axis=1)
                    
                    # Identify top performers
                    top_indices = self.identify_top_values(structured_data['node_metrics'], 'avg_value')
                    for idx in top_indices:
                        if idx < len(node_df):
                            node_df.at[idx, 'avg_formatted'] = self.highlight_network_io_values(
                                node_df.at[idx, 'avg_value'], node_df.at[idx, 'metric_name'], 
                                node_df.at[idx, 'unit'], is_top=True)
                    
                    dataframes['node_performance'] = node_df
            
            # Cluster metrics table
            if structured_data.get('cluster_metrics'):
                cluster_df = pd.DataFrame(structured_data['cluster_metrics'])
                if not cluster_df.empty:
                    # Format values for display
                    cluster_df['avg_formatted'] = cluster_df.apply(lambda row: 
                        self.highlight_network_io_values(row['avg_value'], row['metric_name'], row['unit']), axis=1)
                    cluster_df['max_formatted'] = cluster_df.apply(lambda row: 
                        self.highlight_network_io_values(row['max_value'], row['metric_name'], row['unit']), axis=1)
                    cluster_df['latest_formatted'] = cluster_df.apply(lambda row: 
                        self.highlight_network_io_values(row['latest_value'], row['metric_name'], row['unit']), axis=1)
                    
                    dataframes['grpc_streams'] = cluster_df
                    
        except Exception as e:
            logger.error(f"Failed to create DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Network overview table
            if 'network_overview' in dataframes:
                df = dataframes['network_overview'].copy()
                if not df.empty:
                    df_display = df[['metric_name', 'category', 'avg_formatted', 'max_formatted', 'unit']].copy()
                    df_display.columns = ['Metric', 'Category', 'Avg Usage', 'Max Usage', 'Unit']
                    html_tables['metrics_overview'] = self.create_html_table(df_display, 'Network Metrics Overview')
            
            # Container/Pod metrics table (title-free)
            if 'container_metrics' in dataframes:
                df = dataframes['container_metrics'].copy()
                if not df.empty:
                    # Prefer concise columns for readability
                    cols = [c for c in ['pod_name', 'node_name', 'avg_formatted', 'max_formatted', 'unit'] if c in df.columns]
                    df_display = df[cols].copy()
                    rename = {
                        'pod_name': 'Pod Name',
                        'node_name': 'Node',
                        'avg_formatted': 'Average',
                        'max_formatted': 'Maximum',
                        'unit': 'Unit'
                    }
                    df_display.rename(columns=rename, inplace=True)
                    html_tables['container_metrics'] = self.create_html_table(df_display, 'Container Network Usage')
            
            # Node performance table (title-free)
            if 'node_performance' in dataframes:
                df = dataframes['node_performance'].copy()
                if not df.empty:
                    cols = [c for c in ['node_name', 'avg_formatted', 'max_formatted', 'unit'] if c in df.columns]
                    df_display = df[cols].copy()
                    rename = {
                        'node_name': 'Node Name',
                        'avg_formatted': 'Average',
                        'max_formatted': 'Maximum',
                        'unit': 'Unit'
                    }
                    df_display.rename(columns=rename, inplace=True)
                    html_tables['node_performance'] = self.create_html_table(df_display, 'Node Network Usage')
            
            # Cluster metrics (gRPC stream) table (title-free)
            if 'grpc_streams' in dataframes:
                df = dataframes['grpc_streams'].copy()
                if not df.empty:
                    cols = [c for c in ['latest_formatted', 'avg_formatted', 'max_formatted', 'unit'] if c in df.columns]
                    df_display = df[cols].copy()
                    rename = {
                        'latest_formatted': 'Current Count',
                        'avg_formatted': 'Average',
                        'max_formatted': 'Maximum',
                        'unit': 'Unit'
                    }
                    df_display.rename(columns=rename, inplace=True)
                    html_tables['grpc_streams'] = self.create_html_table(df_display, 'gRPC Active Stream')
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}
        
        return html_tables
    
    def summarize_network_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of network I/O performance"""
        try:
            pod_metrics = structured_data.get('pod_metrics', [])
            node_metrics = structured_data.get('node_metrics', [])
            cluster_metrics = structured_data.get('cluster_metrics', [])
            
            summary_parts = ["Network I/O Performance Analysis:"]
            
            if pod_metrics:
                total_pods = len(set(item['pod_name'] for item in pod_metrics))
                avg_throughput = self._calculate_pod_throughput_avg(pod_metrics)
                summary_parts.append(f"• {total_pods} etcd pods monitored with avg throughput {avg_throughput:.1f} Mbps")
                
                # Check for high latency
                latency_metrics = [item for item in pod_metrics if 'latency' in item['metric_name']]
                if latency_metrics:
                    high_latency = [item for item in latency_metrics if item['avg_value'] > 0.01]
                    if high_latency:
                        summary_parts.append(f"⚠️ {len(high_latency)} pods with high peer latency (>10ms)")
            
            if node_metrics:
                total_nodes = len(set(item['node_name'] for item in node_metrics))
                avg_utilization = self._calculate_node_utilization_avg(node_metrics)
                summary_parts.append(f"• {total_nodes} master nodes with avg network utilization {avg_utilization:.1f} Mbps")
            
            if cluster_metrics:
                active_streams = self._get_active_streams_count(cluster_metrics)
                stream_health = self._assess_stream_health(cluster_metrics)
                summary_parts.append(f"• {active_streams} active gRPC streams ({stream_health} load)")
            
            # Overall health assessment
            health_status = self.assess_network_io_health(pod_metrics + node_metrics)
            if health_status == 'critical':
                summary_parts.append("⚠️ Network performance issues detected")
            elif health_status == 'degraded':
                summary_parts.append("⚠️ Some network metrics elevated")
            else:
                summary_parts.append("✓ Network I/O performance appears healthy")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate network I/O summary: {e}")
            return f"Network I/O summary generation failed: {str(e)}"