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
    """Network I/O Extract, Load, Transform class"""
    
    def __init__(self):
        super().__init__()

    def extract_network_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network I/O data from tool results"""
        try:
            # Handle nested data structure from actual JSON format
            # The structure is: data.data.data.{container_metrics, node_metrics, cluster_metrics}
            if 'data' in data and 'data' in data['data'] and 'data' in data['data']['data']:
                network_data = data['data']['data']['data']
            elif 'data' in data and 'data' in data['data']:
                network_data = data['data']['data']
            elif 'data' in data:
                network_data = data['data']
            else:
                network_data = data

            # Get top level metadata
            top_data = data.get('data', {}).get('data', {})
            extracted = {
                'status': top_data.get('status', 'unknown'),
                'collection_time': top_data.get('timestamp', datetime.now().isoformat()),
                'duration': top_data.get('duration', '1h'),
                'container_metrics': [],
                'node_metrics': [],
                'cluster_metrics': []
            }

            # Process container metrics
            container_data = network_data.get('container_metrics', {})
            for metric_name, metric_info in container_data.items():
                if metric_info.get('status') != 'success':
                    continue
                
                pods_data = metric_info.get('pods', {})
                for pod_name, pod_stats in pods_data.items():
                    extracted['container_metrics'].append({
                        'metric_name': metric_name,
                        'title': metric_info.get('title', metric_name),
                        'pod_name': pod_name,
                        'avg_value': pod_stats.get('avg'),
                        'max_value': pod_stats.get('max'),
                        'unit': metric_info.get('unit', ''),
                        'node_name': pod_stats.get('node', 'unknown')
                    })

            # Process node metrics
            node_data = network_data.get('node_metrics', {})
            for metric_name, metric_info in node_data.items():
                if metric_info.get('status') != 'success':
                    continue
                
                nodes_data = metric_info.get('nodes', {})
                for node_name, node_stats in nodes_data.items():
                    extracted['node_metrics'].append({
                        'metric_name': metric_name,
                        'title': metric_info.get('title', metric_name),
                        'node_name': node_name,
                        'avg_value': node_stats.get('avg'),
                        'max_value': node_stats.get('max'),
                        'unit': metric_info.get('unit', ''),
                        'device_count': node_stats.get('device_count', 0)
                    })

            # Process cluster metrics
            cluster_data = network_data.get('cluster_metrics', {})
            for metric_name, metric_info in cluster_data.items():
                if metric_info.get('status') != 'success':
                    continue
                
                extracted['cluster_metrics'].append({
                    'metric_name': metric_name,
                    'title': metric_info.get('title', metric_name),
                    'avg_value': metric_info.get('avg'),
                    'max_value': metric_info.get('max'),
                    'latest_value': metric_info.get('latest'),
                    'unit': metric_info.get('unit', '')
                })

            return extracted

        except Exception as e:
            logger.error(f"Failed to extract network I/O data: {e}")
            return {'error': str(e)}

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured network I/O data into pandas DataFrames"""
        dataframes = {}
        
        try:
            # Create individual tables for each container metric
            container_metrics = structured_data.get('container_metrics', [])
            container_metric_names = list(set([m['metric_name'] for m in container_metrics]))
            
            for metric_name in container_metric_names:
                metric_data = [m for m in container_metrics if m['metric_name'] == metric_name]
                if metric_data:
                    df_data = []
                    for item in metric_data:
                        df_data.append({
                            'Metric Name': item['title'],
                            'Pod Name': self.truncate_node_name(item['pod_name']),
                            'Average': self._format_network_value(item['avg_value'], item['unit']),
                            'Maximum': self._format_network_value(item['max_value'], item['unit']),
                            'Unit': item['unit']
                        })
                    
                    df = pd.DataFrame(df_data)
                    if not df.empty:
                        # Highlight top performers
                        top_avg_indices = self.identify_top_values(metric_data, 'avg_value')
                        for idx in top_avg_indices:
                            if idx < len(df):
                                df.loc[idx, 'Average'] = self.highlight_network_io_values(
                                    metric_data[idx]['avg_value'],
                                    metric_data[idx]['metric_name'],
                                    is_top=True
                                )
                        
                        dataframes[metric_name.replace('network_io_', '')] = df

            # Create individual tables for each node metric
            node_metrics = structured_data.get('node_metrics', [])
            node_metric_names = list(set([m['metric_name'] for m in node_metrics]))
            
            for metric_name in node_metric_names:
                metric_data = [m for m in node_metrics if m['metric_name'] == metric_name]
                if metric_data:
                    df_data = []
                    for item in metric_data:
                        df_data.append({
                            'Metric Name': item['title'],
                            'Node Name': self.truncate_node_name(item['node_name']),
                            'Average': self._format_network_value(item['avg_value'], item['unit']),
                            'Maximum': self._format_network_value(item['max_value'], item['unit']),
                            'Unit': item['unit']
                        })
                    
                    df = pd.DataFrame(df_data)
                    if not df.empty:
                        # Highlight top performers
                        top_avg_indices = self.identify_top_values(metric_data, 'avg_value')
                        for idx in top_avg_indices:
                            if idx < len(df):
                                df.loc[idx, 'Average'] = self.highlight_network_io_values(
                                    metric_data[idx]['avg_value'],
                                    metric_data[idx]['metric_name'],
                                    is_top=True
                                )
                        
                        dataframes[metric_name.replace('network_io_', '')] = df

            # Create individual tables for each cluster metric
            cluster_metrics = structured_data.get('cluster_metrics', [])
            for item in cluster_metrics:
                df_data = [{
                    'Metric Name': item['title'],
                    'Average': self._format_network_value(item['avg_value'], item['unit']),
                    'Maximum': self._format_network_value(item['max_value'], item['unit']),
                    'Unit': item['unit']
                }]
                
                df = pd.DataFrame(df_data)
                if not df.empty:
                    dataframes[item['metric_name'].replace('network_io_', '')] = df

        except Exception as e:
            logger.error(f"Failed to create network I/O DataFrames: {e}")
        
        return dataframes

    def _format_network_value(self, value: Optional[Union[float, int]], unit: str) -> str:
        """Format network values with appropriate units"""
        if value is None:
            return "N/A"
        
        try:
            if unit == 'bytes_per_second':
                return self.format_bytes_per_second(float(value))
            elif unit == 'bits_per_second':
                return self.format_bits_per_second(float(value))
            elif unit == 'packets_per_second':
                return self.format_packets_per_second(float(value))
            elif unit == 'seconds':
                return self.format_network_latency_seconds(float(value))
            elif unit == 'count':
                return self.format_count_value(float(value))
            else:
                return f"{value:.2f}"
        except (ValueError, TypeError):
            return str(value)

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for network I/O data"""
        html_tables = {}
        
        for table_name, df in dataframes.items():
            if not df.empty:
                html_tables[table_name] = self.create_html_table(df, table_name)
        
        return html_tables

    def summarize_network_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for network I/O data"""
        try:
            summary_parts = [f"Network I/O Performance Analysis (Duration: {structured_data.get('duration', 'unknown')})"]
            
            # Container metrics summary
            container_metrics = structured_data.get('container_metrics', [])
            if container_metrics:
                unique_pods = set(m['pod_name'] for m in container_metrics)
                etcd_pods = [pod for pod in unique_pods if 'etcd-openshift' in pod]
                summary_parts.append(f"• Container network metrics: {len(etcd_pods)} etcd pods monitored")
                
                # Find highest throughput for RX
                rx_metrics = [m for m in container_metrics if 'container_network_rx' in m.get('metric_name', '') and 'etcd-openshift' in m['pod_name']]
                if rx_metrics:
                    max_rx = max(rx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_rx = self._format_network_value(max_rx['avg_value'], max_rx['unit'])
                    pod_short = max_rx['pod_name'].split('.')[0].replace('etcd-openshift-qe-', 'node-')
                    summary_parts.append(f"• Highest RX throughput: {formatted_rx} ({pod_short})")
                
                # Find highest throughput for TX  
                tx_metrics = [m for m in container_metrics if 'container_network_tx' in m.get('metric_name', '') and 'etcd-openshift' in m['pod_name']]
                if tx_metrics:
                    max_tx = max(tx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_tx = self._format_network_value(max_tx['avg_value'], max_tx['unit'])
                    pod_short = max_tx['pod_name'].split('.')[0].replace('etcd-openshift-qe-', 'node-')
                    summary_parts.append(f"• Highest TX throughput: {formatted_tx} ({pod_short})")
                
                # Check for peer latency
                peer_latency_metrics = [m for m in container_metrics if 'peer2peer_latency' in m.get('metric_name', '')]
                if peer_latency_metrics:
                    avg_latency = sum(m.get('avg_value', 0) or 0 for m in peer_latency_metrics) / len(peer_latency_metrics)
                    formatted_latency = self._format_network_value(avg_latency, 'seconds')
                    summary_parts.append(f"• Average peer latency (P99): {formatted_latency}")

                # Check gRPC traffic
                grpc_rx_metrics = [m for m in container_metrics if 'client_grpc_received' in m.get('metric_name', '')]
                if grpc_rx_metrics:
                    total_grpc_rx = sum(m.get('avg_value', 0) or 0 for m in grpc_rx_metrics)
                    formatted_grpc = self._format_network_value(total_grpc_rx, 'bytes_per_second')
                    summary_parts.append(f"• Total gRPC RX traffic: {formatted_grpc}")

            # Node metrics summary
            node_metrics = structured_data.get('node_metrics', [])
            if node_metrics:
                unique_nodes = set(m['node_name'] for m in node_metrics)
                summary_parts.append(f"• Node network metrics: {len(unique_nodes)} master nodes monitored")
                
                # Find highest node utilization
                node_rx_metrics = [m for m in node_metrics if 'rx_utilization' in m.get('metric_name', '')]
                if node_rx_metrics:
                    max_node_rx = max(node_rx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_node_rx = self._format_network_value(max_node_rx['avg_value'], max_node_rx['unit'])
                    node_short = max_node_rx['node_name'].split('.')[0].replace('openshift-qe-', 'node-')
                    summary_parts.append(f"• Highest node RX utilization: {formatted_node_rx} ({node_short})")

            # Cluster metrics summary
            cluster_metrics = structured_data.get('cluster_metrics', [])
            if cluster_metrics:
                watch_streams = next((m for m in cluster_metrics if 'watch_streams' in m.get('metric_name', '')), None)
                if watch_streams and watch_streams.get('latest_value'):
                    formatted_streams = self._format_network_value(watch_streams['latest_value'], watch_streams['unit'])
                    summary_parts.append(f"• Active watch streams: {formatted_streams}")
                
                lease_streams = next((m for m in cluster_metrics if 'lease_streams' in m.get('metric_name', '')), None)
                if lease_streams and lease_streams.get('latest_value') is not None:
                    formatted_lease = self._format_network_value(lease_streams['latest_value'], lease_streams['unit'])
                    summary_parts.append(f"• Active lease streams: {formatted_lease}")

            # Status assessment
            collection_time = structured_data.get('collection_time', 'unknown')
            if collection_time != 'unknown':
                summary_parts.append(f"• Collection completed at: {collection_time[:19].replace('T', ' ')}")
            
            summary_parts.append("• Status: All network metrics collected successfully")

            return " ".join(summary_parts)

        except Exception as e:
            logger.error(f"Failed to generate network I/O summary: {e}")
            return f"Network I/O summary generation failed: {str(e)}"

    # Helper methods for formatting (using inherited methods from utilityELT)
    def format_bits_per_second(self, bits_per_sec: float) -> str:
        """Format bits per second to readable units"""
        return self.format_network_bits_per_second(bits_per_sec)

    def format_packets_per_second(self, packets_per_sec: float) -> str:
        """Format packets per second to readable units"""
        return self.format_network_packets_per_second(packets_per_sec)

    def highlight_network_io_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight network I/O values with metric-specific thresholds"""
        return super().highlight_network_io_values(value, metric_type, unit, is_top)

    def _get_network_io_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for network I/O metrics"""
        thresholds_map = {
            'rx': {'warning': 100000000, 'critical': 500000000},  # 100MB/s, 500MB/s
            'tx': {'warning': 100000000, 'critical': 500000000},  # 100MB/s, 500MB/s
            'round_trip_time': {'warning': 0.01, 'critical': 0.05},  # 10ms, 50ms
            'packets': {'warning': 1000, 'critical': 5000},  # 1K pps, 5K pps
            'watch_streams': {'warning': 500, 'critical': 1000}  # 500, 1000 streams
        }
        
        metric_lower = metric_type.lower()
        for key, threshold in thresholds_map.items():
            if key in metric_lower:
                return threshold
        
        return {}