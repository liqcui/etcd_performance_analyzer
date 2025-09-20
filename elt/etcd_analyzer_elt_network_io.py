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
            # Create metrics overview table
            overview_data = []
            
            container_metrics = structured_data.get('container_metrics', [])
            node_metrics = structured_data.get('node_metrics', [])
            cluster_metrics = structured_data.get('cluster_metrics', [])
            
            # Container metrics overview
            if container_metrics:
                etcd_pods = set(m['pod_name'] for m in container_metrics if 'etcd' in m['pod_name'].lower())
                overview_data.append({
                    'Metric Category': 'Container Network',
                    'Description': f'{len(etcd_pods)} etcd pods monitored',
                    'Key Metrics': 'RX/TX throughput, gRPC traffic',
                    'Status': 'Active' if etcd_pods else 'No Data'
                })
                
                # Add peak throughput info
                rx_metrics = [m for m in container_metrics if 'network_rx' in m.get('metric_name', '')]
                tx_metrics = [m for m in container_metrics if 'network_tx' in m.get('metric_name', '')]
                
                if rx_metrics:
                    max_rx = max(rx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    peak_rx = self._format_network_value(max_rx['avg_value'], max_rx['unit'])
                    overview_data.append({
                        'Metric Category': 'Peak RX Throughput',
                        'Description': f'Highest container network receive rate',
                        'Key Metrics': peak_rx,
                        'Status': 'Measured'
                    })
                
                if tx_metrics:
                    max_tx = max(tx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    peak_tx = self._format_network_value(max_tx['avg_value'], max_tx['unit'])
                    overview_data.append({
                        'Metric Category': 'Peak TX Throughput',
                        'Description': f'Highest container network transmit rate',
                        'Key Metrics': peak_tx,
                        'Status': 'Measured'
                    })
            
            # Node metrics overview
            if node_metrics:
                unique_nodes = set(m['node_name'] for m in node_metrics)
                overview_data.append({
                    'Metric Category': 'Node Network',
                    'Description': f'{len(unique_nodes)} nodes monitored',
                    'Key Metrics': 'Utilization, packets, drops',
                    'Status': 'Active' if unique_nodes else 'No Data'
                })
                
                # Check for packet drops
                drop_metrics = [m for m in node_metrics if 'drop' in m.get('metric_name', '')]
                if drop_metrics:
                    total_drops = sum(m.get('avg_value', 0) or 0 for m in drop_metrics)
                    drop_status = 'Warning' if total_drops > 0 else 'Normal'
                    overview_data.append({
                        'Metric Category': 'Packet Drops',
                        'Description': 'Network packet loss monitoring',
                        'Key Metrics': f'{total_drops:.2f} drops/sec average',
                        'Status': drop_status
                    })
            
            # Cluster metrics overview
            if cluster_metrics:
                overview_data.append({
                    'Metric Category': 'gRPC Streams',
                    'Description': 'Active client connections',
                    'Key Metrics': 'Watch streams, lease streams',
                    'Status': 'Monitored'
                })
                
                # Add specific stream counts
                for item in cluster_metrics:
                    if 'watch_streams' in item['metric_name']:
                        stream_count = self._format_network_value(item.get('latest_value'), item.get('unit', ''))
                        overview_data.append({
                            'Metric Category': 'Active Watch Streams',
                            'Description': 'Current watch stream connections',
                            'Key Metrics': stream_count,
                            'Status': 'Active' if item.get('latest_value', 0) > 0 else 'Idle'
                        })
                    elif 'lease_streams' in item['metric_name']:
                        lease_count = self._format_network_value(item.get('latest_value'), item.get('unit', ''))
                        overview_data.append({
                            'Metric Category': 'Active Lease Streams',
                            'Description': 'Current lease stream connections',
                            'Key Metrics': lease_count,
                            'Status': 'Active' if item.get('latest_value', 0) > 0 else 'Idle'
                        })
            
            if overview_data:
                df_overview = pd.DataFrame(overview_data)
                dataframes['metrics_overview'] = df_overview
            # Create consolidated container metrics table (Container Network Usage)
            container_metrics = structured_data.get('container_metrics', [])
            if container_metrics:
                # Group by pod name for cleaner display
                pod_data = {}
                for item in container_metrics:
                    pod_name = item['pod_name']
                    metric_type = item['metric_name']
                    
                    if pod_name not in pod_data:
                        pod_data[pod_name] = {'pod_name': pod_name, 'node_name': item.get('node_name', 'unknown')}
                    
                    # Store values based on metric type
                    if 'network_rx' in metric_type:
                        pod_data[pod_name]['rx_avg'] = item.get('avg_value')
                        pod_data[pod_name]['rx_max'] = item.get('max_value')
                        pod_data[pod_name]['rx_unit'] = item.get('unit', '')
                    elif 'network_tx' in metric_type:
                        pod_data[pod_name]['tx_avg'] = item.get('avg_value')
                        pod_data[pod_name]['tx_max'] = item.get('max_value')
                        pod_data[pod_name]['tx_unit'] = item.get('unit', '')
                    elif 'grpc_received' in metric_type:
                        pod_data[pod_name]['grpc_rx_avg'] = item.get('avg_value')
                        pod_data[pod_name]['grpc_rx_unit'] = item.get('unit', '')
                    elif 'grpc_sent' in metric_type:
                        pod_data[pod_name]['grpc_tx_avg'] = item.get('avg_value')
                        pod_data[pod_name]['grpc_tx_unit'] = item.get('unit', '')
                
                # Create DataFrame for container metrics
                container_rows = []
                for pod_name, data in pod_data.items():
                    row = {
                        'Pod Name': self.truncate_node_name(pod_name),
                        'Node': self.truncate_node_name(data['node_name'])
                    }
                    
                    # Add network RX/TX if available
                    if 'rx_avg' in data:
                        row['Network RX (Avg)'] = self._format_network_value(data['rx_avg'], data.get('rx_unit', ''))
                        row['Network TX (Avg)'] = self._format_network_value(data.get('tx_avg'), data.get('tx_unit', ''))
                    
                    # Add gRPC traffic if available
                    if 'grpc_rx_avg' in data:
                        row['gRPC RX (Avg)'] = self._format_network_value(data['grpc_rx_avg'], data.get('grpc_rx_unit', ''))
                        row['gRPC TX (Avg)'] = self._format_network_value(data.get('grpc_tx_avg'), data.get('grpc_tx_unit', ''))
                    
                    container_rows.append(row)
                
                if container_rows:
                    df_container = pd.DataFrame(container_rows)
                    try:
                        if 'Pod Name' in df_container.columns:
                            df_container = df_container.sort_values(by=['Pod Name'])
                    except Exception:
                        pass
                    dataframes['container_metrics'] = df_container

            # Create consolidated node metrics table (Node Network Usage)
            node_metrics = structured_data.get('node_metrics', [])
            if node_metrics:
                # Group by node name for cleaner display
                node_data = {}
                for item in node_metrics:
                    node_name = item['node_name']
                    metric_type = item['metric_name']
                    
                    if node_name not in node_data:
                        node_data[node_name] = {'node_name': node_name}
                    
                    # Store values based on metric type
                    if 'rx_utilization' in metric_type:
                        node_data[node_name]['rx_util_avg'] = item.get('avg_value')
                        node_data[node_name]['rx_util_unit'] = item.get('unit', '')
                    elif 'tx_utilization' in metric_type:
                        node_data[node_name]['tx_util_avg'] = item.get('avg_value')
                        node_data[node_name]['tx_util_unit'] = item.get('unit', '')
                    elif 'rx_package' in metric_type:
                        node_data[node_name]['rx_pkg_avg'] = item.get('avg_value')
                        node_data[node_name]['rx_pkg_unit'] = item.get('unit', '')
                    elif 'tx_package' in metric_type:
                        node_data[node_name]['tx_pkg_avg'] = item.get('avg_value')
                        node_data[node_name]['tx_pkg_unit'] = item.get('unit', '')
                    elif 'rx_drop' in metric_type:
                        node_data[node_name]['rx_drop_avg'] = item.get('avg_value')
                    elif 'tx_drop' in metric_type:
                        node_data[node_name]['tx_drop_avg'] = item.get('avg_value')
                
                # Create DataFrame for node metrics
                node_rows = []
                for node_name, data in node_data.items():
                    row = {
                        'Node Name': self.truncate_node_name(node_name)
                    }
                    
                    # Add utilization metrics
                    if 'rx_util_avg' in data:
                        row['RX Utilization'] = self._format_network_value(data['rx_util_avg'], data.get('rx_util_unit', ''))
                    if 'tx_util_avg' in data:
                        row['TX Utilization'] = self._format_network_value(data['tx_util_avg'], data.get('tx_util_unit', ''))
                    
                    # Add package metrics
                    if 'rx_pkg_avg' in data:
                        row['RX Packages'] = self._format_network_value(data['rx_pkg_avg'], data.get('rx_pkg_unit', ''))
                    if 'tx_pkg_avg' in data:
                        row['TX Packages'] = self._format_network_value(data['tx_pkg_avg'], data.get('tx_pkg_unit', ''))
                    
                    # Add drop metrics
                    if 'rx_drop_avg' in data:
                        row['RX Drops'] = f"{data['rx_drop_avg']:.2f}" if data['rx_drop_avg'] > 0 else "0"
                    if 'tx_drop_avg' in data:
                        row['TX Drops'] = f"{data['tx_drop_avg']:.2f}" if data['tx_drop_avg'] > 0 else "0"
                    
                    node_rows.append(row)
                
                if node_rows:
                    df_node = pd.DataFrame(node_rows)
                    try:
                        if 'Node Name' in df_node.columns:
                            df_node = df_node.sort_values(by=['Node Name'])
                    except Exception:
                        pass
                    dataframes['node_performance'] = df_node

            # Create cluster metrics table (gRPC Active Stream)
            cluster_metrics = structured_data.get('cluster_metrics', [])
            if cluster_metrics:
                cluster_rows = []
                for item in cluster_metrics:
                    metric_name = item['metric_name']
                    if 'watch_streams' in metric_name:
                        stream_type = 'Watch Streams'
                    elif 'lease_streams' in metric_name:
                        stream_type = 'Lease Streams'
                    else:
                        stream_type = metric_name.replace('network_io_grpc_active_', '').replace('_', ' ').title()
                    
                    row = {
                        'Stream Type': stream_type,
                        'Current Count': self._format_network_value(item.get('latest_value'), item.get('unit', '')),
                        'Average': self._format_network_value(item.get('avg_value'), item.get('unit', '')),
                        'Maximum': self._format_network_value(item.get('max_value'), item.get('unit', ''))
                    }
                    cluster_rows.append(row)
                
                if cluster_rows:
                    df_cluster = pd.DataFrame(cluster_rows)
                    try:
                        if 'Stream Type' in df_cluster.columns:
                            df_cluster = df_cluster.sort_values(by=['Stream Type'])
                    except Exception:
                        pass
                    dataframes['grpc_streams'] = df_cluster

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
        """Generate HTML tables for network I/O data with custom naming"""
        html_tables = {}
        
        table_name_mapping = {
            'metrics_overview': 'Network Metrics Overview',
            'container_metrics': 'Container Network Usage',
            'node_performance': 'Node Network Usage', 
            'grpc_streams': 'gRPC Active Stream'
        }
        
        for table_name, df in dataframes.items():
            if not df.empty:
                display_name = table_name_mapping.get(table_name, table_name.replace('_', ' ').title())
                html_tables[table_name] = self.create_html_table(df, display_name)
        
        return html_tables

    def summarize_network_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for network I/O data"""
        try:
            summary_parts = [f"Network I/O Performance Analysis (Duration: {structured_data.get('duration', 'unknown')})"]
            
            # Container metrics summary
            container_metrics = structured_data.get('container_metrics', [])
            if container_metrics:
                unique_pods = set(m['pod_name'] for m in container_metrics)
                etcd_pods = [pod for pod in unique_pods if 'etcd' in pod.lower()]
                summary_parts.append(f"• Container network usage: {len(etcd_pods)} etcd pods monitored")
                
                # Find highest throughput for RX
                rx_metrics = [m for m in container_metrics if 'network_rx' in m.get('metric_name', '')]
                if rx_metrics:
                    max_rx = max(rx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_rx = self._format_network_value(max_rx['avg_value'], max_rx['unit'])
                    pod_short = max_rx['pod_name'].split('.')[0].replace('etcd-', '')
                    summary_parts.append(f"• Highest RX throughput: {formatted_rx} ({pod_short})")
                
                # Find highest throughput for TX  
                tx_metrics = [m for m in container_metrics if 'network_tx' in m.get('metric_name', '')]
                if tx_metrics:
                    max_tx = max(tx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_tx = self._format_network_value(max_tx['avg_value'], max_tx['unit'])
                    pod_short = max_tx['pod_name'].split('.')[0].replace('etcd-', '')
                    summary_parts.append(f"• Highest TX throughput: {formatted_tx} ({pod_short})")

            # Node metrics summary
            node_metrics = structured_data.get('node_metrics', [])
            if node_metrics:
                unique_nodes = set(m['node_name'] for m in node_metrics)
                summary_parts.append(f"• Node network usage: {len(unique_nodes)} nodes monitored")
                
                # Find highest node utilization
                node_rx_metrics = [m for m in node_metrics if 'rx_utilization' in m.get('metric_name', '')]
                if node_rx_metrics:
                    max_node_rx = max(node_rx_metrics, key=lambda x: x.get('avg_value', 0) or 0)
                    formatted_node_rx = self._format_network_value(max_node_rx['avg_value'], max_node_rx['unit'])
                    node_short = max_node_rx['node_name'].split('.')[0]
                    summary_parts.append(f"• Highest node RX utilization: {formatted_node_rx} ({node_short})")

            # Cluster metrics summary
            cluster_metrics = structured_data.get('cluster_metrics', [])
            if cluster_metrics:
                watch_streams = next((m for m in cluster_metrics if 'watch_streams' in m.get('metric_name', '')), None)
                if watch_streams and watch_streams.get('latest_value') is not None:
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