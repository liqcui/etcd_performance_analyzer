"""
etcd Performance Deep Drive Analyzer
Comprehensive performance analysis module for etcd clusters
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz

# Import existing modules
from tools.etcd_general_info import GeneralInfoCollector
from tools.etcd_disk_wal_fsync import DiskWALFsyncCollector
from tools.etcd_disk_io import DiskIOCollector
from tools.etcd_network_io import NetworkIOCollector
from tools.etcd_disk_backend_commit import DiskBackendCommitCollector
from tools.etcd_disk_compact_defrag import CompactDefragCollector
from analysis.etcd_analyzer_performance_utility import etcdAnalyzerUtility


class etcdDeepDriveAnalyzer:
    """Deep drive analyzer for etcd cluster performance"""
    
    def __init__(self, ocp_auth, duration: str = "1h"):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
        self.utility = etcdAnalyzerUtility()
        
        # Initialize collectors
        self.general_collector = GeneralInfoCollector(ocp_auth)
        self.wal_fsync_collector = DiskWALFsyncCollector(ocp_auth, duration)
        self.disk_io_collector = DiskIOCollector(ocp_auth, duration)
        self.network_collector = NetworkIOCollector(ocp_auth)
        self.backend_commit_collector = DiskBackendCommitCollector(ocp_auth)
        self.compact_defrag_collector = CompactDefragCollector(ocp_auth, duration)
        
        self.test_id = self.utility.generate_test_id()
    
    async def analyze_performance_deep_drive(self) -> Dict[str, Any]:
        """Comprehensive performance analysis of etcd cluster"""
        try:
            self.logger.info(f"Starting etcd performance deep drive analysis with test ID: {self.test_id}")
            
            analysis_result = {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "duration": self.duration,
                "timezone": "UTC",
                "status": "success",
                "data": {
                    "general_info_data": [],
                    "wal_fsync_data": [],
                    "disk_io_data": [],
                    "network_data": {},
                    "backend_commit_data": [],
                    "compact_defrag_data": []
                },
                "analysis": {},
                "summary": {}
            }
            
            # 2.1 Collect general info metrics
            analysis_result["data"]["general_info_data"] = await self._collect_general_info_metrics()
            
            # 2.2 Collect WAL fsync metrics
            analysis_result["data"]["wal_fsync_data"] = await self._collect_wal_fsync_metrics()
            
            # 2.3 Collect disk I/O metrics
            analysis_result["data"]["disk_io_data"] = await self._collect_disk_io_metrics()
            
            # 2.4 Collect network I/O metrics
            analysis_result["data"]["network_data"] = await self._collect_network_io_metrics()
            
            # 2.5 Collect backend commit metrics
            analysis_result["data"]["backend_commit_data"] = await self._collect_backend_commit_metrics()
            
            # 2.6 Collect compact/defrag metrics
            analysis_result["data"]["compact_defrag_data"] = await self._collect_compact_defrag_metrics()
            
            # Perform latency analysis
            analysis_result["analysis"] = self.utility.analyze_latency_patterns(analysis_result["data"])
            
            # Create performance summary (include latency analysis so health can be derived)
            summary_input = {**analysis_result["data"], "latency_analysis": analysis_result["analysis"]}
            analysis_result["summary"] = self.utility.create_performance_summary(summary_input)
            
            self.logger.info(f"Successfully completed etcd performance deep drive analysis")
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Error in performance deep drive analysis: {e}")
            return {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "status": "error",
                "error": str(e)
            }
    
    async def _collect_general_info_metrics(self) -> List[Dict[str, Any]]:
        """Collect general info metrics for specified metrics"""
        # Use metric names exactly as defined in config/metrics-etcd.yml
        target_metrics = [
            'etcd_pods_cpu_usage',
            'etcd_pods_memory_usage',
            'proposal_failure_rate',
            'proposal_pending_total',
            'proposal_commit_rate',
            'proposal_apply_rate',
            'total_proposals_committed',
            'leader_changes_rate',
            'etcd_slow_applies',
            'etcd_slow_read_indexes',
            'etcd_mvcc_put_operations_rate',
            'etcd_mvcc_delete_operations_rate',
            'etcd_network_heartbeat_send_failures',
            'etcd_server_health_failures',
            'vmstat_pgmajfault_total',
            'vmstat_pgmajfault_rate'
        ]
        
        general_metrics = []
        
        try:
            for metric_name in target_metrics:
                try:
                    # Get single metric per pod
                    result = await self.general_collector._get_single_metric_per_pod(metric_name, self.duration)
                    
                    if result.get('status') == 'success':
                        pods_data = result.get('pods', {})
                        unit = result.get('unit', 'unknown')
                        
                        for pod_name, pod_stats in pods_data.items():
                            general_metrics.append({
                                "metric_name": metric_name,
                                "pod_name": pod_name,
                                "avg": pod_stats.get('avg'),
                                "max": pod_stats.get('max'),
                                "unit": unit
                            })
                
                except Exception as e:
                    self.logger.error(f"Error collecting general metric {metric_name}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error in _collect_general_info_metrics: {e}")
        
        return general_metrics
    
    async def _collect_wal_fsync_metrics(self) -> List[Dict[str, Any]]:
        """Collect WAL fsync metrics"""
        target_metrics = [
            'disk_wal_fsync_seconds_duration_p99',
            'disk_wal_fsync_duration_seconds_sum_rate',
            'disk_wal_fsync_duration_sum',
            'disk_wal_fsync_duration_seconds_count_rate',
            'disk_wal_fsync_duration_seconds_count'
        ]
        
        wal_metrics = []
        
        try:
            for metric_name in target_metrics:
                try:
                    result = await self.wal_fsync_collector.get_metric_by_name(metric_name)
                    
                    if result.get('status') == 'success':
                        pod_metrics_data = result.get('pod_metrics', {})
                        unit = result.get('unit', 'unknown')
                        
                        for pod_name, pod_stats in pod_metrics_data.items():
                            # Extract appropriate value based on metric type
                            avg_value = None
                            max_value = None
                            
                            if 'p99' in metric_name or 'percentile' in metric_name.lower():
                                avg_value = pod_stats.get('avg_seconds')
                                max_value = pod_stats.get('max_seconds')
                            elif 'rate' in metric_name and 'duration' in metric_name:
                                avg_value = pod_stats.get('avg_rate_seconds')
                                max_value = pod_stats.get('max_rate_seconds')
                            elif 'sum' in metric_name and 'rate' not in metric_name:
                                avg_value = pod_stats.get('avg_sum_seconds')
                                max_value = pod_stats.get('max_sum_seconds')
                            elif 'count' in metric_name and 'rate' in metric_name:
                                avg_value = pod_stats.get('avg_ops_per_sec')
                                max_value = pod_stats.get('max_ops_per_sec')
                            elif 'count' in metric_name:
                                avg_value = pod_stats.get('avg_count')
                                max_value = pod_stats.get('max_count')
                            else:
                                avg_value = pod_stats.get('avg_value')
                                max_value = pod_stats.get('max_value')
                            
                            wal_metrics.append({
                                "metric_name": metric_name,
                                "pod_name": pod_name,
                                "avg": avg_value,
                                "max": max_value,
                                "unit": unit
                            })
                
                except Exception as e:
                    self.logger.error(f"Error collecting WAL metric {metric_name}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error in _collect_wal_fsync_metrics: {e}")
        
        return wal_metrics
    
    async def _collect_disk_io_metrics(self) -> List[Dict[str, Any]]:
        """Collect disk I/O metrics"""
        disk_metrics = []
        
        try:
            all_metrics_result = await self.disk_io_collector.collect_all_metrics()
            
            if all_metrics_result.get('status') == 'success':
                metrics_data = all_metrics_result.get('metrics', {})
                
                for metric_name, metric_data in metrics_data.items():
                    if metric_data.get('status') == 'success':
                        nodes_data = metric_data.get('nodes', {})
                        unit = metric_data.get('unit', 'unknown')
                        
                        for node_name, node_stats in nodes_data.items():
                            disk_metrics.append({
                                "metric_name": metric_name,
                                "node_name": node_name,
                                "avg": node_stats.get('avg'),
                                "max": node_stats.get('max'),
                                "unit": unit,
                                "devices": node_stats.get('devices', [])
                            })
        
        except Exception as e:
            self.logger.error(f"Error in _collect_disk_io_metrics: {e}")
        
        return disk_metrics
    
    async def _collect_network_io_metrics(self) -> Dict[str, Any]:
        """Collect network I/O metrics"""
        network_data = {
            "pod_metrics": [],
            "node_metrics": [],
            "cluster_metrics": []
        }
        
        try:
            result = await self.network_collector.collect_metrics(self.duration)
            
            if result.get('status') == 'success':
                data = result.get('data', {})
                
                # Pod-level metrics
                pod_target_metrics = [
                    'network_io_container_network_rx',
                    'network_io_container_network_tx',
                    'network_io_peer2peer_latency_p99',
                    'network_io_network_peer_received_bytes',
                    'network_io_network_peer_sent_bytes',
                    'network_io_network_client_grpc_received_bytes',
                    'network_io_network_client_grpc_sent_bytes'
                ]
                
                pods_metrics = data.get('pods_metrics', data.get('container_metrics', {}))
                for metric_name in pod_target_metrics:
                    if metric_name in pods_metrics:
                        metric_data = pods_metrics[metric_name]
                        if metric_data.get('status') == 'success':
                            pods_data = metric_data.get('pods', {})
                            unit = metric_data.get('unit', 'unknown')
                            
                            for pod_name, pod_stats in pods_data.items():
                                network_data["pod_metrics"].append({
                                    "metric_name": metric_name,
                                    "pod_name": pod_name,
                                    "avg": pod_stats.get('avg'),
                                    "max": pod_stats.get('max'),
                                    "unit": unit
                                })
                
                # Node-level metrics
                node_target_metrics = [
                    'network_io_node_network_rx_utilization',
                    'network_io_node_network_tx_utilization',
                    'network_io_node_network_rx_package',
                    'network_io_node_network_tx_package',
                    'network_io_node_network_rx_drop',
                    'network_io_node_network_tx_drop'
                ]
                
                node_metrics = data.get('node_metrics', {})
                for metric_name in node_target_metrics:
                    if metric_name in node_metrics:
                        metric_data = node_metrics[metric_name]
                        if metric_data.get('status') == 'success':
                            nodes_data = metric_data.get('nodes', {})
                            unit = metric_data.get('unit', 'unknown')
                            query = metric_data.get('query', 'N/A')
                            
                            for node_name, node_stats in nodes_data.items():
                                network_data["node_metrics"].append({
                                    "metric_name": metric_name,
                                    "node_name": node_name,
                                    "avg": node_stats.get('avg'),
                                    "max": node_stats.get('max'),
                                    "unit": unit,
                                    "query": query
                                })
                
                # Cluster-level metrics
                cluster_target_metrics = [
                    'network_io_grpc_active_watch_streams',
                    'network_io_grpc_active_lease_streams'
                ]
                
                cluster_metrics = data.get('cluster_metrics', {})
                for metric_name in cluster_target_metrics:
                    if metric_name in cluster_metrics:
                        metric_data = cluster_metrics[metric_name]
                        if metric_data.get('status') == 'success':
                            network_data["cluster_metrics"].append({
                                "metric_name": metric_name,
                                "test_id": self.test_id,
                                "avg": metric_data.get('avg'),
                                "max": metric_data.get('max'),
                                "unit": metric_data.get('unit', 'unknown'),
                                "query": metric_data.get('query', 'N/A')
                            })
        
        except Exception as e:
            self.logger.error(f"Error in _collect_network_io_metrics: {e}")
        
        return network_data
    
    async def _collect_backend_commit_metrics(self) -> List[Dict[str, Any]]:
        """Collect backend commit metrics"""
        backend_metrics = []

        try:
            # Use collector to fetch all metrics defined in YAML to avoid name mismatches
            collected = await self.backend_commit_collector.collect_metrics(self.duration)
            if collected.get('status') != 'success':
                return backend_metrics

            metrics_map = collected.get('data', {}).get('pods_metrics', {})
            for metric_name, metric_result in metrics_map.items():
                if metric_result.get('status') != 'success':
                    continue
                unit = metric_result.get('unit', 'unknown')
                for pod_name, pod_stats in metric_result.get('pods', {}).items():
                    backend_metrics.append({
                        "metric_name": metric_name,
                        "pod_name": pod_name,
                        "avg": pod_stats.get('avg'),
                        "max": pod_stats.get('max'),
                        "unit": unit
                    })

        except Exception as e:
            self.logger.error(f"Error in _collect_backend_commit_metrics: {e}")

        return backend_metrics
    
    async def _collect_compact_defrag_metrics(self) -> List[Dict[str, Any]]:
        """Collect compact/defrag metrics"""
        target_metrics = [
            'debugging_mvcc_db_compacted_keys',
            'debugging_mvcc_db_compaction_duration_sum_delta',
            'debugging_mvcc_db_compaction_duration_sum',
            'debugging_snapshot_duration',
            'disk_backend_defrag_duration_sum_rate',
            'disk_backend_defrag_duration_sum'
        ]
        
        compact_defrag_metrics = []
        
        try:
            # Get all metrics at once
            all_metrics_result = await self.compact_defrag_collector.collect_all_metrics()
            
            if all_metrics_result.get('status') == 'success':
                metrics_data = all_metrics_result.get('metrics', {})
                
                for metric_name in target_metrics:
                    if metric_name in metrics_data:
                        metric_data = metrics_data[metric_name]
                        
                        if metric_data.get('status') == 'success':
                            # Handle different data structures
                            pods_data = {}
                            unit = metric_data.get('unit', 'unknown')
                            
                            if 'data' in metric_data:
                                if 'pods' in metric_data['data']:
                                    pods_data = metric_data['data']['pods']
                                elif 'instances' in metric_data['data']:
                                    pods_data = metric_data['data']['instances']
                            elif 'pods' in metric_data:
                                pods_data = metric_data['pods']
                            elif 'instances' in metric_data:
                                pods_data = metric_data['instances']
                            
                            for pod_name, pod_stats in pods_data.items():
                                compact_defrag_metrics.append({
                                    "metric_name": metric_name,
                                    "pod_name": pod_name,
                                    "avg": pod_stats.get('avg'),
                                    "max": pod_stats.get('max'),
                                    "unit": unit
                                })
        
        except Exception as e:
            self.logger.error(f"Error in _collect_compact_defrag_metrics: {e}")
        
        return compact_defrag_metrics
    
    async def analyze_bottlenecks(self) -> Dict[str, Any]:
        """Analyze performance bottlenecks and provide detailed insights"""
        try:
            analysis_data = await self.analyze_performance_deep_drive()
            
            if analysis_data.get('status') != 'success':
                return analysis_data
            
            bottleneck_analysis = {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "bottleneck_analysis": {
                    "disk_io_bottlenecks": [],
                    "network_bottlenecks": [],
                    "memory_bottlenecks": [],
                    "consensus_bottlenecks": []
                },
                "root_cause_analysis": [],
                "performance_recommendations": []
            }
            
            data = analysis_data.get('data', {})
            
            # Analyze disk I/O bottlenecks
            await self._analyze_disk_bottlenecks(data, bottleneck_analysis)
            
            # Analyze network bottlenecks
            await self._analyze_network_bottlenecks(data, bottleneck_analysis)
            
            # Analyze memory bottlenecks
            await self._analyze_memory_bottlenecks(data, bottleneck_analysis)
            
            # Analyze consensus bottlenecks
            await self._analyze_consensus_bottlenecks(data, bottleneck_analysis)
            
            # Generate root cause analysis
            bottleneck_analysis["root_cause_analysis"] = self._generate_root_cause_analysis(bottleneck_analysis)
            
            # Generate performance recommendations
            bottleneck_analysis["performance_recommendations"] = self._generate_performance_recommendations(bottleneck_analysis)
            
            return bottleneck_analysis
            
        except Exception as e:
            self.logger.error(f"Error in analyze_bottlenecks: {e}")
            return {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "status": "error",
                "error": str(e)
            }
    
    async def _analyze_disk_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze disk I/O related bottlenecks"""
        try:
            # Check WAL fsync latency
            wal_data = data.get('wal_fsync_data', [])
            for metric in wal_data:
                if 'p99' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0.1:  # > 100ms
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "wal_fsync_high_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "WAL fsync P99 latency exceeds 100ms threshold"
                        })
                    elif metric['avg'] > 0.05:  # > 50ms
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "wal_fsync_elevated_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "WAL fsync P99 latency elevated above 50ms"
                        })
            
            # Check backend commit latency
            backend_data = data.get('backend_commit_data', [])
            for metric in backend_data:
                if 'p99' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0.05:  # > 50ms
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "backend_commit_high_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Backend commit P99 latency exceeds 50ms threshold"
                        })
            
            # Check disk throughput
            disk_data = data.get('disk_io_data', [])
            for metric in disk_data:
                if 'throughput' in metric.get('metric_name', ''):
                    # Low throughput could indicate bottleneck
                    if metric.get('avg', 0) < 1024 * 1024:  # < 1MB/s
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "low_disk_throughput",
                            "node": metric.get('node_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Disk throughput below expected levels"
                        })
        
        except Exception as e:
            self.logger.error(f"Error analyzing disk bottlenecks: {e}")
    
    async def _analyze_network_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
            """Analyze network related bottlenecks"""
            try:
                network_data = data.get('network_data', {})
                
                # Check peer-to-peer latency
                pod_metrics = network_data.get('pod_metrics', [])
                for metric in pod_metrics:
                    if 'peer2peer_latency' in metric.get('metric_name', '') and metric.get('avg'):
                        if metric['avg'] > 0.1:  # > 100ms
                            analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                "type": "high_peer_latency",
                                "pod": metric.get('pod_name'),
                                "value": metric['avg'],
                                "unit": metric.get('unit'),
                                "severity": "high",
                                "description": "Peer-to-peer latency exceeds 100ms threshold"
                            })
                
                # Check network utilization - calculate percentage based on actual bandwidth
                node_metrics = network_data.get('node_metrics', [])
                
                # Group metrics by node to get both bandwidth and utilization data
                node_data = {}
                
                for metric in node_metrics:
                    node_name = metric.get('node_name')
                    metric_name = metric.get('metric_name', '')
                    
                    if node_name not in node_data:
                        node_data[node_name] = {
                            'rx_bytes_per_sec': 0,
                            'tx_bytes_per_sec': 0,
                            'nic_bandwidth_bps': None
                        }
                    
                    # Get network throughput metrics (should be in bytes/sec)
                    if 'network_rx_utilization' in metric_name:
                        # Extract actual bytes/sec from the metric data or calculate from rate
                        node_data[node_name]['rx_bytes_per_sec'] = metric.get('avg', 0)
                    elif 'network_tx_utilization' in metric_name:
                        node_data[node_name]['tx_bytes_per_sec'] = metric.get('avg', 0)
                
                # Get NIC bandwidth capacity and calculate actual utilization
                for node_name, node_info in node_data.items():
                    try:
                        # Try to get NIC bandwidth from network collector or estimate
                        nic_bandwidth_bps = await self._get_nic_bandwidth(node_name)
                        
                        if nic_bandwidth_bps:
                            node_info['nic_bandwidth_bps'] = nic_bandwidth_bps
                            
                            # Calculate RX utilization percentage
                            if node_info['rx_bytes_per_sec'] > 0:
                                rx_utilization_pct = (node_info['rx_bytes_per_sec'] / nic_bandwidth_bps) * 100
                                
                                if rx_utilization_pct > 80:
                                    analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                        "type": "high_network_utilization",
                                        "node": node_name,
                                        "value": round(rx_utilization_pct, 2),
                                        "unit": "percent",
                                        "severity": "high",
                                        "description": f"Network RX utilization exceeds 80% ({self._format_bandwidth(node_info['rx_bytes_per_sec'])}/s of {self._format_bandwidth(nic_bandwidth_bps)}/s capacity)"
                                    })
                                elif rx_utilization_pct > 60:
                                    analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                        "type": "elevated_network_utilization",
                                        "node": node_name,
                                        "value": round(rx_utilization_pct, 2),
                                        "unit": "percent",
                                        "severity": "medium",
                                        "description": f"Network RX utilization elevated above 60% ({self._format_bandwidth(node_info['rx_bytes_per_sec'])}/s of {self._format_bandwidth(nic_bandwidth_bps)}/s capacity)"
                                    })
                            
                            # Calculate TX utilization percentage
                            if node_info['tx_bytes_per_sec'] > 0:
                                tx_utilization_pct = (node_info['tx_bytes_per_sec'] / nic_bandwidth_bps) * 100
                                
                                if tx_utilization_pct > 80:
                                    analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                        "type": "high_network_utilization",
                                        "node": node_name,
                                        "value": round(tx_utilization_pct, 2),
                                        "unit": "percent",
                                        "severity": "high",
                                        "description": f"Network TX utilization exceeds 80% ({self._format_bandwidth(node_info['tx_bytes_per_sec'])}/s of {self._format_bandwidth(nic_bandwidth_bps)}/s capacity)"
                                    })
                                elif tx_utilization_pct > 60:
                                    analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                        "type": "elevated_network_utilization",
                                        "node": node_name,
                                        "value": round(tx_utilization_pct, 2),
                                        "unit": "percent",
                                        "severity": "medium",
                                        "description": f"Network TX utilization elevated above 60% ({self._format_bandwidth(node_info['tx_bytes_per_sec'])}/s of {self._format_bandwidth(nic_bandwidth_bps)}/s capacity)"
                                    })
                        
                    except Exception as e:
                        self.logger.error(f"Error calculating network utilization for node {node_name}: {e}")
                        continue
                
                # Check packet drops
                for metric in node_metrics:
                    if 'drop' in metric.get('metric_name', '') and metric.get('avg'):
                        if metric['avg'] > 0:
                            direction = 'RX' if 'rx_drop' in metric.get('metric_name', '') else 'TX'
                            analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                "type": "packet_drops",
                                "node": metric.get('node_name'),
                                "value": metric['avg'],
                                "unit": metric.get('unit'),
                                "severity": "medium",
                                "description": f"Network {direction} packet drops detected"
                            })
            
            except Exception as e:
                self.logger.error(f"Error analyzing network bottlenecks: {e}")
        
    async def _get_nic_bandwidth(self, node_name: str) -> Optional[float]:
        """Get NIC bandwidth capacity for a node in bytes per second"""
        try:
            # Try to query NIC speed from Prometheus metrics first
            # This would typically be a custom metric or node_exporter metric
            bandwidth_query = f'node_network_speed_bytes{{instance=~".*{node_name}.*"}}'
            
            try:
                # Attempt to get bandwidth from monitoring system
                result = await self.network_collector._query_prometheus(bandwidth_query)
                if result and len(result) > 0:
                    # Get the highest bandwidth interface (usually the primary NIC)
                    max_bandwidth = max([float(item['value'][1]) for item in result])
                    return max_bandwidth
            except:
                pass
            
            # Fallback to common bandwidth estimates based on instance type patterns
            # This is environment-specific and should be adjusted based on your infrastructure
            if 'master' in node_name.lower() or 'control' in node_name.lower():
                # Typically control plane nodes have good network connectivity
                return 10 * 1024 * 1024 * 1024 / 8  # 10 Gbps in bytes/sec
            elif 'worker' in node_name.lower():
                # Worker nodes might have varied connectivity
                return 1 * 1024 * 1024 * 1024 / 8   # 1 Gbps in bytes/sec
            else:
                # Default assumption for cloud instances
                return 1 * 1024 * 1024 * 1024 / 8   # 1 Gbps in bytes/sec
        
        except Exception as e:
            self.logger.error(f"Error getting NIC bandwidth for {node_name}: {e}")
            # Return a conservative default
            return 1 * 1024 * 1024 * 1024 / 8  # 1 Gbps in bytes/sec
  
    def _format_bandwidth(self, bytes_per_sec: float) -> str:
        """Format bandwidth in human-readable format"""
        if bytes_per_sec >= 1024**3:
            return f"{bytes_per_sec/(1024**3):.2f}GB"
        elif bytes_per_sec >= 1024**2:
            return f"{bytes_per_sec/(1024**2):.2f}MB"
        elif bytes_per_sec >= 1024:
            return f"{bytes_per_sec/1024:.2f}KB"
        else:
            return f"{bytes_per_sec:.0f}B"

    async def _analyze_memory_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze memory related bottlenecks"""
        try:
            general_data = data.get('general_info_data', [])
            
            for metric in general_data:
                if metric.get('metric_name') == 'memory_usage' and metric.get('avg'):
                    # Assuming memory usage is in percentage or bytes
                    if metric['avg'] > 80:  # > 80% memory usage
                        analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                            "type": "high_memory_usage",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Memory usage exceeds 80%"
                        })
                    elif metric['avg'] > 70:  # > 70% memory usage
                        analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                            "type": "elevated_memory_usage",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Memory usage elevated above 70%"
                        })
        
        except Exception as e:
            self.logger.error(f"Error analyzing memory bottlenecks: {e}")
    
    async def _analyze_consensus_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze etcd consensus related bottlenecks"""
        try:
            general_data = data.get('general_info_data', [])
            
            for metric in general_data:
                metric_name = metric.get('metric_name', '')
                
                # Check proposal failures
                if metric_name == 'proposal_failure_rate' and metric.get('avg'):
                    if metric['avg'] > 0:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "proposal_failures",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Proposal failures detected in etcd consensus"
                        })
                
                # Check pending proposals
                elif metric_name == 'proposal_pending_total' and metric.get('avg'):
                    if metric['avg'] > 10:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "high_pending_proposals",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "High number of pending proposals"
                        })
                
                # Check slow applies
                elif metric_name == 'slow_applies' and metric.get('avg'):
                    if metric['avg'] > 0:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "slow_applies",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Slow apply operations detected"
                        })
                
                # Check leader changes
                elif metric_name == 'leader_changes_rate' and metric.get('avg'):
                    if metric['avg'] > 1:  # More than 1 leader change per hour
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "frequent_leader_changes",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Frequent leader changes indicating cluster instability"
                        })
        
        except Exception as e:
            self.logger.error(f"Error analyzing consensus bottlenecks: {e}")
    
    def _generate_root_cause_analysis(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate root cause analysis based on identified bottlenecks"""
        root_causes = []
        
        bottlenecks = analysis.get('bottleneck_analysis', {})
        
        # Disk I/O root causes
        if bottlenecks.get('disk_io_bottlenecks'):
            root_causes.append({
                "category": "disk_io",
                "root_cause": "Storage subsystem performance degradation",
                "evidence": "High WAL fsync latency and/or backend commit latency",
                "impact": "Increased write operation latency affecting cluster performance",
                "likelihood": "high"
            })
        
        # Network root causes
        if bottlenecks.get('network_bottlenecks'):
            root_causes.append({
                "category": "network",
                "root_cause": "Network connectivity or bandwidth issues",
                "evidence": "High peer-to-peer latency, network utilization, or packet drops",
                "impact": "Degraded cluster communication and consensus performance",
                "likelihood": "high"
            })
        
        # Memory root causes
        if bottlenecks.get('memory_bottlenecks'):
            root_causes.append({
                "category": "memory",
                "root_cause": "Insufficient memory resources or memory leaks",
                "evidence": "High memory usage on etcd pods",
                "impact": "Potential pod restarts and performance degradation",
                "likelihood": "medium"
            })
        
        # Consensus root causes
        if bottlenecks.get('consensus_bottlenecks'):
            root_causes.append({
                "category": "consensus",
                "root_cause": "etcd cluster consensus mechanism under stress",
                "evidence": "Proposal failures, slow applies, or frequent leader changes",
                "impact": "Degraded cluster stability and write performance",
                "likelihood": "high"
            })
        
        return root_causes
    
    def _generate_performance_recommendations(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate performance optimization recommendations"""
        recommendations = []
        
        bottlenecks = analysis.get('bottleneck_analysis', {})
        
        # Disk I/O recommendations
        if bottlenecks.get('disk_io_bottlenecks'):
            recommendations.extend([
                {
                    "category": "disk_io",
                    "priority": "high",
                    "recommendation": "Upgrade storage to high-performance NVMe SSDs",
                    "rationale": "Reduce WAL fsync and backend commit latency"
                },
                {
                    "category": "disk_io",
                    "priority": "medium",
                    "recommendation": "Ensure etcd data directory is on dedicated storage",
                    "rationale": "Prevent I/O contention with other workloads"
                }
            ])
        
        # Network recommendations
        if bottlenecks.get('network_bottlenecks'):
            recommendations.extend([
                {
                    "category": "network",
                    "priority": "high",
                    "recommendation": "Optimize network topology and reduce network hops",
                    "rationale": "Minimize peer-to-peer communication latency"
                },
                {
                    "category": "network",
                    "priority": "medium",
                    "recommendation": "Increase network bandwidth or use dedicated network interfaces",
                    "rationale": "Reduce network utilization and eliminate packet drops"
                }
            ])
        
        # Memory recommendations
        if bottlenecks.get('memory_bottlenecks'):
            recommendations.extend([
                {
                    "category": "memory",
                    "priority": "high",
                    "recommendation": "Increase memory limits for etcd pods",
                    "rationale": "Prevent memory pressure and potential OOM kills"
                },
                {
                    "category": "memory",
                    "priority": "medium",
                    "recommendation": "Monitor for memory leaks and optimize database size",
                    "rationale": "Maintain stable memory usage patterns"
                }
            ])
        
        # Consensus recommendations
        if bottlenecks.get('consensus_bottlenecks'):
            recommendations.extend([
                {
                    "category": "consensus",
                    "priority": "high",
                    "recommendation": "Review etcd configuration and tune performance parameters",
                    "rationale": "Optimize consensus algorithm performance"
                },
                {
                    "category": "consensus",
                    "priority": "medium",
                    "recommendation": "Consider database compaction and defragmentation",
                    "rationale": "Improve apply operation performance"
                }
            ])
        
        if not recommendations:
            recommendations.append({
                "category": "general",
                "priority": "low",
                "recommendation": "Continue monitoring etcd cluster performance",
                "rationale": "No significant bottlenecks detected in current analysis"
            })
        
        return recommendations