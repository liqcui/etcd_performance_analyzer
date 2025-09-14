"""
etcd Analyzer Performance Report Module
Comprehensive performance analysis and reporting for etcd clusters
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pytz
from analysis.etcd_analyzer_performance_utility import etcdAnalyzerUtility


class etcdReportAnalyzer:
    """Performance report analyzer for etcd cluster metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.utility = etcdAnalyzerUtility()
        self.timezone = pytz.UTC
        
        # Performance thresholds (based on etcd best practices)
        self.thresholds = {
            'wal_fsync_p99_ms': 10.0,  # 10ms threshold
            'backend_commit_p99_ms': 25.0,  # 25ms threshold
            'cpu_usage_warning': 70.0,  # 70% CPU usage warning
            'cpu_usage_critical': 85.0,  # 85% CPU usage critical
            'memory_usage_warning': 70.0,  # 70% memory usage warning
            'memory_usage_critical': 85.0,  # 85% memory usage critical
            'peer_latency_warning_ms': 50.0,  # 50ms peer latency warning
            'peer_latency_critical_ms': 100.0,  # 100ms peer latency critical
            'network_utilization_warning': 70.0,  # 70% network utilization
            'network_utilization_critical': 85.0,  # 85% network utilization
        }

        # Add advanced thresholds for root cause analysis
        self.advanced_thresholds = {
            'disk_write_throughput_mb_s': 50.0,
            'disk_iops_write_min': 1000,
            'network_peer_bytes_threshold': 10000,
            'compaction_duration_warning_ms': 100,
            'proposal_pending_critical': 5,
        }    

    def analyze_performance_metrics(self, metrics_data: Dict[str, Any], test_id: str) -> Dict[str, Any]:
        """Analyze comprehensive performance metrics"""
        try:
            analysis_results = {
                'test_id': test_id,
                'timestamp': self.utility.format_timestamp(),
                'duration': metrics_data.get('duration', '1h'),
                'status': 'success',
                'critical_metrics_analysis': {},
                'performance_summary': {},
                'baseline_comparison': {},
                'recommendations': [],
                'alerts': [],
                'metric_tables': {}
            }
            
            data = metrics_data.get('data', {})
            
            # Analyze critical metrics (WAL fsync and backend commit)
            analysis_results['critical_metrics_analysis'] = self._analyze_critical_metrics(data)
            
            # Analyze supporting metrics (CPU, memory, network, disk)
            analysis_results['performance_summary'] = self._analyze_supporting_metrics(data)
            
            # Generate baseline comparison
            analysis_results['baseline_comparison'] = self._generate_baseline_comparison(data)
            
            # Generate recommendations based on analysis
            analysis_results['recommendations'] = self._generate_recommendations(
                analysis_results['critical_metrics_analysis'],
                analysis_results['performance_summary']
            )
            
            # Generate alerts for critical issues
            analysis_results['alerts'] = self._generate_alerts(
                analysis_results['critical_metrics_analysis'],
                analysis_results['performance_summary']
            )
            
            # Create formatted metric tables
            analysis_results['metric_tables'] = self._create_metric_tables(data)
            
            return analysis_results
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance metrics: {e}")
            return {
                'test_id': test_id,
                'timestamp': self.utility.format_timestamp(),
                'status': 'error',
                'error': str(e)
            }
    
    def _analyze_critical_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze critical performance metrics (WAL fsync and backend commit)"""
        critical_analysis = {
            'wal_fsync_analysis': {},
            'backend_commit_analysis': {},
            'overall_disk_health': 'unknown'
        }
        
        try:
            # Analyze WAL fsync metrics
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            
            if wal_p99_metrics:
                wal_analysis = self._analyze_latency_metrics(
                    wal_p99_metrics, 
                    'wal_fsync_p99', 
                    self.thresholds['wal_fsync_p99_ms']
                )
                critical_analysis['wal_fsync_analysis'] = wal_analysis
            
            # Analyze backend commit metrics
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            
            if backend_p99_metrics:
                backend_analysis = self._analyze_latency_metrics(
                    backend_p99_metrics, 
                    'backend_commit_p99', 
                    self.thresholds['backend_commit_p99_ms']
                )
                critical_analysis['backend_commit_analysis'] = backend_analysis
            
            # Determine overall disk health
            critical_analysis['overall_disk_health'] = self._determine_disk_health(
                critical_analysis['wal_fsync_analysis'],
                critical_analysis['backend_commit_analysis']
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing critical metrics: {e}")
            critical_analysis['error'] = str(e)
            
        return critical_analysis
    
    def _analyze_latency_metrics(self, metrics: List[Dict[str, Any]], metric_type: str, threshold_ms: float) -> Dict[str, Any]:
        """Analyze latency metrics against thresholds"""
        analysis = {
            'metric_type': metric_type,
            'threshold_ms': threshold_ms,
            'pod_results': [],
            'cluster_summary': {},
            'health_status': 'unknown'
        }
        
        try:
            avg_latencies = []
            max_latencies = []
            issues_found = 0
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_value = metric.get('avg', 0)
                max_value = metric.get('max', 0)
                unit = metric.get('unit', 'seconds')
                
                # Convert to milliseconds if needed
                if unit == 'seconds':
                    avg_ms = avg_value * 1000
                    max_ms = max_value * 1000
                else:
                    avg_ms = avg_value
                    max_ms = max_value
                
                # Analyze against threshold
                avg_exceeds = avg_ms > threshold_ms
                max_exceeds = max_ms > threshold_ms
                
                if avg_exceeds or max_exceeds:
                    issues_found += 1
                
                pod_result = {
                    'pod_name': pod_name,
                    'avg_ms': round(avg_ms, 3),
                    'max_ms': round(max_ms, 3),
                    'avg_exceeds_threshold': avg_exceeds,
                    'max_exceeds_threshold': max_exceeds,
                    'status': 'critical' if avg_exceeds else ('warning' if max_exceeds else 'good')
                }
                
                analysis['pod_results'].append(pod_result)
                avg_latencies.append(avg_ms)
                max_latencies.append(max_ms)
            
            # Cluster summary
            if avg_latencies:
                analysis['cluster_summary'] = {
                    'avg_latency_ms': round(sum(avg_latencies) / len(avg_latencies), 3),
                    'max_latency_ms': round(max(max_latencies), 3),
                    'pods_with_issues': issues_found,
                    'total_pods': len(metrics),
                    'threshold_exceeded': issues_found > 0
                }
                
                # Determine health status
                if issues_found == 0:
                    analysis['health_status'] = 'excellent'
                elif issues_found <= len(metrics) / 2:
                    analysis['health_status'] = 'warning'
                else:
                    analysis['health_status'] = 'critical'
            
        except Exception as e:
            self.logger.error(f"Error analyzing latency metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_supporting_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze supporting performance metrics (CPU, memory, network, disk I/O)"""
        supporting_analysis = {
            'cpu_analysis': {},
            'memory_analysis': {},
            'network_analysis': {},
            'disk_io_analysis': {}
        }
        
        try:
            # Analyze CPU usage
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            supporting_analysis['cpu_analysis'] = self._analyze_resource_metrics(
                cpu_metrics, 'cpu', self.thresholds['cpu_usage_warning'], self.thresholds['cpu_usage_critical']
            )
            
            # Analyze memory usage
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            supporting_analysis['memory_analysis'] = self._analyze_resource_metrics(
                memory_metrics, 'memory', self.thresholds['memory_usage_warning'], self.thresholds['memory_usage_critical']
            )
            
            # Analyze network performance
            network_data = data.get('network_data', {})
            supporting_analysis['network_analysis'] = self._analyze_network_metrics(network_data)
            
            # Analyze disk I/O
            disk_data = data.get('disk_io_data', [])
            supporting_analysis['disk_io_analysis'] = self._analyze_disk_io_metrics(disk_data)
            
        except Exception as e:
            self.logger.error(f"Error analyzing supporting metrics: {e}")
            supporting_analysis['error'] = str(e)
            
        return supporting_analysis
    
    def _analyze_resource_metrics(self, metrics: List[Dict[str, Any]], resource_type: str, 
                                warning_threshold: float, critical_threshold: float) -> Dict[str, Any]:
        """Analyze resource utilization metrics (CPU/Memory)"""
        analysis = {
            'resource_type': resource_type,
            'warning_threshold': warning_threshold,
            'critical_threshold': critical_threshold,
            'pod_results': [],
            'cluster_summary': {},
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
            
            usage_values = []
            critical_pods = 0
            warning_pods = 0
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                unit = metric.get('unit', 'percent')
                
                # For memory metrics, convert to percentage of node capacity
                if resource_type == 'memory' and unit == 'MB':
                    # Get node memory capacity (default 16GB if not available)
                    node_memory_gb = 16.0  # Conservative estimate
                    avg_percent = (avg_usage / 1024) / node_memory_gb * 100
                    max_percent = (max_usage / 1024) / node_memory_gb * 100
                else:
                    avg_percent = avg_usage
                    max_percent = max_usage
                
                # Determine status
                status = 'good'
                if max_percent >= critical_threshold:
                    status = 'critical'
                    critical_pods += 1
                elif max_percent >= warning_threshold or avg_percent >= warning_threshold:
                    status = 'warning'
                    warning_pods += 1
                
                pod_result = {
                    'pod_name': pod_name,
                    'avg_usage': round(avg_percent, 2),
                    'max_usage': round(max_percent, 2),
                    'unit': 'percent' if resource_type == 'memory' and unit == 'MB' else unit,
                    'status': status
                }
                
                analysis['pod_results'].append(pod_result)
                usage_values.append(avg_percent)
            
            # Cluster summary
            analysis['cluster_summary'] = {
                'avg_usage': round(sum(usage_values) / len(usage_values), 2),
                'max_usage': round(max([m['max_usage'] for m in analysis['pod_results']]), 2),
                'critical_pods': critical_pods,
                'warning_pods': warning_pods,
                'total_pods': len(metrics)
            }
            
            # Health status
            if critical_pods > 0:
                analysis['health_status'] = 'critical'
            elif warning_pods > 0:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'good'
                
        except Exception as e:
            self.logger.error(f"Error analyzing {resource_type} metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis

    def _analyze_network_metrics(self, network_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network performance metrics"""
        analysis = {
            'peer_latency_analysis': {},
            'network_utilization_analysis': {},
            'packet_drop_analysis': {},
            'health_status': 'unknown'
        }
        
        try:
            # Analyze peer-to-peer latency
            pod_metrics = network_data.get('pod_metrics', [])
            peer_latency_metrics = [m for m in pod_metrics if 'peer2peer_latency' in m.get('metric_name', '')]
            
            if peer_latency_metrics:
                analysis['peer_latency_analysis'] = self._analyze_latency_metrics(
                    peer_latency_metrics, 
                    'peer_latency', 
                    self.thresholds['peer_latency_critical_ms']
                )
            
            # Analyze network utilization
            node_metrics = network_data.get('node_metrics', [])
            utilization_metrics = [m for m in node_metrics if 'utilization' in m.get('metric_name', '')]
            analysis['network_utilization_analysis'] = self._analyze_network_utilization(utilization_metrics)
            
            # Analyze packet drops
            drop_metrics = [m for m in node_metrics if 'drop' in m.get('metric_name', '')]
            analysis['packet_drop_analysis'] = self._analyze_packet_drops(drop_metrics)
            
            # Overall network health
            latency_health = analysis['peer_latency_analysis'].get('health_status', 'unknown')
            utilization_health = analysis['network_utilization_analysis'].get('health_status', 'unknown')
            drop_health = analysis['packet_drop_analysis'].get('health_status', 'unknown')
            
            health_scores = {'excellent': 4, 'good': 3, 'warning': 2, 'critical': 1, 'unknown': 0}
            avg_score = sum(health_scores.get(h, 0) for h in [latency_health, utilization_health, drop_health]) / 3
            
            if avg_score >= 3.5:
                analysis['health_status'] = 'excellent'
            elif avg_score >= 2.5:
                analysis['health_status'] = 'good'
            elif avg_score >= 1.5:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'critical'
            
        except Exception as e:
            self.logger.error(f"Error analyzing network metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_network_utilization(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze network utilization metrics"""
        analysis = {
            'node_results': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
                
            high_util_nodes = 0
            
            for metric in metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_util = metric.get('avg', 0)
                max_util = metric.get('max', 0)
                
                # Convert bits to percentage (assuming 1Gbps = 1,000,000,000 bits/s)
                if metric.get('unit') == 'bits_per_second':
                    # Convert to percentage of 1Gbps
                    avg_percent = (avg_util / 1000000000) * 100
                    max_percent = (max_util / 1000000000) * 100
                else:
                    avg_percent = avg_util
                    max_percent = max_util
                
                status = 'good'
                if max_percent > self.thresholds['network_utilization_critical']:
                    status = 'critical'
                    high_util_nodes += 1
                elif avg_percent > self.thresholds['network_utilization_warning']:
                    status = 'warning'
                    high_util_nodes += 1
                
                analysis['node_results'].append({
                    'node_name': node_name,
                    'avg_utilization_percent': round(avg_percent, 2),
                    'max_utilization_percent': round(max_percent, 2),
                    'status': status
                })
            
            # Health status
            if high_util_nodes == 0:
                analysis['health_status'] = 'good'
            elif high_util_nodes <= len(metrics) / 2:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'critical'
                
        except Exception as e:
            self.logger.error(f"Error analyzing network utilization: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_packet_drops(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze packet drop metrics"""
        analysis = {
            'node_results': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
                
            nodes_with_drops = 0
            
            for metric in metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_drops = metric.get('avg', 0)
                max_drops = metric.get('max', 0)
                
                status = 'good'
                if avg_drops > 0 or max_drops > 0:
                    status = 'warning' if avg_drops < 1 else 'critical'
                    nodes_with_drops += 1
                
                analysis['node_results'].append({
                    'node_name': node_name,
                    'avg_drops_per_sec': round(avg_drops, 6),
                    'max_drops_per_sec': round(max_drops, 6),
                    'status': status
                })
            
            # Health status
            if nodes_with_drops == 0:
                analysis['health_status'] = 'good'
            else:
                analysis['health_status'] = 'warning'
                
        except Exception as e:
            self.logger.error(f"Error analyzing packet drops: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_disk_io_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze disk I/O performance metrics"""
        analysis = {
            'throughput_analysis': [],
            'iops_analysis': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
            
            # Separate throughput and IOPS metrics
            throughput_metrics = [m for m in metrics if 'throughput' in m.get('metric_name', '')]
            iops_metrics = [m for m in metrics if 'iops' in m.get('metric_name', '')]
            
            # Analyze throughput
            for metric in throughput_metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_throughput = metric.get('avg', 0)
                max_throughput = metric.get('max', 0)
                unit = metric.get('unit', 'bytes_per_second')
                
                # Convert to MB/s for readability
                if unit == 'bytes_per_second':
                    avg_mb_s = avg_throughput / (1024 * 1024)
                    max_mb_s = max_throughput / (1024 * 1024)
                else:
                    avg_mb_s = avg_throughput
                    max_mb_s = max_throughput
                
                analysis['throughput_analysis'].append({
                    'node_name': node_name,
                    'metric_type': metric.get('metric_name', 'unknown'),
                    'avg_mb_per_sec': round(avg_mb_s, 2),
                    'max_mb_per_sec': round(max_mb_s, 2),
                    'devices': metric.get('devices', [])
                })
            
            # Analyze IOPS
            for metric in iops_metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_iops = metric.get('avg', 0)
                max_iops = metric.get('max', 0)
                
                analysis['iops_analysis'].append({
                    'node_name': node_name,
                    'metric_type': metric.get('metric_name', 'unknown'),
                    'avg_iops': round(avg_iops, 2),
                    'max_iops': round(max_iops, 2),
                    'devices': metric.get('devices', [])
                })
            
            # Simple health assessment - if we have data, it's at least 'good'
            analysis['health_status'] = 'good' if (throughput_metrics or iops_metrics) else 'no_data'
            
        except Exception as e:
            self.logger.error(f"Error analyzing disk I/O metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis

    async def script_based_root_cause_analysis(self, failed_thresholds: List[Dict[str, Any]], 
                                                metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform script-based root cause analysis (called by agent) - FIXED: Made async"""
        script_analysis = {
            'disk_io_analysis': {},
            'network_analysis': {},
            'consensus_analysis': {},
            'resource_contention_analysis': {}
        }
        
        try:
            data = metrics_data.get('data', {})
            
            # Analyze disk I/O patterns if latency thresholds failed
            latency_failures = [f for f in failed_thresholds if f['threshold_type'] == 'latency']
            if latency_failures:
                script_analysis['disk_io_analysis'] = await self._analyze_disk_io_patterns(data)
                script_analysis['consensus_analysis'] = await self._analyze_consensus_patterns(data)
            
            # Analyze network patterns if performance issues detected
            if any(f['metric'] in ['wal_fsync_p99', 'backend_commit_p99'] for f in failed_thresholds):
                script_analysis['network_analysis'] = await self._analyze_network_patterns(data)
            
            # Analyze resource contention
            cpu_failures = [f for f in failed_thresholds if f['metric'] == 'cpu_usage']
            if cpu_failures:
                script_analysis['resource_contention_analysis'] = await self._analyze_resource_contention(data)
            
        except Exception as e:
            self.logger.error(f"Error in script-based root cause analysis: {e}")
            script_analysis['error'] = str(e)
            
        return script_analysis

    def _determine_disk_health(self, wal_analysis: Dict[str, Any], backend_analysis: Dict[str, Any]) -> str:
        """Determine overall disk health based on WAL fsync and backend commit analysis"""
        try:
            wal_health = wal_analysis.get('health_status', 'unknown')
            backend_health = backend_analysis.get('health_status', 'unknown')
            
            # Priority: critical > warning > good > excellent > unknown
            health_priorities = {'critical': 4, 'warning': 3, 'good': 2, 'excellent': 1, 'unknown': 0}
            
            wal_priority = health_priorities.get(wal_health, 0)
            backend_priority = health_priorities.get(backend_health, 0)
            
            # Take the worse of the two
            max_priority = max(wal_priority, backend_priority)
            
            for health, priority in health_priorities.items():
                if priority == max_priority:
                    return health
                    
            return 'unknown'
            
        except Exception as e:
            self.logger.error(f"Error determining disk health: {e}")
            return 'unknown'
    
    def _generate_baseline_comparison(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate baseline comparison analysis"""
        comparison = {
            'benchmark_standards': {
                'wal_fsync_p99_target_ms': self.thresholds['wal_fsync_p99_ms'],
                'backend_commit_p99_target_ms': self.thresholds['backend_commit_p99_ms'],
                'cpu_usage_target_percent': 70.0,
                'memory_usage_target_percent': 70.0,
                'peer_latency_target_ms': self.thresholds['peer_latency_warning_ms']
            },
            'current_vs_baseline': {},
            'performance_grade': 'unknown'
        }
        
        try:
            # Compare WAL fsync against baseline
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            
            if wal_p99_metrics:
                avg_wal_latency = sum(m.get('avg', 0) * 1000 for m in wal_p99_metrics) / len(wal_p99_metrics)
                comparison['current_vs_baseline']['wal_fsync_p99_ms'] = {
                    'current': round(avg_wal_latency, 3),
                    'target': self.thresholds['wal_fsync_p99_ms'],
                    'within_target': avg_wal_latency <= self.thresholds['wal_fsync_p99_ms']
                }
            
            # Compare backend commit against baseline
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            
            if backend_p99_metrics:
                avg_backend_latency = sum(m.get('avg', 0) * 1000 for m in backend_p99_metrics) / len(backend_p99_metrics)
                comparison['current_vs_baseline']['backend_commit_p99_ms'] = {
                    'current': round(avg_backend_latency, 3),
                    'target': self.thresholds['backend_commit_p99_ms'],
                    'within_target': avg_backend_latency <= self.thresholds['backend_commit_p99_ms']
                }
            
            # Compare CPU usage against baseline
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            
            if cpu_metrics:
                avg_cpu_usage = sum(m.get('avg', 0) for m in cpu_metrics) / len(cpu_metrics)
                comparison['current_vs_baseline']['cpu_usage_percent'] = {
                    'current': round(avg_cpu_usage, 2),
                    'target': 70.0,
                    'within_target': avg_cpu_usage <= 70.0
                }
            
            # Compare memory usage against baseline - FIX: Get actual node memory capacity
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            
            if memory_metrics:
                # Get node memory capacity from cluster info or estimate based on typical OpenShift nodes
                node_memory_capacity_gb = self._get_node_memory_capacity(data)
                
                avg_memory_mb = sum(m.get('avg', 0) for m in memory_metrics) / len(memory_metrics)
                avg_memory_gb = avg_memory_mb / 1024
                
                # Calculate actual percentage of node memory used by etcd
                avg_memory_percent = (avg_memory_gb / node_memory_capacity_gb) * 100
                
                comparison['current_vs_baseline']['memory_usage_percent'] = {
                    'current': round(avg_memory_percent, 2),
                    'target': 70.0,
                    'within_target': avg_memory_percent <= 70.0
                }
            
            # Calculate performance grade
            comparison['performance_grade'] = self._calculate_performance_grade(comparison['current_vs_baseline'])
            
        except Exception as e:
            self.logger.error(f"Error generating baseline comparison: {e}")
            comparison['error'] = str(e)
            
        return comparison

    def _calculate_performance_grade(self, baseline_comparison: Dict[str, Any]) -> str:
        """Calculate overall performance grade"""
        try:
            within_target_count = 0
            total_metrics = len(baseline_comparison)
            
            if total_metrics == 0:
                return 'insufficient_data'
            
            for metric, comparison in baseline_comparison.items():
                if comparison.get('within_target', False):
                    within_target_count += 1
            
            percentage = (within_target_count / total_metrics) * 100
            
            if percentage >= 90:
                return 'excellent'
            elif percentage >= 75:
                return 'good'
            elif percentage >= 50:
                return 'fair'
            else:
                return 'poor'
                
        except Exception as e:
            self.logger.error(f"Error calculating performance grade: {e}")
            return 'unknown'
    
    def _generate_recommendations(self, critical_analysis: Dict[str, Any], 
                                performance_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate performance optimization recommendations"""
        recommendations = []
        
        try:
            # Check WAL fsync performance
            wal_analysis = critical_analysis.get('wal_fsync_analysis', {})
            if wal_analysis.get('health_status') in ['critical', 'warning']:
                recommendations.extend([
                    {
                        'category': 'disk_performance',
                        'priority': 'high',
                        'issue': 'High WAL fsync latency detected',
                        'recommendation': 'Upgrade to high-performance NVMe SSDs with low latency',
                        'rationale': 'Ensure etcd gets adequate CPU resources over other processes'
                    }
                ])
            
            # Check network performance
            network_analysis = performance_analysis.get('network_analysis', {})
            peer_latency_health = network_analysis.get('peer_latency_analysis', {}).get('health_status')
            if peer_latency_health in ['critical', 'warning']:
                recommendations.extend([
                    {
                        'category': 'network_optimization',
                        'priority': 'high',
                        'issue': 'High peer-to-peer network latency',
                        'recommendation': 'Optimize network topology and reduce network hops between etcd members',
                        'rationale': 'High network latency affects cluster consensus and performance'
                    },
                    {
                        'category': 'network_capacity',
                        'priority': 'medium',
                        'issue': 'Network performance issues',
                        'recommendation': 'Consider dedicated network interfaces or higher bandwidth',
                        'rationale': 'Ensure adequate network capacity for etcd cluster communication'
                    }
                ])
            
            # Add general recommendations if no specific issues
            if not recommendations:
                recommendations.extend([
                    {
                        'category': 'monitoring',
                        'priority': 'low',
                        'issue': 'Ongoing performance monitoring',
                        'recommendation': 'Continue regular performance monitoring and analysis',
                        'rationale': 'No critical issues detected, maintain current monitoring practices'
                    },
                    {
                        'category': 'preventive_maintenance',
                        'priority': 'low',
                        'issue': 'Preventive maintenance',
                        'recommendation': 'Schedule regular database compaction and system maintenance',
                        'rationale': 'Preventive maintenance helps maintain optimal performance'
                    }
                ])
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")
            recommendations.append({
                'category': 'error',
                'priority': 'high',
                'issue': 'Failed to generate recommendations',
                'recommendation': f'Review analysis error: {str(e)}',
                'rationale': 'Error occurred during recommendation generation'
            })
            
        return recommendations
    
    def _generate_alerts(self, critical_analysis: Dict[str, Any], 
                        performance_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts for critical performance issues"""
        alerts = []
        
        try:
            # Critical disk performance alerts
            wal_health = critical_analysis.get('wal_fsync_analysis', {}).get('health_status')
            if wal_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'disk_performance',
                    'message': 'WAL fsync latency exceeds critical threshold (>10ms)',
                    'impact': 'High write latency affecting cluster stability',
                    'action_required': 'Immediate storage performance investigation required'
                })
            
            backend_health = critical_analysis.get('backend_commit_analysis', {}).get('health_status')
            if backend_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'disk_performance',
                    'message': 'Backend commit latency exceeds critical threshold (>25ms)',
                    'impact': 'Database write performance significantly degraded',
                    'action_required': 'Immediate storage optimization required'
                })
            
            # CPU starvation alerts
            cpu_health = performance_analysis.get('cpu_analysis', {}).get('health_status')
            if cpu_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'resource_starvation',
                    'message': 'Critical CPU utilization detected on etcd pods',
                    'impact': 'Potential CPU starvation affecting etcd performance',
                    'action_required': 'Increase CPU resources or reduce load immediately'
                })
            
            # Network performance alerts
            network_health = performance_analysis.get('network_analysis', {}).get('health_status')
            if network_health == 'critical':
                alerts.append({
                    'severity': 'warning',
                    'category': 'network_performance',
                    'message': 'Network performance issues detected',
                    'impact': 'Cluster communication may be affected',
                    'action_required': 'Investigate network connectivity and bandwidth'
                })
            
        except Exception as e:
            self.logger.error(f"Error generating alerts: {e}")
            alerts.append({
                'severity': 'error',
                'category': 'system_error',
                'message': f'Alert generation failed: {str(e)}',
                'impact': 'Unable to assess critical alerts',
                'action_required': 'Review system logs for alert generation errors'
            })
            
        return alerts
    
    def _create_metric_tables(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Create formatted metric tables for display"""
        tables = {}
        
        try:
            # WAL fsync table
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            if wal_p99_metrics:
                tables['wal_fsync_p99'] = self._format_latency_table(
                    wal_p99_metrics, 'WAL Fsync P99 Latency', self.thresholds['wal_fsync_p99_ms']
                )
            
            # Backend commit table
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            if backend_p99_metrics:
                tables['backend_commit_p99'] = self._format_latency_table(
                    backend_p99_metrics, 'Backend Commit P99 Latency', self.thresholds['backend_commit_p99_ms']
                )
            
            # CPU usage table
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            if cpu_metrics:
                tables['cpu_usage'] = self._format_resource_table(cpu_metrics, 'CPU Usage')
            
            # Memory usage table
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            if memory_metrics:
                tables['memory_usage'] = self._format_memory_table(memory_metrics, 'Memory Usage')
            
        except Exception as e:
            self.logger.error(f"Error creating metric tables: {e}")
            tables['error'] = f"Error creating tables: {str(e)}"
            
        return tables
    
    def _format_latency_table(self, metrics: List[Dict[str, Any]], title: str, threshold_ms: float) -> str:
        """Format latency metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (ms)':<12} {'Max (ms)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_value = metric.get('avg', 0)
                max_value = metric.get('max', 0)
                unit = metric.get('unit', 'seconds')
                
                # Convert to milliseconds
                if unit == 'seconds':
                    avg_ms = avg_value * 1000
                    max_ms = max_value * 1000
                else:
                    avg_ms = avg_value
                    max_ms = max_value
                
                # Determine status
                if avg_ms > threshold_ms:
                    status = "CRITICAL"
                elif max_ms > threshold_ms:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_ms:<12.3f} {max_ms:<12.3f} {status:<10}")
            
            table_lines.append(f"\nThreshold: {threshold_ms} ms")
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting latency table: {str(e)}"
    
    def _format_resource_table(self, metrics: List[Dict[str, Any]], title: str) -> str:
        """Format resource metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (%)':<12} {'Max (%)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                
                # Determine status
                if max_usage > self.thresholds['cpu_usage_critical']:
                    status = "CRITICAL"
                elif avg_usage > self.thresholds['cpu_usage_warning']:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_usage:<12.2f} {max_usage:<12.2f} {status:<10}")
            
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting resource table: {str(e)}"
    
    def _format_memory_table(self, metrics: List[Dict[str, Any]], title: str) -> str:
        """Format memory metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (MB)':<12} {'Max (MB)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                
                # Estimate status based on memory usage (assuming reasonable limits)
                memory_percent = (avg_usage / 1024) * 100  # Assuming ~1GB as reasonable limit
                if memory_percent > 85:
                    status = "CRITICAL"
                elif memory_percent > 70:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_usage:<12.2f} {max_usage:<12.2f} {status:<10}")
            
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting memory table: {str(e)}"
    
    def generate_performance_report(self, analysis_results: Dict[str, Any], 
                                  test_id: str, duration: str) -> str:
        """Generate comprehensive performance report"""
        try:
            report_lines = []
            
            # Header
            report_lines.extend([
                "=" * 100,
                "ETCD PERFORMANCE ANALYSIS REPORT",
                "=" * 100,
                f"Test ID: {test_id}",
                f"Analysis Duration: {duration}",
                f"Report Generated: {datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                f"Analysis Status: {analysis_results.get('status', 'unknown').upper()}",
                ""
            ])
            
            # Executive Summary
            report_lines.extend([
                "EXECUTIVE SUMMARY",
                "=" * 50,
                ""
            ])
            
            # Critical metrics summary
            critical_analysis = analysis_results.get('critical_metrics_analysis', {})
            overall_disk_health = critical_analysis.get('overall_disk_health', 'unknown')
            
            report_lines.append(f"Overall Disk Performance Health: {overall_disk_health.upper()}")
            
            # Baseline comparison
            baseline_comparison = analysis_results.get('baseline_comparison', {})
            performance_grade = baseline_comparison.get('performance_grade', 'unknown')
            report_lines.append(f"Performance Grade: {performance_grade.upper()}")
            report_lines.append("")
            
            # Alerts section
            alerts = analysis_results.get('alerts', [])
            if alerts:
                report_lines.extend([
                    "CRITICAL ALERTS",
                    "=" * 50,
                    ""
                ])
                
                for alert in alerts:
                    severity = alert.get('severity', 'unknown').upper()
                    message = alert.get('message', 'No message')
                    impact = alert.get('impact', 'Unknown impact')
                    action = alert.get('action_required', 'No action specified')
                    
                    report_lines.extend([
                        f"[{severity}] {message}",
                        f"Impact: {impact}",
                        f"Action Required: {action}",
                        ""
                    ])
            else:
                report_lines.extend([
                    "CRITICAL ALERTS",
                    "=" * 50,
                    "No critical alerts detected.",
                    ""
                ])
            
            # Critical Metrics Analysis
            report_lines.extend([
                "CRITICAL METRICS ANALYSIS",
                "=" * 50,
                ""
            ])
            
            # WAL fsync analysis
            wal_analysis = critical_analysis.get('wal_fsync_analysis', {})
            if wal_analysis:
                cluster_summary = wal_analysis.get('cluster_summary', {})
                health_status = wal_analysis.get('health_status', 'unknown')
                
                report_lines.extend([
                    "WAL Fsync Performance:",
                    f"  Health Status: {health_status.upper()}",
                    f"  Average Latency: {cluster_summary.get('avg_latency_ms', 'N/A')} ms",
                    f"  Maximum Latency: {cluster_summary.get('max_latency_ms', 'N/A')} ms",
                    f"  Threshold: {self.thresholds['wal_fsync_p99_ms']} ms",
                    f"  Pods with Issues: {cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Backend commit analysis
            backend_analysis = critical_analysis.get('backend_commit_analysis', {})
            if backend_analysis:
                cluster_summary = backend_analysis.get('cluster_summary', {})
                health_status = backend_analysis.get('health_status', 'unknown')
                
                report_lines.extend([
                    "Backend Commit Performance:",
                    f"  Health Status: {health_status.upper()}",
                    f"  Average Latency: {cluster_summary.get('avg_latency_ms', 'N/A')} ms",
                    f"  Maximum Latency: {cluster_summary.get('max_latency_ms', 'N/A')} ms",
                    f"  Threshold: {self.thresholds['backend_commit_p99_ms']} ms",
                    f"  Pods with Issues: {cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Supporting metrics summary
            performance_summary = analysis_results.get('performance_summary', {})
            
            # CPU Analysis
            cpu_analysis = performance_summary.get('cpu_analysis', {})
            if cpu_analysis.get('cluster_summary'):
                cpu_summary = cpu_analysis['cluster_summary']
                report_lines.extend([
                    "CPU Performance:",
                    f"  Health Status: {cpu_analysis.get('health_status', 'unknown').upper()}",
                    f"  Average Usage: {cpu_summary.get('avg_usage', 'N/A')}%",
                    f"  Maximum Usage: {cpu_summary.get('max_usage', 'N/A')}%",
                    f"  Critical Pods: {cpu_summary.get('critical_pods', 0)}/{cpu_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Memory Analysis
            memory_analysis = performance_summary.get('memory_analysis', {})
            if memory_analysis.get('cluster_summary'):
                memory_summary = memory_analysis['cluster_summary']
                report_lines.extend([
                    "Memory Performance:",
                    f"  Health Status: {memory_analysis.get('health_status', 'unknown').upper()}",
                    f"  Average Usage: {memory_summary.get('avg_usage', 'N/A')} MB",
                    f"  Maximum Usage: {memory_summary.get('max_usage', 'N/A')} MB",
                    f"  Critical Pods: {memory_summary.get('critical_pods', 0)}/{memory_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Network Analysis
            network_analysis = performance_summary.get('network_analysis', {})
            if network_analysis:
                report_lines.extend([
                    "Network Performance:",
                    f"  Overall Health: {network_analysis.get('health_status', 'unknown').upper()}",
                ])
                
                # Peer latency details
                peer_latency = network_analysis.get('peer_latency_analysis', {})
                if peer_latency.get('cluster_summary'):
                    peer_summary = peer_latency['cluster_summary']
                    report_lines.extend([
                        f"  Peer Latency: {peer_summary.get('avg_latency_ms', 'N/A')} ms avg, {peer_summary.get('max_latency_ms', 'N/A')} ms max",
                        f"  Pods with Latency Issues: {peer_summary.get('pods_with_issues', 0)}/{peer_summary.get('total_pods', 0)}"
                    ])
                
                report_lines.append("")
            
            # Detailed Metric Tables
            report_lines.extend([
                "DETAILED METRICS",
                "=" * 50
            ])
            
            metric_tables = analysis_results.get('metric_tables', {})
            for table_name, table_content in metric_tables.items():
                if table_content:
                    report_lines.append(table_content)
                    report_lines.append("")
            
            # Baseline Comparison
            report_lines.extend([
                "BASELINE COMPARISON",
                "=" * 50,
                ""
            ])
            
            current_vs_baseline = baseline_comparison.get('current_vs_baseline', {})
            benchmark_standards = baseline_comparison.get('benchmark_standards', {})
            
            if current_vs_baseline:
                report_lines.append(f"{'Metric':<35} {'Current':<15} {'Target':<15} {'Status':<10}")
                report_lines.append("-" * 75)
                
                for metric, comparison in current_vs_baseline.items():
                    current = comparison.get('current', 'N/A')
                    target = comparison.get('target', 'N/A')
                    within_target = comparison.get('within_target', False)
                    status = "PASS" if within_target else "FAIL"
                    
                    # Format metric name for display
                    display_name = metric.replace('_', ' ').title()
                    
                    report_lines.append(f"{display_name:<35} {current:<15} {target:<15} {status:<10}")
                
                report_lines.extend(["", f"Overall Performance Grade: {performance_grade.upper()}", ""])
            
            # Recommendations
            recommendations = analysis_results.get('recommendations', [])
            if recommendations:
                report_lines.extend([
                    "RECOMMENDATIONS",
                    "=" * 50,
                    ""
                ])
                
                # Group by priority
                high_priority = [r for r in recommendations if r.get('priority') == 'high']
                medium_priority = [r for r in recommendations if r.get('priority') == 'medium']
                low_priority = [r for r in recommendations if r.get('priority') == 'low']
                
                for priority, recs in [("HIGH PRIORITY", high_priority), ("MEDIUM PRIORITY", medium_priority), ("LOW PRIORITY", low_priority)]:
                    if recs:
                        report_lines.extend([priority, "-" * len(priority), ""])
                        
                        for i, rec in enumerate(recs, 1):
                            report_lines.extend([
                                f"{i}. {rec.get('issue', 'Unknown issue')}",
                                f"   Category: {rec.get('category', 'unknown').replace('_', ' ').title()}",
                                f"   Recommendation: {rec.get('recommendation', 'No recommendation')}",
                                f"   Rationale: {rec.get('rationale', 'No rationale provided')}",
                                ""
                            ])
            
            # Analysis Methodology
            report_lines.extend([
                "ANALYSIS METHODOLOGY",
                "=" * 50,
                "",
                "This report analyzes etcd performance based on industry best practices:",
                "",
                "Critical Thresholds:",
                f"• WAL fsync P99 latency should be < {self.thresholds['wal_fsync_p99_ms']} ms",
                f"• Backend commit P99 latency should be < {self.thresholds['backend_commit_p99_ms']} ms",
                f"• CPU usage should remain below {self.thresholds['cpu_usage_warning']}% (warning) / {self.thresholds['cpu_usage_critical']}% (critical)",
                f"• Memory usage monitoring for resource planning",
                f"• Network peer latency should be < {self.thresholds['peer_latency_warning_ms']} ms",
                "",
                "Performance degradation can be caused by:",
                "1. Slow disk I/O (most common) - upgrade to NVMe SSDs, dedicated storage",
                "2. CPU starvation - increase CPU resources, process isolation",
                "3. Network issues - optimize topology, increase bandwidth",
                "4. Memory pressure - monitor and increase as needed",
                "",
                "Data Sources:",
                f"• Metrics collection duration: {duration}",
                f"• Test ID: {test_id}",
                f"• Collection timestamp: {analysis_results.get('timestamp', 'Unknown')}",
                ""
            ])
            
            # Footer
            report_lines.extend([
                "=" * 100,
                "END OF REPORT",
                "=" * 100
            ])
            
            return "\n".join(report_lines)
            
        except Exception as e:
            self.logger.error(f"Error generating performance report: {e}")
            return f"Error generating performance report: {str(e)}"

    def _get_node_memory_capacity(self, data: Dict[str, Any]) -> float:
        """Get node memory capacity in GB"""
        try:
            # First, try to get from cluster info if available
            cluster_info = data.get('cluster_info', {})
            if cluster_info and 'nodes' in cluster_info:
                for node in cluster_info['nodes']:
                    if node.get('memory_capacity'):
                        # Convert from bytes/KB/MB to GB
                        capacity_str = node['memory_capacity']
                        if 'Gi' in capacity_str:
                            return float(capacity_str.replace('Gi', ''))
                        elif 'Mi' in capacity_str:
                            return float(capacity_str.replace('Mi', '')) / 1024
            
            # If not available, estimate based on typical OpenShift master node sizes
            # Common configurations: 16GB, 32GB, 64GB for master nodes
            # Use conservative estimate of 16GB for calculation
            return 16.0
            
        except Exception as e:
            self.logger.warning(f"Could not determine node memory capacity, using default 16GB: {e}")
            return 16.0

    """
    Enhanced root cause analysis functions for etcd performance analyzer
    """

    async def _analyze_disk_io_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced disk I/O pattern analysis for root cause identification"""
        analysis = {
            'disk_performance_assessment': {},
            'io_bottleneck_indicators': [],
            'storage_recommendations': [],
            'detailed_findings': {}
        }
        
        try:
            disk_data = data.get('disk_io_data', [])
            
            if not disk_data:
                analysis['detailed_findings']['no_data'] = "No disk I/O metrics available for analysis"
                return analysis
            
            # Analyze write throughput patterns
            write_metrics = [m for m in disk_data if 'write' in m.get('metric_name', '').lower() and 'throughput' in m.get('metric_name', '').lower()]
            if write_metrics:
                total_write_throughput = 0
                node_count = len(write_metrics)
                
                for metric in write_metrics:
                    node_name = metric.get('node_name', 'unknown')
                    avg_bytes_per_sec = metric.get('avg', 0)
                    max_bytes_per_sec = metric.get('max', 0)
                    
                    # Convert to MB/s
                    avg_mb_s = avg_bytes_per_sec / (1024 * 1024)
                    max_mb_s = max_bytes_per_sec / (1024 * 1024)
                    total_write_throughput += avg_mb_s
                    
                    analysis['detailed_findings'][f'write_throughput_{node_name}'] = {
                        'avg_mb_s': round(avg_mb_s, 2),
                        'max_mb_s': round(max_mb_s, 2),
                        'assessment': 'low' if avg_mb_s < 50 else 'adequate' if avg_mb_s < 200 else 'high'
                    }
                    
                    # Check for low throughput (potential bottleneck)
                    if avg_mb_s < 50:  # Less than 50 MB/s indicates potential bottleneck
                        analysis['io_bottleneck_indicators'].append({
                            'type': 'low_write_throughput',
                            'node': node_name,
                            'value': f'{avg_mb_s:.2f} MB/s',
                            'severity': 'high' if avg_mb_s < 20 else 'medium',
                            'description': f'Node {node_name} showing low write throughput ({avg_mb_s:.2f} MB/s)'
                        })
                
                # Cluster-wide assessment
                avg_cluster_throughput = total_write_throughput / node_count if node_count > 0 else 0
                analysis['disk_performance_assessment']['write_throughput'] = {
                    'cluster_avg_mb_s': round(avg_cluster_throughput, 2),
                    'total_nodes': node_count,
                    'performance_grade': (
                        'excellent' if avg_cluster_throughput > 200 else
                        'good' if avg_cluster_throughput > 100 else
                        'poor'
                    )
                }
            
            # Analyze IOPS patterns
            iops_metrics = [m for m in disk_data if 'iops' in m.get('metric_name', '').lower()]
            if iops_metrics:
                for metric in iops_metrics:
                    node_name = metric.get('node_name', 'unknown')
                    avg_iops = metric.get('avg', 0)
                    max_iops = metric.get('max', 0)
                    
                    analysis['detailed_findings'][f'iops_{node_name}'] = {
                        'avg_iops': round(avg_iops, 2),
                        'max_iops': round(max_iops, 2),
                        'assessment': 'low' if avg_iops < 1000 else 'adequate' if avg_iops < 5000 else 'high'
                    }
                    
                    # Check for low IOPS (etcd typically needs 1000+ write IOPS)
                    if avg_iops < 1000:
                        analysis['io_bottleneck_indicators'].append({
                            'type': 'low_iops',
                            'node': node_name,
                            'value': f'{avg_iops:.0f} IOPS',
                            'severity': 'high' if avg_iops < 500 else 'medium',
                            'description': f'Node {node_name} showing low IOPS ({avg_iops:.0f}), etcd requires 1000+ write IOPS'
                        })
            
            # Generate storage recommendations
            if analysis['io_bottleneck_indicators']:
                analysis['storage_recommendations'] = [
                    "Upgrade to high-performance NVMe SSDs with sustained write performance > 100 MB/s",
                    "Ensure dedicated storage for etcd data directory with minimum 3000 IOPS",
                    "Consider enterprise-grade storage with consistent low latency characteristics",
                    "Verify storage is not shared with other I/O intensive workloads",
                    "Check for storage controller bottlenecks or outdated drivers"
                ]
            else:
                analysis['storage_recommendations'] = [
                    "Current storage performance appears adequate for etcd workload",
                    "Continue monitoring for sustained performance under peak loads"
                ]
        
        except Exception as e:
            self.logger.error(f"Error analyzing disk I/O patterns: {e}")
            analysis['error'] = str(e)
        
        return analysis

    async def _analyze_network_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced network pattern analysis for root cause identification"""
        analysis = {
            'network_health_assessment': {},
            'connectivity_issues': [],
            'latency_analysis': {},
            'bandwidth_analysis': {},
            'network_recommendations': []
        }
        
        try:
            network_data = data.get('network_data', {})
            
            if not network_data:
                analysis['network_health_assessment']['status'] = 'no_data'
                analysis['network_recommendations'] = ["No network metrics available for analysis"]
                return analysis
            
            # Analyze peer-to-peer latency
            pod_metrics = network_data.get('pod_metrics', [])
            peer_latency_metrics = [m for m in pod_metrics if 'peer2peer_latency' in m.get('metric_name', '')]
            
            if peer_latency_metrics:
                total_latency = 0
                high_latency_pods = 0
                
                for metric in peer_latency_metrics:
                    pod_name = metric.get('pod_name', 'unknown')
                    avg_latency = metric.get('avg', 0)
                    max_latency = metric.get('max', 0)
                    
                    # Convert to ms if in seconds
                    if avg_latency < 1:  # Assume seconds if < 1
                        avg_latency_ms = avg_latency * 1000
                        max_latency_ms = max_latency * 1000
                    else:
                        avg_latency_ms = avg_latency
                        max_latency_ms = max_latency
                    
                    total_latency += avg_latency_ms
                    
                    analysis['latency_analysis'][pod_name] = {
                        'avg_latency_ms': round(avg_latency_ms, 3),
                        'max_latency_ms': round(max_latency_ms, 3),
                        'status': (
                            'critical' if avg_latency_ms > 100 else
                            'warning' if avg_latency_ms > 50 else
                            'good'
                        )
                    }
                    
                    if avg_latency_ms > 50:  # > 50ms indicates potential network issues
                        high_latency_pods += 1
                        analysis['connectivity_issues'].append({
                            'type': 'high_peer_latency',
                            'pod': pod_name,
                            'avg_latency_ms': round(avg_latency_ms, 3),
                            'max_latency_ms': round(max_latency_ms, 3),
                            'severity': 'critical' if avg_latency_ms > 100 else 'warning',
                            'description': f'Pod {pod_name} experiencing high peer latency ({avg_latency_ms:.1f}ms avg)'
                        })
                
                # Cluster network assessment
                avg_cluster_latency = total_latency / len(peer_latency_metrics) if peer_latency_metrics else 0
                analysis['network_health_assessment'] = {
                    'avg_peer_latency_ms': round(avg_cluster_latency, 3),
                    'total_pods': len(peer_latency_metrics),
                    'high_latency_pods': high_latency_pods,
                    'network_grade': (
                        'excellent' if avg_cluster_latency < 10 else
                        'good' if avg_cluster_latency < 25 else
                        'poor'
                    )
                }
            
            # Analyze network utilization
            node_metrics = network_data.get('node_metrics', [])
            utilization_metrics = [m for m in node_metrics if 'utilization' in m.get('metric_name', '')]
            
            if utilization_metrics:
                for metric in utilization_metrics:
                    node_name = metric.get('node_name', 'unknown')
                    avg_util = metric.get('avg', 0)
                    max_util = metric.get('max', 0)
                    
                    # Convert to percentage if in bits/bytes per second
                    if avg_util > 100:  # Likely bits/bytes per second
                        # Assume 1Gbps network capacity
                        avg_util_percent = (avg_util / 1000000000) * 100
                        max_util_percent = (max_util / 1000000000) * 100
                    else:
                        avg_util_percent = avg_util
                        max_util_percent = max_util
                    
                    analysis['bandwidth_analysis'][node_name] = {
                        'avg_utilization_percent': round(avg_util_percent, 2),
                        'max_utilization_percent': round(max_util_percent, 2),
                        'status': (
                            'critical' if max_util_percent > 85 else
                            'warning' if avg_util_percent > 70 else
                            'good'
                        )
                    }
                    
                    if max_util_percent > 70:
                        analysis['connectivity_issues'].append({
                            'type': 'high_network_utilization',
                            'node': node_name,
                            'max_utilization': f'{max_util_percent:.1f}%',
                            'severity': 'critical' if max_util_percent > 85 else 'warning',
                            'description': f'Node {node_name} showing high network utilization ({max_util_percent:.1f}%)'
                        })
            
            # Generate network recommendations
            if analysis['connectivity_issues']:
                high_latency_issues = [i for i in analysis['connectivity_issues'] if i['type'] == 'high_peer_latency']
                high_util_issues = [i for i in analysis['connectivity_issues'] if i['type'] == 'high_network_utilization']
                
                if high_latency_issues:
                    analysis['network_recommendations'].extend([
                        "Investigate network topology - ensure etcd nodes are on same subnet/VLAN when possible",
                        "Check for network switch configuration issues or suboptimal routing",
                        "Verify network interface driver versions and hardware compatibility",
                        "Consider dedicated network interfaces for etcd cluster communication"
                    ])
                
                if high_util_issues:
                    analysis['network_recommendations'].extend([
                        "Monitor network bandwidth usage and consider upgrading to higher capacity links",
                        "Implement QoS policies to prioritize etcd traffic",
                        "Check for network congestion from other workloads on shared infrastructure"
                    ])
            else:
                analysis['network_recommendations'] = [
                    "Network performance appears healthy for etcd cluster operations",
                    "Continue monitoring network latency and utilization trends"
                ]
        
        except Exception as e:
            self.logger.error(f"Error analyzing network patterns: {e}")
            analysis['error'] = str(e)
        
        return analysis

    async def _analyze_consensus_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced consensus pattern analysis for etcd cluster health"""
        analysis = {
            'consensus_health': {},
            'raft_performance': {},
            'leader_election_stability': {},
            'consensus_recommendations': []
        }
        
        try:
            # Look for etcd-specific consensus metrics
            general_data = data.get('general_info_data', [])
            
            # Analyze proposal patterns (if available)
            proposal_metrics = [m for m in general_data if 'proposal' in m.get('metric_name', '').lower()]
            if proposal_metrics:
                for metric in proposal_metrics:
                    metric_name = metric.get('metric_name', '')
                    pod_name = metric.get('pod_name', 'unknown')
                    avg_value = metric.get('avg', 0)
                    max_value = metric.get('max', 0)
                    
                    if 'pending' in metric_name.lower():
                        analysis['raft_performance'][f'pending_proposals_{pod_name}'] = {
                            'avg_pending': avg_value,
                            'max_pending': max_value,
                            'status': 'critical' if avg_value > 5 else 'warning' if avg_value > 1 else 'good'
                        }
                        
                        if avg_value > 1:
                            analysis['consensus_recommendations'].append(
                                f"Pod {pod_name} has elevated pending proposals ({avg_value:.1f} avg), "
                                "indicating potential consensus delays"
                            )
            
            # Analyze leader election patterns (correlate with high latencies)
            wal_data = data.get('wal_fsync_data', [])
            backend_data = data.get('backend_commit_data', [])
            
            # Check for latency-induced consensus issues
            high_latency_pods = []
            
            for wal_metric in wal_data:
                if 'p99' in wal_metric.get('metric_name', ''):
                    avg_latency = wal_metric.get('avg', 0) * 1000  # Convert to ms
                    if avg_latency > 10:  # Above 10ms threshold
                        high_latency_pods.append({
                            'pod': wal_metric.get('pod_name'),
                            'wal_latency_ms': avg_latency,
                            'issue_type': 'wal_fsync_slow'
                        })
            
            for backend_metric in backend_data:
                if 'p99' in backend_metric.get('metric_name', ''):
                    avg_latency = backend_metric.get('avg', 0) * 1000  # Convert to ms
                    if avg_latency > 25:  # Above 25ms threshold
                        pod_name = backend_metric.get('pod_name')
                        existing_pod = next((p for p in high_latency_pods if p['pod'] == pod_name), None)
                        if existing_pod:
                            existing_pod['backend_latency_ms'] = avg_latency
                            existing_pod['issue_type'] = 'both_slow'
                        else:
                            high_latency_pods.append({
                                'pod': pod_name,
                                'backend_latency_ms': avg_latency,
                                'issue_type': 'backend_commit_slow'
                            })
            
            if high_latency_pods:
                analysis['consensus_health']['affected_pods'] = len(high_latency_pods)
                analysis['consensus_health']['risk_level'] = (
                    'high' if len(high_latency_pods) >= 2 else 'medium'
                )
                
                analysis['leader_election_stability'] = {
                    'potential_instability': True,
                    'reason': f"{len(high_latency_pods)} pods showing high I/O latency that may affect consensus",
                    'affected_pods': [p['pod'] for p in high_latency_pods]
                }
                
                analysis['consensus_recommendations'].extend([
                    "High I/O latency detected on multiple nodes may cause consensus instability",
                    "Consider isolating slow nodes or improving their storage performance",
                    "Monitor for frequent leader elections which indicate consensus issues",
                    "Verify etcd heartbeat and election timeout configurations"
                ])
            else:
                analysis['consensus_health'] = {
                    'status': 'stable',
                    'risk_level': 'low'
                }
                analysis['consensus_recommendations'] = [
                    "No consensus-affecting performance issues detected",
                    "Continue monitoring I/O latency to prevent future consensus problems"
                ]
        
        except Exception as e:
            self.logger.error(f"Error analyzing consensus patterns: {e}")
            analysis['error'] = str(e)
        
        return analysis

    async def _analyze_resource_contention(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced resource contention analysis"""
        analysis = {
            'cpu_contention': {},
            'memory_pressure': {},
            'resource_recommendations': [],
            'contention_indicators': []
        }
        
        try:
            general_data = data.get('general_info_data', [])
            
            # Analyze CPU contention patterns
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            
            if cpu_metrics:
                high_cpu_pods = 0
                critical_cpu_pods = 0
                total_cpu_usage = 0
                
                for metric in cpu_metrics:
                    pod_name = metric.get('pod_name', 'unknown')
                    avg_cpu = metric.get('avg', 0)
                    max_cpu = metric.get('max', 0)
                    
                    total_cpu_usage += avg_cpu
                    
                    analysis['cpu_contention'][pod_name] = {
                        'avg_cpu_percent': avg_cpu,
                        'max_cpu_percent': max_cpu,
                        'contention_level': (
                            'critical' if max_cpu > 85 else
                            'warning' if avg_cpu > 70 else
                            'normal'
                        )
                    }
                    
                    if max_cpu > 85:
                        critical_cpu_pods += 1
                        analysis['contention_indicators'].append({
                            'type': 'critical_cpu_usage',
                            'pod': pod_name,
                            'max_cpu': max_cpu,
                            'description': f'Pod {pod_name} reached {max_cpu:.1f}% CPU usage (critical threshold: 85%)'
                        })
                    elif avg_cpu > 70:
                        high_cpu_pods += 1
                        analysis['contention_indicators'].append({
                            'type': 'high_cpu_usage',
                            'pod': pod_name,
                            'avg_cpu': avg_cpu,
                            'description': f'Pod {pod_name} averaging {avg_cpu:.1f}% CPU usage (warning threshold: 70%)'
                        })
                
                # Cluster CPU assessment
                avg_cluster_cpu = total_cpu_usage / len(cpu_metrics) if cpu_metrics else 0
                analysis['cpu_contention']['cluster_summary'] = {
                    'avg_cpu_percent': round(avg_cluster_cpu, 2),
                    'high_usage_pods': high_cpu_pods,
                    'critical_usage_pods': critical_cpu_pods,
                    'total_pods': len(cpu_metrics)
                }
            
            # Analyze memory patterns
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            
            if memory_metrics:
                for metric in memory_metrics:
                    pod_name = metric.get('pod_name', 'unknown')
                    avg_memory_mb = metric.get('avg', 0)
                    max_memory_mb = metric.get('max', 0)
                    
                    # Convert to GB for readability
                    avg_memory_gb = avg_memory_mb / 1024
                    max_memory_gb = max_memory_mb / 1024
                    
                    analysis['memory_pressure'][pod_name] = {
                        'avg_memory_gb': round(avg_memory_gb, 2),
                        'max_memory_gb': round(max_memory_gb, 2),
                        'pressure_level': (
                            'high' if avg_memory_gb > 4 else
                            'moderate' if avg_memory_gb > 2 else
                            'low'
                        )
                    }
                    
                    if avg_memory_gb > 4:  # More than 4GB average usage
                        analysis['contention_indicators'].append({
                            'type': 'high_memory_usage',
                            'pod': pod_name,
                            'avg_memory_gb': round(avg_memory_gb, 2),
                            'description': f'Pod {pod_name} using {avg_memory_gb:.1f}GB memory (monitor for memory pressure)'
                        })
            
            # Generate resource recommendations
            if analysis['contention_indicators']:
                cpu_issues = [i for i in analysis['contention_indicators'] if 'cpu' in i['type']]
                memory_issues = [i for i in analysis['contention_indicators'] if 'memory' in i['type']]
                
                if cpu_issues:
                    analysis['resource_recommendations'].extend([
                        "Increase CPU limits/requests for etcd pods to prevent CPU starvation",
                        "Consider node affinity to place etcd pods on dedicated or less loaded nodes",
                        "Review other workloads on the same nodes that might be competing for CPU",
                        "Implement CPU pinning or priority classes for etcd workloads"
                    ])
                
                if memory_issues:
                    analysis['resource_recommendations'].extend([
                        "Monitor memory usage trends and increase memory limits if growing",
                        "Consider regular etcd compaction to manage memory usage",
                        "Review etcd configuration for memory-intensive settings"
                    ])
            else:
                analysis['resource_recommendations'] = [
                    "Resource utilization appears within acceptable ranges",
                    "Continue monitoring for resource usage trends over time"
                ]
        
        except Exception as e:
            self.logger.error(f"Error analyzing resource contention: {e}")
            analysis['error'] = str(e)
        
        return analysis

    async def _ai_based_root_cause_analysis(self, failed_thresholds: List[Dict[str, Any]], 
                                        metrics_data: Dict[str, Any], 
                                        script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced AI-based root cause analysis with detailed technical insights"""
        try:
            # Prepare comprehensive context for AI analysis
            context = self._prepare_detailed_cluster_context(metrics_data, script_analysis, failed_thresholds)
            
            # Create enhanced prompt for AI analysis
            prompt = ChatPromptTemplate.from_template("""
            You are a senior etcd performance engineer with 10+ years of experience in distributed systems and database performance tuning.
            
            PERFORMANCE ISSUE SUMMARY:
            {failed_thresholds_summary}
            
            DETAILED TECHNICAL CONTEXT:
            {technical_context}
            
            SCRIPT-BASED ANALYSIS RESULTS:
            {script_analysis_summary}
            
            CLUSTER METRICS OVERVIEW:
            {cluster_metrics_overview}
            
            Based on your expertise, provide a comprehensive root cause analysis following this exact JSON structure:
            
            {{
            "primary_root_cause": {{
                "cause": "Most likely primary cause based on evidence",
                "confidence_level": 8,
                "technical_explanation": "Detailed technical explanation of why this is the primary cause"
            }},
            "secondary_factors": [
                "Contributing factor 1",
                "Contributing factor 2"
            ],
            "evidence_analysis": [
                "Key evidence point 1 supporting the diagnosis",
                "Key evidence point 2 supporting the diagnosis"
            ],
            "technical_recommendations": [
                {{
                "priority": "high",
                "recommendation": "Specific technical action 1",
                "implementation_details": "How to implement this recommendation",
                "expected_impact": "Expected performance improvement"
                }},
                {{
                "priority": "medium", 
                "recommendation": "Specific technical action 2",
                "implementation_details": "Implementation guidance",
                "expected_impact": "Expected improvement"
                }}
            ],
            "performance_impact_assessment": {{
                "current_impact": "Assessment of current performance impact",
                "escalation_risk": "Risk if issues are not addressed",
                "recovery_timeline": "Estimated time to resolve with proper actions"
            }},
            "monitoring_recommendations": [
                "Specific metric to monitor during resolution",
                "Key indicator to track improvement"
            ]
            }}
            
            Focus on the most technically sound diagnosis based on etcd performance best practices:
            - WAL fsync latency >10ms typically indicates storage I/O issues
            - Backend commit latency >25ms suggests database/disk performance problems  
            - High CPU usage may indicate resource starvation or inefficient operations
            - Network latency affects cluster consensus and data replication
            
            Consider the interaction between these factors and provide actionable technical recommendations.
            """)
            
            # Prepare context strings
            failed_thresholds_summary = self._format_failed_thresholds_summary(failed_thresholds)
            technical_context = self._format_technical_context(context)
            script_analysis_summary = self._format_script_analysis_summary(script_analysis)
            cluster_metrics_overview = self._format_cluster_metrics_overview(context)
            
            # Get AI analysis
            chain = prompt | self.llm
            response = await chain.ainvoke({
                'failed_thresholds_summary': failed_thresholds_summary,
                'technical_context': technical_context,
                'script_analysis_summary': script_analysis_summary,
                'cluster_metrics_overview': cluster_metrics_overview
            })
            
            # Parse AI response
            try:
                import re
                import json
                
                # Try to extract JSON from the response
                json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
                if json_match:
                    ai_analysis = json.loads(json_match.group())
                    
                    # Validate required fields and add defaults if missing
                    ai_analysis = self._validate_and_enhance_ai_analysis(ai_analysis, failed_thresholds, script_analysis)
                    
                else:
                    # Fallback to structured parsing of non-JSON response
                    ai_analysis = self._parse_unstructured_ai_response(response.content, failed_thresholds, script_analysis)
                    
            except json.JSONDecodeError:
                # Handle malformed JSON
                ai_analysis = self._parse_unstructured_ai_response(response.content, failed_thresholds, script_analysis)
                
            # Enhance analysis with technical insights
            ai_analysis['technical_insights'] = self._generate_technical_insights(failed_thresholds, script_analysis)
            ai_analysis['root_cause_confidence'] = self._calculate_root_cause_confidence(ai_analysis, script_analysis)
            
            return ai_analysis
            
        except Exception as e:
            self.logger.error(f"Error in AI-based root cause analysis: {e}")
            return {
                'error': str(e),
                'fallback_analysis': self._generate_fallback_analysis(failed_thresholds, script_analysis),
                'technical_insights': self._generate_technical_insights(failed_thresholds, script_analysis)
            }

    def _prepare_detailed_cluster_context(self, metrics_data: Dict[str, Any], 
                                        script_analysis: Dict[str, Any], 
                                        failed_thresholds: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare detailed cluster context for AI analysis"""
        context = {}
        
        try:
            data = metrics_data.get('data', {})
            
            # Enhanced WAL fsync analysis
            wal_data = data.get('wal_fsync_data', [])
            if wal_data:
                wal_latencies = []
                for m in wal_data:
                    if 'p99' in m.get('metric_name', ''):
                        latency_ms = m.get('avg', 0) * 1000
                        wal_latencies.append({
                            'pod': m.get('pod_name'),
                            'latency_ms': latency_ms,
                            'exceeds_threshold': latency_ms > 10
                        })
                context['wal_fsync_details'] = wal_latencies
            
            # Enhanced backend commit analysis  
            backend_data = data.get('backend_commit_data', [])
            if backend_data:
                backend_latencies = []
                for m in backend_data:
                    if 'p99' in m.get('metric_name', ''):
                        latency_ms = m.get('avg', 0) * 1000
                        backend_latencies.append({
                            'pod': m.get('pod_name'),
                            'latency_ms': latency_ms,
                            'exceeds_threshold': latency_ms > 25
                        })
                context['backend_commit_details'] = backend_latencies
            
            # Resource utilization context
            general_data = data.get('general_info_data', [])
            cpu_usage = []
            memory_usage = []
            
            for m in general_data:
                if 'cpu_usage' in m.get('metric_name', ''):
                    cpu_usage.append({
                        'pod': m.get('pod_name'),
                        'avg_percent': m.get('avg', 0),
                        'max_percent': m.get('max', 0)
                    })
                elif 'memory_usage' in m.get('metric_name', ''):
                    memory_usage.append({
                        'pod': m.get('pod_name'),
                        'avg_mb': m.get('avg', 0),
                        'max_mb': m.get('max', 0)
                    })
            
            context['cpu_utilization'] = cpu_usage
            context['memory_utilization'] = memory_usage
            
            # Network performance context
            network_data = data.get('network_data', {})
            if network_data:
                pod_metrics = network_data.get('pod_metrics', [])
                peer_latencies = []
                
                for m in pod_metrics:
                    if 'peer2peer_latency' in m.get('metric_name', ''):
                        latency_ms = m.get('avg', 0) * 1000
                        peer_latencies.append({
                            'pod': m.get('pod_name'),
                            'latency_ms': latency_ms
                        })
                
                context['network_latencies'] = peer_latencies
            
            # Storage I/O context from script analysis
            if script_analysis:
                disk_analysis = script_analysis.get('disk_io_analysis', {})
                context['storage_performance'] = {
                    'write_throughput_analysis': disk_analysis.get('disk_performance_assessment', {}),
                    'io_bottlenecks': disk_analysis.get('io_bottleneck_indicators', []),
                    'storage_grade': disk_analysis.get('disk_performance_assessment', {}).get('write_throughput', {}).get('performance_grade', 'unknown')
                }
            
        except Exception as e:
            self.logger.error(f"Error preparing detailed cluster context: {e}")
            context['error'] = str(e)
        
        return context

    def _format_failed_thresholds_summary(self, failed_thresholds: List[Dict[str, Any]]) -> str:
        """Format failed thresholds for AI analysis"""
        if not failed_thresholds:
            return "No threshold failures detected."
        
        summary = []
        for threshold in failed_thresholds:
            metric = threshold.get('metric', 'unknown')
            current = threshold.get('current_value', 0)
            target = threshold.get('threshold_value', 0)
            severity = threshold.get('severity', 'unknown')
            pods_affected = threshold.get('pods_affected', 0)
            
            summary.append(f"- {metric}: Current {current}, Threshold {target}, Severity {severity}, Pods affected: {pods_affected}")
        
        return "\n".join(summary)

    def _format_technical_context(self, context: Dict[str, Any]) -> str:
        """Format technical context for AI analysis"""
        formatted = []
        
        # WAL fsync details
        wal_details = context.get('wal_fsync_details', [])
        if wal_details:
            formatted.append("WAL Fsync Performance:")
            for detail in wal_details:
                status = "EXCEEDS THRESHOLD" if detail['exceeds_threshold'] else "Within threshold"
                formatted.append(f"  - Pod {detail['pod']}: {detail['latency_ms']:.2f}ms ({status})")
        
        # Backend commit details
        backend_details = context.get('backend_commit_details', [])
        if backend_details:
            formatted.append("Backend Commit Performance:")
            for detail in backend_details:
                status = "EXCEEDS THRESHOLD" if detail['exceeds_threshold'] else "Within threshold"
                formatted.append(f"  - Pod {detail['pod']}: {detail['latency_ms']:.2f}ms ({status})")
        
        # CPU utilization
        cpu_usage = context.get('cpu_utilization', [])
        if cpu_usage:
            formatted.append("CPU Utilization:")
            for cpu in cpu_usage:
                formatted.append(f"  - Pod {cpu['pod']}: {cpu['avg_percent']:.1f}% avg, {cpu['max_percent']:.1f}% max")
        
        # Storage performance
        storage_perf = context.get('storage_performance', {})
        if storage_perf:
            grade = storage_perf.get('storage_grade', 'unknown')
            formatted.append(f"Storage Performance Grade: {grade}")
            
            bottlenecks = storage_perf.get('io_bottlenecks', [])
            if bottlenecks:
                formatted.append("Storage Bottlenecks:")
                for bottleneck in bottlenecks:
                    formatted.append(f"  - {bottleneck.get('description', 'Unknown bottleneck')}")
        
        return "\n".join(formatted) if formatted else "No detailed technical context available."

    def _format_script_analysis_summary(self, script_analysis: Dict[str, Any]) -> str:
        """Format script analysis summary for AI"""
        if not script_analysis:
            return "No script analysis available."
        
        summary = []
        
        # Disk I/O findings
        disk_analysis = script_analysis.get('disk_io_analysis', {})
        if disk_analysis:
            bottlenecks = disk_analysis.get('io_bottleneck_indicators', [])
            if bottlenecks:
                summary.append(f"Disk I/O Analysis: {len(bottlenecks)} bottlenecks detected")
                for bottleneck in bottlenecks[:3]:  # Limit to top 3
                    summary.append(f"  - {bottleneck.get('type', 'unknown')}: {bottleneck.get('description', 'No description')}")
        
        # Network findings
        network_analysis = script_analysis.get('network_analysis', {})
        if network_analysis:
            issues = network_analysis.get('connectivity_issues', [])
            if issues:
                summary.append(f"Network Analysis: {len(issues)} connectivity issues detected")
        
        # Resource contention
        resource_analysis = script_analysis.get('resource_contention_analysis', {})
        if resource_analysis:
            indicators = resource_analysis.get('contention_indicators', [])
            if indicators:
                summary.append(f"Resource Analysis: {len(indicators)} contention indicators detected")
        
        return "\n".join(summary) if summary else "Script analysis completed with no major issues detected."

    def _format_cluster_metrics_overview(self, context: Dict[str, Any]) -> str:
        """Format cluster metrics overview for AI"""
        overview = []
        
        # Calculate cluster averages
        wal_details = context.get('wal_fsync_details', [])
        if wal_details:
            avg_wal_latency = sum(d['latency_ms'] for d in wal_details) / len(wal_details)
            exceeds_count = sum(1 for d in wal_details if d['exceeds_threshold'])
            overview.append(f"WAL Fsync: {avg_wal_latency:.2f}ms avg, {exceeds_count}/{len(wal_details)} pods exceed threshold")
        
        backend_details = context.get('backend_commit_details', [])
        if backend_details:
            avg_backend_latency = sum(d['latency_ms'] for d in backend_details) / len(backend_details)
            exceeds_count = sum(1 for d in backend_details if d['exceeds_threshold'])
            overview.append(f"Backend Commit: {avg_backend_latency:.2f}ms avg, {exceeds_count}/{len(backend_details)} pods exceed threshold")
        
        cpu_usage = context.get('cpu_utilization', [])
        if cpu_usage:
            avg_cpu = sum(c['avg_percent'] for c in cpu_usage) / len(cpu_usage)
            high_cpu_count = sum(1 for c in cpu_usage if c['avg_percent'] > 70)
            overview.append(f"CPU Usage: {avg_cpu:.1f}% avg across cluster, {high_cpu_count} pods >70%")
        
        return "\n".join(overview) if overview else "Limited cluster metrics available."

    def _validate_and_enhance_ai_analysis(self, ai_analysis: Dict[str, Any], 
                                        failed_thresholds: List[Dict[str, Any]], 
                                        script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enhance AI analysis results"""
        # Ensure required fields exist
        if 'primary_root_cause' not in ai_analysis:
            ai_analysis['primary_root_cause'] = {
                'cause': self._infer_primary_cause(failed_thresholds, script_analysis),
                'confidence_level': 7,
                'technical_explanation': 'Inferred from threshold failures and script analysis'
            }
        
        if 'secondary_factors' not in ai_analysis:
            ai_analysis['secondary_factors'] = self._infer_secondary_factors(failed_thresholds, script_analysis)
        
        if 'technical_recommendations' not in ai_analysis:
            ai_analysis['technical_recommendations'] = self._generate_default_recommendations(failed_thresholds)
        
        return ai_analysis

    def _infer_primary_cause(self, failed_thresholds: List[Dict[str, Any]], script_analysis: Dict[str, Any]) -> str:
        """Infer primary root cause from available data"""
        # Check for storage issues first (most common)
        storage_issues = any(t['metric'] in ['wal_fsync_p99', 'backend_commit_p99'] for t in failed_thresholds)
        
        if storage_issues:
            disk_analysis = script_analysis.get('disk_io_analysis', {})
            if disk_analysis.get('io_bottleneck_indicators'):
                return "Disk I/O performance bottleneck - insufficient storage throughput or high latency storage"
            else:
                return "Storage subsystem performance degradation affecting etcd write operations"
        
        # Check for CPU issues
        cpu_issues = any(t['metric'] == 'cpu_usage' for t in failed_thresholds)
        if cpu_issues:
            return "CPU resource starvation limiting etcd performance"
        
        return "Multiple performance factors contributing to threshold failures"

    def _generate_technical_insights(self, failed_thresholds: List[Dict[str, Any]], 
                                script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate technical insights for root cause analysis"""
        insights = {
            'likely_root_causes': [],
            'performance_impact_severity': 'medium',
            'immediate_actions_required': [],
            'long_term_optimizations': []
        }
        
        # Analyze threshold patterns
        latency_failures = [t for t in failed_thresholds if t['threshold_type'] == 'latency']
        cpu_failures = [t for t in failed_thresholds if t['metric'] == 'cpu_usage']
        
        if latency_failures:
            insights['likely_root_causes'].append('Storage I/O bottleneck (primary suspect)')
            insights['immediate_actions_required'].extend([
                'Check storage device performance and IOPS capacity',
                'Verify etcd data directory is on dedicated high-performance storage',
                'Monitor disk utilization during peak etcd operations'
            ])
            
            if len(latency_failures) > 1:
                insights['performance_impact_severity'] = 'high'
                insights['likely_root_causes'].append('Systematic storage performance issues across cluster')
        
        if cpu_failures:
            insights['likely_root_causes'].append('CPU resource contention')
            insights['immediate_actions_required'].extend([
                'Increase CPU limits for etcd pods',
                'Verify node placement and resource allocation',
                'Check for competing workloads on etcd nodes'
            ])
        
        # Long-term optimizations
        insights['long_term_optimizations'] = [
            'Implement dedicated etcd nodes with optimized storage',
            'Establish comprehensive monitoring and alerting',
            'Regular performance benchmarking and capacity planning',
            'Consider etcd cluster topology optimization'
        ]
        
        return insights


def main():
    """Test function for the performance report analyzer"""
    import json
    
    # Sample test data structure
    sample_data = {
        "status": "success",
        "data": {
            "wal_fsync_data": [
                {
                    "metric_name": "disk_wal_fsync_seconds_duration_p99",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 0.015,  # 15ms - above 10ms threshold
                    "max": 0.020,
                    "unit": "seconds"
                }
            ],
            "backend_commit_data": [
                {
                    "metric_name": "disk_backend_commit_duration_seconds_p99",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 0.030,  # 30ms - above 25ms threshold
                    "max": 0.035,
                    "unit": "seconds"
                }
            ],
            "general_info_data": [
                {
                    "metric_name": "etcd_pods_cpu_usage",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 75.5,  # Above 70% warning threshold
                    "max": 89.2,  # Above 85% critical threshold
                    "unit": "percent"
                }
            ]
        },
        "duration": "1h",
        "test_id": "test-123"
    }
    
    # Test the analyzer
    analyzer = etcdReportAnalyzer()
    analysis_results = analyzer.analyze_performance_metrics(sample_data, "test-001")
    report = analyzer.generate_performance_report(analysis_results, "test-001", "1h")
    
    print("Sample Performance Report:")
    print("=" * 80)
    print(report)

if __name__ == "__main__":
    main()