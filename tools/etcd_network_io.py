"""
etcd Network I/O Metrics Collector
Collects and analyzes network I/O metrics for etcd monitoring
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
from .ocp_promql_basequery import PrometheusBaseQuery
from .etcd_tools_utility import mcpToolsUtility
from config.etcd_config import get_config


class NetworkIOCollector:
    """Network I/O metrics collector for etcd monitoring"""
    
    def __init__(self, ocp_auth):
        self.ocp_auth = ocp_auth
        # Derive Prometheus config from OCP auth (server initializes auth first)
        self.prometheus_config = self.ocp_auth.get_prometheus_config()
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        self.config = get_config()
        
        # Cache for node mappings
        self._node_mappings = {}
        self._cache_valid = False
    
    async def collect_all_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all network I/O metrics"""
        try:
            self.logger.info(f"Starting network I/O metrics collection for duration: {duration}")
            
            # Get network_io category metrics from config
            network_metrics = self.config.get_metrics_by_category('network_io')
            if not network_metrics:
                return {
                    'status': 'error',
                    'error': 'No network_io metrics found in configuration'
                }
            
            # Initialize mappings
            await self._update_node_mappings()
            
            results = {
                'status': 'success',
                'collection_time': datetime.now(pytz.UTC).isoformat(),
                'duration': duration,
                'metrics': {},
                'summary': {
                    'total_metrics': len(network_metrics),
                    'collected_metrics': 0,
                    'failed_metrics': 0
                }
            }
            
            async with PrometheusBaseQuery(self.prometheus_config) as prom:
                # Test connection first
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}"
                    }
                
                # Collect each metric
                for metric in network_metrics:
                    metric_name = metric['name']
                    try:
                        self.logger.debug(f"Collecting metric: {metric_name}")
                        
                        # Route to appropriate collection method
                        if metric_name == 'container_network_rx':
                            metric_result = await self._collect_container_network_rx(prom, duration)
                        elif metric_name == 'container_network_tx':
                            metric_result = await self._collect_container_network_tx(prom, duration)
                        elif metric_name == 'network_peer_round_trip_time_p99':
                            metric_result = await self._collect_peer_round_trip_time(prom, duration)
                        elif metric_name == 'network_peer_received_bytes':
                            metric_result = await self._collect_peer_received_bytes(prom, duration)
                        elif metric_name == 'network_peer_sent_bytes':
                            metric_result = await self._collect_peer_sent_bytes(prom, duration)
                        elif metric_name == 'network_client_grpc_received_bytes':
                            metric_result = await self._collect_client_grpc_received(prom, duration)
                        elif metric_name == 'network_client_grpc_sent_bytes':
                            metric_result = await self._collect_client_grpc_sent(prom, duration)
                        elif metric_name == 'snapshot_duration':
                            metric_result = await self._collect_snapshot_duration(prom, duration)
                        elif metric_name == 'node_network_rx_utilization':
                            metric_result = await self._collect_node_network_rx(prom, duration)
                        elif metric_name == 'node_network_tx_utilization':
                            metric_result = await self._collect_node_network_tx(prom, duration)
                        elif metric_name == 'node_network_rx_package':
                            metric_result = await self._collect_node_network_rx_packets(prom, duration)
                        elif metric_name == 'node_network_tx_package':
                            metric_result = await self._collect_node_network_tx_packets(prom, duration)
                        elif metric_name == 'node_network_rx_drop':
                            metric_result = await self._collect_node_network_rx_drops(prom, duration)
                        elif metric_name == 'node_network_tx_drop':
                            metric_result = await self._collect_node_network_tx_drops(prom, duration)
                        elif metric_name == 'grpc_active_watch_streams':
                            metric_result = await self._collect_grpc_watch_streams(prom, duration)
                        elif metric_name == 'grpc_active_lease_streams':
                            metric_result = await self._collect_grpc_lease_streams(prom, duration)
                        else:
                            # Generic collection for any other metrics
                            metric_result = await self._collect_generic_metric(prom, metric, duration)
                        
                        if metric_result['status'] == 'success':
                            results['metrics'][metric_name] = metric_result
                            results['summary']['collected_metrics'] += 1
                        else:
                            results['metrics'][metric_name] = metric_result
                            results['summary']['failed_metrics'] += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                        results['summary']['failed_metrics'] += 1
            
            self.logger.info(f"Network I/O collection completed. Success: {results['summary']['collected_metrics']}, Failed: {results['summary']['failed_metrics']}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in network I/O metrics collection: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'collection_time': datetime.now(pytz.UTC).isoformat()
            }

    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Backward-compatible entrypoint expected by server; wraps collect_all_metrics into {status,data,...}."""
        try:
            result = await self.collect_all_metrics(duration)
            if result.get('status') == 'success':
                return {
                    'status': 'success',
                    'data': {
                        'metrics': result.get('metrics', {}),
                        'summary': result.get('summary', {})
                    },
                    'error': None,
                    'timestamp': result.get('collection_time', datetime.now(pytz.UTC).isoformat()),
                    'category': 'network_io',
                    'duration': duration
                }
            else:
                return {
                    'status': result.get('status', 'error'),
                    'data': None,
                    'error': result.get('error'),
                    'timestamp': result.get('collection_time', datetime.now(pytz.UTC).isoformat()),
                    'category': 'network_io',
                    'duration': duration
                }
        except Exception as e:
            return {
                'status': 'error',
                'data': None,
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'category': 'network_io',
                'duration': duration
            }
    
    async def _update_node_mappings(self):
        """Update node and pod mappings"""
        try:
            self._node_mappings = {
                'pod_to_node': await self.utility.get_pod_to_node_mapping(),
                'node_exporter_to_node': await self.utility.get_node_exporter_to_node_mapping(),
                'master_nodes': await self.utility.get_master_nodes()
            }
            self._cache_valid = True
            self.logger.debug("Updated node mappings cache")
        except Exception as e:
            self.logger.warning(f"Failed to update node mappings: {e}")
            self._cache_valid = False
    
    def _resolve_node_name(self, labels: Dict[str, str]) -> str:
        """Resolve node name from metric labels"""
        # Try different label keys for node identification
        node_keys = ['node', 'instance', 'pod', 'kubernetes_node']
        
        for key in node_keys:
            if key in labels:
                value = labels[key]
                
                # If it's a pod name, resolve to node
                if key == 'pod' and value in self._node_mappings.get('pod_to_node', {}):
                    return self._node_mappings['pod_to_node'][value]
                
                # If it's a node-exporter instance, resolve to node
                if 'node-exporter' in value and value in self._node_mappings.get('node_exporter_to_node', {}):
                    return self._node_mappings['node_exporter_to_node'][value]
                
                # If it contains port info, strip it
                if ':' in value:
                    value = value.split(':')[0]
                
                return value
        
        return 'unknown'
    
    async def _collect_container_network_rx(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect container network receive bytes metrics"""
        query = 'sum(rate(container_network_receive_bytes_total{ namespace=~"openshift-etcd.*"}[2m])) BY (namespace, pod)'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_container_network_tx(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect container network transmit bytes metrics"""
        query = 'sum(rate(container_network_transmit_bytes_total{ namespace=~"openshift-etcd.*"}[2m])) BY (namespace, pod)'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_peer_round_trip_time(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect peer round trip time metrics"""
        query = 'histogram_quantile(0.99, rate(etcd_network_peer_round_trip_time_seconds_bucket{namespace="openshift-etcd",pod=~".*"}[2m]))'
        return await self._collect_pod_metric(prom, query, duration, 'seconds')
    
    async def _collect_peer_received_bytes(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect peer received bytes metrics"""
        query = 'rate(etcd_network_peer_received_bytes_total{namespace="openshift-etcd",pod=~".*"}[2m])'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_peer_sent_bytes(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect peer sent bytes metrics"""
        query = 'rate(etcd_network_peer_sent_bytes_total{namespace="openshift-etcd",pod=~".*"}[2m])'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_client_grpc_received(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect client gRPC received bytes metrics"""
        query = 'rate(etcd_network_client_grpc_received_bytes_total{namespace="openshift-etcd",pod=~".*"}[2m])'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_client_grpc_sent(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect client gRPC sent bytes metrics"""
        query = 'rate(etcd_network_client_grpc_sent_bytes_total{namespace="openshift-etcd",pod=~".*"}[2m])'
        return await self._collect_pod_metric(prom, query, duration, 'bytes_per_second')
    
    async def _collect_snapshot_duration(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect snapshot duration metrics"""
        query = 'sum(rate(etcd_debugging_snap_save_total_duration_seconds_sum{namespace="openshift-etcd"}[2m]))'
        return await self._collect_cluster_metric(prom, query, duration, 'seconds')
    
    async def _collect_node_network_rx(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network receive utilization metrics"""
        query = 'rate(node_network_receive_bytes_total{instance=~".*",device=~".*"}[2m]) * 8'
        return await self._collect_node_metric(prom, query, duration, 'bits_per_second')
    
    async def _collect_node_network_tx(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network transmit utilization metrics"""
        query = 'rate(node_network_transmit_bytes_total{instance=~".*",device=~".*"}[5m]) * 8'
        return await self._collect_node_metric(prom, query, duration, 'bits_per_second')
    
    async def _collect_node_network_rx_packets(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network receive packets metrics"""
        query = 'rate(node_network_receive_packets_total{instance=~".*",device=~".*"}[5m])'
        return await self._collect_node_metric(prom, query, duration, 'packets_per_second')
    
    async def _collect_node_network_tx_packets(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network transmit packets metrics"""
        query = 'rate(node_network_transmit_packets_total{instance=~".*",device=~".*"}[5m])'
        return await self._collect_node_metric(prom, query, duration, 'packets_per_second')
    
    async def _collect_node_network_rx_drops(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network receive drops metrics"""
        query = 'topk(10, rate(node_network_receive_drop_total{instance=~".*"}[5m]))'
        return await self._collect_node_metric(prom, query, duration, 'packets_per_second')
    
    async def _collect_node_network_tx_drops(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect node network transmit drops metrics"""
        query = 'topk(10,rate(node_network_transmit_drop_total{instance=~".*"}[5m]))'
        return await self._collect_node_metric(prom, query, duration, 'packets_per_second')
    
    async def _collect_grpc_watch_streams(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect active gRPC watch streams metrics"""
        query = 'sum(grpc_server_started_total{namespace="openshift-etcd",grpc_service="etcdserverpb.Watch",grpc_type="bidi_stream"}) - sum(grpc_server_handled_total{namespace="openshift-etcd",grpc_service="etcdserverpb.Watch",grpc_type="bidi_stream"})'
        return await self._collect_cluster_metric(prom, query, duration, 'count')
    
    async def _collect_grpc_lease_streams(self, prom: PrometheusBaseQuery, duration: str) -> Dict[str, Any]:
        """Collect active gRPC lease streams metrics"""
        query = 'sum(grpc_server_started_total{namespace="openshift-etcd",grpc_service="etcdserverpb.Lease",grpc_type="bidi_stream"}) - sum(grpc_server_handled_total{namespace="openshift-etcd",grpc_service="etcdserverpb.Lease",grpc_type="bidi_stream"})'
        return await self._collect_cluster_metric(prom, query, duration, 'count')
    
    async def _collect_pod_metric(self, prom: PrometheusBaseQuery, query: str, duration: str, unit: str) -> Dict[str, Any]:
        """Collect metric with pod-level granularity"""
        try:
            result = await prom.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            pod_stats = {}
            node_stats = {}
            
            # Process each series
            for series in result.get('series_data', []):
                labels = series['labels']
                stats = series['statistics']
                
                # Get pod name
                pod_name = labels.get('pod', 'unknown')
                
                # Resolve node name
                node_name = self._resolve_node_name(labels)
                
                # Store pod-level stats
                pod_stats[pod_name] = {
                    'avg': stats.get('avg'),
                    'max': stats.get('max'),
                    'node': node_name,
                    'unit': unit
                }
                
                # Aggregate node-level stats
                if node_name not in node_stats:
                    node_stats[node_name] = {
                        'values': [],
                        'pod_count': 0
                    }
                
                if stats.get('avg') is not None:
                    node_stats[node_name]['values'].append(stats['avg'])
                node_stats[node_name]['pod_count'] += 1
            
            # Calculate node aggregates
            for node_name in node_stats:
                values = node_stats[node_name]['values']
                if values:
                    node_stats[node_name] = {
                        'avg': sum(values) / len(values),
                        'max': max(values),
                        'pod_count': node_stats[node_name]['pod_count'],
                        'unit': unit
                    }
                else:
                    node_stats[node_name] = {
                        'avg': None,
                        'max': None,
                        'pod_count': node_stats[node_name]['pod_count'],
                        'unit': unit
                    }
            
            return {
                'status': 'success',
                'query': query,
                'unit': unit,
                'overall_stats': result.get('overall_statistics', {}),
                'by_pod': pod_stats,
                'by_node': node_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting pod metric: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'query': query
            }
    
    async def _collect_node_metric(self, prom: PrometheusBaseQuery, query: str, duration: str, unit: str) -> Dict[str, Any]:
        """Collect metric with node-level granularity"""
        try:
            result = await prom.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            node_stats = {}
            
            # Process each series
            for series in result.get('series_data', []):
                labels = series['labels']
                stats = series['statistics']
                
                # Resolve node name
                node_name = self._resolve_node_name(labels)
                device = labels.get('device', 'unknown')
                
                # Create unique key for node+device
                key = f"{node_name}_{device}" if device != 'unknown' else node_name
                
                node_stats[key] = {
                    'node': node_name,
                    'device': device,
                    'avg': stats.get('avg'),
                    'max': stats.get('max'),
                    'unit': unit
                }
            
            # Aggregate by node (sum across devices)
            node_aggregates = {}
            for key, stats in node_stats.items():
                node = stats['node']
                if node not in node_aggregates:
                    node_aggregates[node] = {
                        'avg_values': [],
                        'max_values': [],
                        'devices': []
                    }
                
                if stats['avg'] is not None:
                    node_aggregates[node]['avg_values'].append(stats['avg'])
                if stats['max'] is not None:
                    node_aggregates[node]['max_values'].append(stats['max'])
                node_aggregates[node]['devices'].append(stats['device'])
            
            # Calculate final aggregates
            for node in node_aggregates:
                avg_values = node_aggregates[node]['avg_values']
                max_values = node_aggregates[node]['max_values']
                
                node_aggregates[node] = {
                    'avg': sum(avg_values) if avg_values else None,
                    'max': max(max_values) if max_values else None,
                    'device_count': len(node_aggregates[node]['devices']),
                    'unit': unit
                }
            
            return {
                'status': 'success',
                'query': query,
                'unit': unit,
                'overall_stats': result.get('overall_statistics', {}),
                'by_device': node_stats,
                'by_node': node_aggregates
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node metric: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'query': query
            }
    
    async def _collect_cluster_metric(self, prom: PrometheusBaseQuery, query: str, duration: str, unit: str) -> Dict[str, Any]:
        """Collect cluster-level metrics (like gRPC streams)"""
        try:
            result = await prom.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            overall_stats = result.get('overall_statistics', {})
            
            return {
                'status': 'success',
                'query': query,
                'unit': unit,
                'cluster_stats': {
                    'avg': overall_stats.get('avg'),
                    'max': overall_stats.get('max'),
                    'latest': overall_stats.get('latest'),
                    'unit': unit
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cluster metric: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'query': query
            }
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any], duration: str) -> Dict[str, Any]:
        """Generic metric collection for any configured metric"""
        try:
            query = metric_config['expr']
            unit = metric_config.get('unit', 'unknown')
            
            result = await prom.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            return {
                'status': 'success',
                'query': query,
                'unit': unit,
                'overall_stats': result.get('overall_statistics', {}),
                'series_count': result.get('series_count', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting generic metric: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'query': metric_config.get('expr', 'unknown')
            }
    
    async def get_network_summary(self, duration: str = "1h") -> Dict[str, Any]:
        """Get network I/O summary across all metrics"""
        try:
            full_results = await self.collect_all_metrics(duration)
            
            if full_results['status'] != 'success':
                return full_results
            
            summary = {
                'status': 'success',
                'collection_time': full_results['collection_time'],
                'duration': duration,
                'network_health': 'healthy',
                'key_metrics': {},
                'node_summary': {},
                'alerts': []
            }
            
            # Extract key metrics
            metrics = full_results.get('metrics', {})
            
            # Container network metrics
            if 'container_network_rx' in metrics and metrics['container_network_rx']['status'] == 'success':
                summary['key_metrics']['container_rx_bytes_per_sec'] = metrics['container_network_rx']['overall_stats']
            
            if 'container_network_tx' in metrics and metrics['container_network_tx']['status'] == 'success':
                summary['key_metrics']['container_tx_bytes_per_sec'] = metrics['container_network_tx']['overall_stats']
            
            # Peer metrics
            if 'network_peer_round_trip_time_p99' in metrics and metrics['network_peer_round_trip_time_p99']['status'] == 'success':
                rtt_stats = metrics['network_peer_round_trip_time_p99']['overall_stats']
                summary['key_metrics']['peer_rtt_p99_seconds'] = rtt_stats
                
                # Alert on high RTT
                if rtt_stats.get('avg', 0) > 0.1:  # 100ms threshold
                    summary['alerts'].append({
                        'level': 'warning',
                        'metric': 'peer_round_trip_time',
                        'message': f'High peer RTT detected: {rtt_stats["avg"]:.3f}s avg'
                    })
            
            # gRPC streams
            if 'grpc_active_watch_streams' in metrics and metrics['grpc_active_watch_streams']['status'] == 'success':
                summary['key_metrics']['active_watch_streams'] = metrics['grpc_active_watch_streams']['cluster_stats']
            
            if 'grpc_active_lease_streams' in metrics and metrics['grpc_active_lease_streams']['status'] == 'success':
                summary['key_metrics']['active_lease_streams'] = metrics['grpc_active_lease_streams']['cluster_stats']
            
            # Set health status based on alerts
            if summary['alerts']:
                summary['network_health'] = 'degraded' if any(a['level'] == 'warning' for a in summary['alerts']) else 'critical'
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating network summary: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }