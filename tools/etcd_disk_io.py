"""
etcd Disk I/O Metrics Collector Module
Collects and processes disk I/O performance metrics for etcd monitoring
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import pytz

from .ocp_promql_basequery import PrometheusBaseQuery
from .etcd_tools_utility import mcpToolsUtility
from config.etcd_config import get_config


class DiskIOCollector:
    """Collector for etcd disk I/O performance metrics"""
    
    def __init__(self, ocp_auth, duration: str = "1h"):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.utility = mcpToolsUtility(ocp_auth)
        self.timezone = pytz.UTC
        
        # Get disk I/O metrics from configuration
        self.metrics = self.config.get_metrics_by_category('disk_io')
        if not self.metrics:
            self.logger.warning("No disk_io metrics found in configuration")
            self.metrics = []
        
        self.logger.info(f"Initialized DiskIOCollector with {len(self.metrics)} metrics")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all disk I/O metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom_client:
                # Test connection
                connection_test = await prom_client.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Collect all metrics
                # Map config metric names to collector method names
                name_normalization = {
                    'disk_io_container_disk_writes': 'container_disk_writes',
                    'disk_io_node_disk_throughput_read': 'node_disk_throughput_read',
                    'disk_io_node_disk_throughput_write': 'node_disk_throughput_write',
                    'disk_io_node_disk_iops_read': 'node_disk_iops_read',
                    'disk_io_node_disk_iops_write': 'node_disk_iops_write',
                }

                results = {}
                for metric_config in self.metrics:
                    metric_name = metric_config['name']
                    normalized_name = name_normalization.get(metric_name, metric_name)
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        if normalized_name == 'container_disk_writes':
                            results[metric_name] = await self.collect_container_disk_writes(prom_client)
                        elif normalized_name == 'node_disk_throughput_read':
                            results[metric_name] = await self.collect_node_disk_throughput_read(prom_client)
                        elif normalized_name == 'node_disk_throughput_write':
                            results[metric_name] = await self.collect_node_disk_throughput_write(prom_client)
                        elif normalized_name == 'node_disk_iops_read':
                            results[metric_name] = await self.collect_node_disk_iops_read(prom_client)
                        elif normalized_name == 'node_disk_iops_write':
                            results[metric_name] = await self.collect_node_disk_iops_write(prom_client)
                        else:
                            self.logger.warning(f"Unknown metric: {metric_name}")
                            results[metric_name] = {
                                'status': 'error',
                                'error': f'Unknown metric: {metric_name}'
                            }
                            
                    except Exception as e:
                        self.logger.error(f"Error collecting {metric_name}: {e}")
                        results[metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': self.duration,
                    'metrics': results,
                    'total_metrics': len(results)
                }
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def collect_container_disk_writes(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect etcd container disk write metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_container_disk_writes')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await prom_client.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self.utility.get_master_nodes()
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', '')
                
                # Map pod to node
                node_name = 'unknown'
                if pod_name:
                    node_name = await self.utility.get_node_for_pod(pod_name, 'openshift-etcd')
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'series_count': 0
                        }
                    
                    if stats.get('avg') is not None:
                        # Accumulate averages (will divide by series count later)
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'], 
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] / 
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Container Disk Writes'),
                'unit': metric_config.get('unit', 'bytes_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting container_disk_writes: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_node_disk_throughput_read(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect node disk read throughput metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_node_disk_throughput_read')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await prom_client.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self.utility.get_master_nodes()
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                instance = labels.get('instance', '')
                
                # Resolve instance to node name
                node_name = await self.utility.resolve_node_from_instance(instance)
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    device = labels.get('device', 'unknown')
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'devices': [],
                            'series_count': 0
                        }
                    
                    if device not in node_results[node_name]['devices']:
                        node_results[node_name]['devices'].append(device)
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'],
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] /
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Node Disk Read Throughput'),
                'unit': metric_config.get('unit', 'bytes_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node_disk_throughput_read: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_node_disk_throughput_write(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect node disk write throughput metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_node_disk_throughput_write')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await prom_client.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self.utility.get_master_nodes()
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                instance = labels.get('instance', '')
                
                # Resolve instance to node name
                node_name = await self.utility.resolve_node_from_instance(instance)
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    device = labels.get('device', 'unknown')
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'devices': [],
                            'series_count': 0
                        }
                    
                    if device not in node_results[node_name]['devices']:
                        node_results[node_name]['devices'].append(device)
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'],
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] /
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Node Disk Write Throughput'),
                'unit': metric_config.get('unit', 'bytes_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node_disk_throughput_write: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_node_disk_iops_read(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect node disk read IOPS metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_node_disk_iops_read')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await prom_client.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self.utility.get_master_nodes()
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                instance = labels.get('instance', '')
                
                # Resolve instance to node name
                node_name = await self.utility.resolve_node_from_instance(instance)
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    device = labels.get('device', 'unknown')
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'devices': [],
                            'series_count': 0
                        }
                    
                    if device not in node_results[node_name]['devices']:
                        node_results[node_name]['devices'].append(device)
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'],
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] /
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Node Disk Read IOPS'),
                'unit': metric_config.get('unit', 'operations_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node_disk_iops_read: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_node_disk_iops_write(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect node disk write IOPS metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_node_disk_iops_write')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await prom_client.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self.utility.get_master_nodes()
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                instance = labels.get('instance', '')
                
                # Resolve instance to node name
                node_name = await self.utility.resolve_node_from_instance(instance)
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    device = labels.get('device', 'unknown')
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'devices': [],
                            'series_count': 0
                        }
                    
                    if device not in node_results[node_name]['devices']:
                        node_results[node_name]['devices'].append(device)
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'],
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] /
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Node Disk Write IOPS'),
                'unit': metric_config.get('unit', 'operations_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node_disk_iops_write: {e}")
            return {'status': 'error', 'error': str(e)}


# Convenience function for external usage
async def collect_disk_io_metrics(ocp_auth, duration: str = "1h") -> Dict[str, Any]:
    """Collect all disk I/O metrics"""
    collector = DiskIOCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()