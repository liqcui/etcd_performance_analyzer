"""
etcd Disk WAL Fsync Collector Module
Collects Write-Ahead Log fsync performance metrics for etcd monitoring
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz

from config.etcd_config import get_config
from ocauth.ocp_auth import OCPAuth
from tools.ocp_promql_basequery import PrometheusBaseQuery
from tools.etcd_tools_utility import mcpToolsUtility


class DiskWALFsyncCollector:
    """Collector for etcd WAL fsync performance metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h"):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Get WAL fsync metrics from config
        self.wal_fsync_metrics = self.config.get_metrics_by_category("disk_wal_fsync")
        
        if not self.wal_fsync_metrics:
            self.logger.warning("No disk_wal_fsync metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all WAL fsync metrics and return comprehensive results"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                # Test connection first
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(pytz.UTC).isoformat()
                    }
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': self.duration,
                    'category': 'disk_wal_fsync',
                    'metrics': {}
                }
                
                # Process each metric
                for metric_config in self.wal_fsync_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        if metric_name == "disk_wal_fsync_seconds_duration_p99":
                            metric_result = await self.get_wal_fsync_p99_duration(prom)
                        elif metric_name == "wal_fsync_duration_seconds_sum_rate":
                            metric_result = await self.get_wal_fsync_duration_sum_rate(prom)
                        elif metric_name == "wal_fsync_duration_sum":
                            metric_result = await self.get_wal_fsync_duration_sum(prom)
                        elif metric_name == "wal_fsync_duration_seconds_count_rate":
                            metric_result = await self.get_wal_fsync_duration_count_rate(prom)
                        elif metric_name == "wal_fsync_duration_seconds_count":
                            metric_result = await self.get_wal_fsync_duration_count(prom)
                        else:
                            self.logger.warning(f"Unknown metric: {metric_name}")
                            continue
                        
                        results['metrics'][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Add summary
                successful_metrics = sum(1 for m in results['metrics'].values() if m.get('status') == 'success')
                total_metrics = len(results['metrics'])
                
                results['summary'] = {
                    'total_metrics': total_metrics,
                    'successful_metrics': successful_metrics,
                    'failed_metrics': total_metrics - successful_metrics
                }
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    async def get_wal_fsync_p99_duration(self, prom: PrometheusBaseQuery) -> Dict[str, Any]:
        """Get WAL fsync 99th percentile duration metrics"""
        metric_config = self.config.get_metric_by_name("disk_wal_fsync_seconds_duration_p99")
        if not metric_config:
            return {'status': 'error', 'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        try:
            result = await prom.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    pod_stats[pod_name] = {
                        'avg_seconds': round(stats.get('avg', 0), 6),
                        'max_seconds': round(stats.get('max', 0), 6),
                        'min_seconds': round(stats.get('min', 0), 6),
                        'latest_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                        'data_points': stats.get('count', 0)
                    }
            
            # Get node mapping for pods
            node_mapping = await self.utility.get_pod_to_node_mapping()
            
            return {
                'status': 'success',
                'metric': 'disk_wal_fsync_seconds_duration_p99',
                'title': metric_config.get('title', 'Disk WAL Sync Duration'),
                'unit': 'seconds',
                'description': '99th percentile WAL fsync duration per etcd pod',
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error getting WAL fsync p99 duration: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def get_wal_fsync_duration_sum_rate(self, prom: PrometheusBaseQuery) -> Dict[str, Any]:
        """Get WAL fsync duration sum rate metrics"""
        metric_config = self.config.get_metric_by_name("wal_fsync_duration_seconds_sum_rate")
        if not metric_config:
            return {'status': 'error', 'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        try:
            result = await prom.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    pod_stats[pod_name] = {
                        'avg_rate_seconds': round(stats.get('avg', 0), 6),
                        'max_rate_seconds': round(stats.get('max', 0), 6),
                        'min_rate_seconds': round(stats.get('min', 0), 6),
                        'latest_rate_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                        'data_points': stats.get('count', 0)
                    }
            
            # Get node mapping for pods
            node_mapping = await self.utility.get_pod_to_node_mapping()
            
            return {
                'status': 'success',
                'metric': 'wal_fsync_duration_seconds_sum_rate',
                'title': metric_config.get('title', 'WAL fsync Duration sum - rate'),
                'unit': 'seconds/sec',
                'description': 'Rate of WAL fsync duration sum per etcd pod',
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error getting WAL fsync duration sum rate: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def get_wal_fsync_duration_sum(self, prom: PrometheusBaseQuery) -> Dict[str, Any]:
        """Get WAL fsync duration sum metrics"""
        metric_config = self.config.get_metric_by_name("wal_fsync_duration_sum")
        if not metric_config:
            return {'status': 'error', 'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        try:
            result = await prom.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    pod_stats[pod_name] = {
                        'avg_sum_seconds': round(stats.get('avg', 0), 6),
                        'max_sum_seconds': round(stats.get('max', 0), 6),
                        'min_sum_seconds': round(stats.get('min', 0), 6),
                        'latest_sum_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                        'data_points': stats.get('count', 0)
                    }
            
            # Get node mapping for pods
            node_mapping = await self.utility.get_pod_to_node_mapping()
            
            return {
                'status': 'success',
                'metric': 'wal_fsync_duration_sum',
                'title': metric_config.get('title', 'WAL fsync Duration sum'),
                'unit': 'seconds',
                'description': 'Cumulative WAL fsync duration per etcd pod',
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error getting WAL fsync duration sum: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def get_wal_fsync_duration_count_rate(self, prom: PrometheusBaseQuery) -> Dict[str, Any]:
        """Get WAL fsync duration count rate metrics"""
        metric_config = self.config.get_metric_by_name("wal_fsync_duration_seconds_count_rate")
        if not metric_config:
            return {'status': 'error', 'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        try:
            result = await prom.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    pod_stats[pod_name] = {
                        'avg_ops_per_sec': round(stats.get('avg', 0), 3),
                        'max_ops_per_sec': round(stats.get('max', 0), 3),
                        'min_ops_per_sec': round(stats.get('min', 0), 3),
                        'latest_ops_per_sec': round(stats.get('latest', 0), 3) if stats.get('latest') is not None else None,
                        'data_points': stats.get('count', 0)
                    }
            
            # Get node mapping for pods
            node_mapping = await self.utility.get_pod_to_node_mapping()
            
            return {
                'status': 'success',
                'metric': 'wal_fsync_duration_seconds_count_rate',
                'title': metric_config.get('title', 'WAL fsync Duration count - rate'),
                'unit': 'operations/sec',
                'description': 'Rate of WAL fsync operations per etcd pod',
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error getting WAL fsync duration count rate: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def get_wal_fsync_duration_count(self, prom: PrometheusBaseQuery) -> Dict[str, Any]:
        """Get WAL fsync duration count metrics"""
        metric_config = self.config.get_metric_by_name("wal_fsync_duration_seconds_count")
        if not metric_config:
            return {'status': 'error', 'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        try:
            result = await prom.query_with_stats(query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    pod_stats[pod_name] = {
                        'avg_count': round(stats.get('avg', 0), 0),
                        'max_count': round(stats.get('max', 0), 0),
                        'min_count': round(stats.get('min', 0), 0),
                        'latest_count': round(stats.get('latest', 0), 0) if stats.get('latest') is not None else None,
                        'data_points': stats.get('count', 0)
                    }
            
            # Get node mapping for pods
            node_mapping = await self.utility.get_pod_to_node_mapping()
            
            return {
                'status': 'success',
                'metric': 'wal_fsync_duration_seconds_count',
                'title': metric_config.get('title', 'WAL fsync Duration count'),
                'unit': 'count',
                'description': 'Total count of WAL fsync operations per etcd pod',
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error getting WAL fsync duration count: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster-wide WAL fsync performance summary"""
        try:
            full_results = await self.collect_all_metrics()
            
            if full_results['status'] != 'success':
                return full_results
            
            # Extract cluster-wide insights
            summary = {
                'status': 'success',
                'timestamp': full_results['timestamp'],
                'cluster_health': {
                    'total_etcd_pods': 0,
                    'pods_with_data': 0,
                    'wal_fsync_performance': 'unknown'
                },
                'performance_indicators': {}
            }
            
            # Analyze p99 latency
            p99_metric = full_results['metrics'].get('disk_wal_fsync_seconds_duration_p99', {})
            if p99_metric.get('status') == 'success':
                pod_metrics = p99_metric.get('pod_metrics', {})
                summary['cluster_health']['total_etcd_pods'] = len(pod_metrics)
                summary['cluster_health']['pods_with_data'] = len([p for p in pod_metrics.values() if p['data_points'] > 0])
                
                # Check if any pod has p99 latency > 100ms (performance concern)
                high_latency_pods = [pod for pod, stats in pod_metrics.items() 
                                   if stats['max_seconds'] > 0.1]
                
                if not high_latency_pods:
                    summary['cluster_health']['wal_fsync_performance'] = 'good'
                elif len(high_latency_pods) == 1:
                    summary['cluster_health']['wal_fsync_performance'] = 'warning'
                else:
                    summary['cluster_health']['wal_fsync_performance'] = 'critical'
                
                summary['performance_indicators']['high_latency_pods'] = high_latency_pods
                summary['performance_indicators']['max_p99_latency_seconds'] = max(
                    [stats['max_seconds'] for stats in pod_metrics.values()], default=0
                )
                summary['performance_indicators']['avg_p99_latency_seconds'] = sum(
                    [stats['avg_seconds'] for stats in pod_metrics.values()]
                ) / len(pod_metrics) if pod_metrics else 0
            
            # Analyze operation rates
            rate_metric = full_results['metrics'].get('wal_fsync_duration_seconds_count_rate', {})
            if rate_metric.get('status') == 'success':
                pod_metrics = rate_metric.get('pod_metrics', {})
                summary['performance_indicators']['total_ops_per_sec'] = sum(
                    [stats['avg_ops_per_sec'] for stats in pod_metrics.values()]
                )
                summary['performance_indicators']['max_ops_per_sec_single_pod'] = max(
                    [stats['max_ops_per_sec'] for stats in pod_metrics.values()], default=0
                )
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating cluster summary: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }


# Convenience functions for individual metric collection
async def collect_wal_fsync_metrics(ocp_auth: OCPAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to collect all WAL fsync metrics"""
    collector = DiskWALFsyncCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()


async def get_wal_fsync_cluster_summary(ocp_auth: OCPAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to get WAL fsync cluster summary"""
    collector = DiskWALFsyncCollector(ocp_auth, duration)
    return await collector.get_cluster_summary()


# Example usage and testing
async def main():
    """Example usage of the DiskWALFsyncCollector"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize OCP authentication
    ocp_auth = OCPAuth()
    
    try:
        # Initialize connection
        if not await ocp_auth.initialize():
            print("Failed to initialize OCP authentication")
            return
        
        # Create collector
        collector = DiskWALFsyncCollector(ocp_auth, "30m")
        
        # Collect all metrics
        print("Collecting all WAL fsync metrics...")
        results = await collector.collect_all_metrics()
        
        print("\n=== WAL Fsync Metrics Results ===")
        print(f"Status: {results['status']}")
        print(f"Total metrics: {results.get('summary', {}).get('total_metrics', 0)}")
        print(f"Successful metrics: {results.get('summary', {}).get('successful_metrics', 0)}")
        
        # Show summary for each metric
        for metric_name, metric_data in results.get('metrics', {}).items():
            if metric_data.get('status') == 'success':
                print(f"\n{metric_name}:")
                print(f"  Total pods: {metric_data.get('total_pods', 0)}")
                pod_metrics = metric_data.get('pod_metrics', {})
                for pod, stats in list(pod_metrics.items())[:3]:  # Show first 3 pods
                    print(f"  {pod}: avg={list(stats.values())[0]}")
        
        # Get cluster summary
        print("\n=== Cluster Summary ===")
        summary = await collector.get_cluster_summary()
        if summary['status'] == 'success':
            health = summary['cluster_health']
            indicators = summary['performance_indicators']
            print(f"WAL fsync performance: {health['wal_fsync_performance']}")
            print(f"Total etcd pods: {health['total_etcd_pods']}")
            print(f"Max P99 latency: {indicators.get('max_p99_latency_seconds', 0):.6f}s")
            print(f"Avg P99 latency: {indicators.get('avg_p99_latency_seconds', 0):.6f}s")
        
    except Exception as e:
        print(f"Error in main: {e}")


if __name__ == "__main__":
    asyncio.run(main())