"""
etcd General Info Collector
Collects general etcd cluster information and health metrics with node/pod-level details
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime
import pytz
from .ocp_promql_basequery import PrometheusBaseQuery
from config.etcd_config import get_config
from .etcd_tools_utility import mcpToolsUtility


class GeneralInfoCollector:
    """Collector for general etcd cluster information"""
    
    def __init__(self, ocp_auth):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.timezone = pytz.UTC
        self.tools_utility = mcpToolsUtility(ocp_auth)
    
    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all general info metrics with node/pod level details"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                general_metrics = self.config.get_metrics_by_category('general_info')
                
                collected_data = {
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'category': 'general_info',
                    'pod_metrics': {}
                }
                
                # Collect each metric with pod-level breakdown
                for metric in general_metrics:
                    metric_name = metric['name']
                    metric_query = metric['expr']
                    
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        result = await prom.query_with_stats(metric_query, duration)
                        
                        # Process result to extract pod level data
                        pod_metrics = await self._process_metric_for_pods(result, metric_name)
                        
                        collected_data['pod_metrics'][metric_name] = {
                            'title': metric.get('title', metric_name),
                            'unit': metric.get('unit', 'unknown'),
                            'query': metric_query,
                            'pods': pod_metrics
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        collected_data['pod_metrics'][metric_name] = {
                            'title': metric.get('title', metric_name),
                            'unit': metric.get('unit', 'unknown'),
                            'query': metric_query,
                            'error': str(e)
                        }
                
                return {
                    'status': 'success',
                    'data': collected_data
                }
                
        except Exception as e:
            self.logger.error(f"Error collecting general info metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_cpu_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get CPU usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('cpu_usage', duration)
    
    async def get_memory_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get memory usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('memory_usage', duration)
    
    async def get_db_space_used_percent_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database space usage percentage by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('db_space_used_percent', duration)
    
    async def get_db_physical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database physical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('db_physical_size', duration)
    
    async def get_db_logical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database logical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('db_logical_size', duration)
    
    async def get_proposal_failure_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal failure rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_failure_rate', duration)
    
    async def get_proposal_pending_total_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get pending proposals total by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_pending_total', duration)
    
    async def get_proposal_commit_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal commit rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_commit_rate', duration)
    
    async def get_proposal_apply_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal apply rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_apply_rate', duration)
    
    async def get_total_proposals_committed_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get total proposals committed by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('total_proposals_committed', duration)
    
    async def get_leader_changes_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get leader changes rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('leader_changes_rate', duration)
    
    async def get_etcd_has_leader_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get etcd has leader status by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_has_leader', duration)
    
    async def get_leader_elections_per_day_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get leader elections per day by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('leader_elections_per_day', duration)
    
    async def get_slow_applies_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get slow applies by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('slow_applies', duration)
    
    async def get_slow_read_indexes_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get slow read indexes by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('slow_read_indexes', duration)
    
    async def get_put_operations_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get PUT operations rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('put_operations_rate', duration)
    
    async def get_delete_operations_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get DELETE operations rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('delete_operations_rate', duration)
    
    async def get_heartbeat_send_failures_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get heartbeat send failures by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('heartbeat_send_failures', duration)
    
    async def get_health_failures_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get health failures by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('health_failures', duration)
    
    async def get_total_keys_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get total keys by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('total_keys', duration)
    
    async def get_compacted_keys_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get compacted keys by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('compacted_keys', duration)
    
    async def get_all_metrics_grouped_by_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get all general info metrics grouped by etcd pod name with avg and max values"""
        try:
            general_metrics = self.config.get_metrics_by_category('general_info')
            pod_grouped_data = {}
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Collect all metrics
                for metric in general_metrics:
                    metric_name = metric['name']
                    metric_query = metric['expr']
                    
                    try:
                        result = await prom.query_with_stats(metric_query, duration)
                        pod_metrics = await self._process_metric_for_pods(result, metric_name)
                        
                        # Group by pod name
                        for pod_name, pod_data in pod_metrics.items():
                            if pod_name not in pod_grouped_data:
                                pod_grouped_data[pod_name] = {}
                            
                            pod_grouped_data[pod_name][metric_name] = {
                                'avg': pod_data.get('avg', 0),
                                'max': pod_data.get('max', 0),
                                'unit': metric.get('unit', 'unknown')
                            }
                            
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        continue
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'timezone': 'UTC',
                    'pods': pod_grouped_data
                }
                
        except Exception as e:
            self.logger.error(f"Error getting all metrics grouped by pod: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_key_metrics_summary(self, duration: str = "1h") -> Dict[str, Any]:
        """Get summary of key etcd metrics with avg and max per pod"""
        key_metrics = [
            'cpu_usage',
            'memory_usage', 
            'db_space_used_percent',
            'proposal_failure_rate',
            'etcd_has_leader',
            'slow_applies',
            'total_keys'
        ]
        
        try:
            summary_data = {}
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                for metric_name in key_metrics:
                    try:
                        metric_config = self.config.get_metric_by_name(metric_name)
                        if not metric_config:
                            continue
                        
                        result = await prom.query_with_stats(metric_config['expr'], duration)
                        pod_metrics = await self._process_metric_for_pods(result, metric_name)
                        
                        summary_data[metric_name] = {
                            'title': metric_config.get('title', metric_name),
                            'unit': metric_config.get('unit', 'unknown'),
                            'pods': pod_metrics
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting key metric {metric_name}: {e}")
                        continue
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'timezone': 'UTC',
                    'key_metrics': summary_data
                }
                
        except Exception as e:
            self.logger.error(f"Error getting key metrics summary: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _get_single_metric_per_pod(self, metric_name: str, duration: str) -> Dict[str, Any]:
        """Get a single metric with avg and max values per etcd pod"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                result = await prom.query_with_stats(metric_config['expr'], duration)
                
                # Process result for pod breakdown
                pod_metrics = await self._process_metric_for_pods(result, metric_name)
                
                return {
                    'status': 'success',
                    'metric': metric_name,
                    'title': metric_config.get('title', metric_name),
                    'unit': metric_config.get('unit', 'unknown'),
                    'duration': duration,
                    'timezone': 'UTC',
                    'pods': pod_metrics,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': metric_name,
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _process_metric_for_pods(self, result: Dict[str, Any], metric_name: str) -> Dict[str, Dict[str, float]]:
        """Process Prometheus metric result to extract pod-level avg and max values"""
        pod_data = {}
        
        if result['status'] != 'success':
            return pod_data
        
        try:
            # Get series data from Prometheus result
            series_data = result.get('series_data', [])
            
            # Build pod to node mapping for efficient lookups
            pod_to_node = await self.tools_utility.get_pod_to_node_mapping('openshift-etcd')
            
            # Process each series
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                # Extract pod name from various label fields
                pod_name = self._extract_pod_name(labels)
                if not pod_name or pod_name == 'unknown':
                    continue
                
                # Calculate statistics from values
                numeric_values = []
                for value_data in values:
                    if isinstance(value_data, dict):
                        val = value_data.get('value')
                        if val is not None and val != 'NaN':
                            try:
                                numeric_values.append(float(val))
                            except (ValueError, TypeError):
                                continue
                
                if numeric_values:
                    pod_data[pod_name] = {
                        'avg': round(sum(numeric_values) / len(numeric_values), 4),
                        'max': round(max(numeric_values), 4),
                        'min': round(min(numeric_values), 4),
                        'count': len(numeric_values),
                        'node': pod_to_node.get(pod_name, 'unknown')
                    }
        
        except Exception as e:
            self.logger.error(f"Error processing metric data for {metric_name}: {e}")
        
        return pod_data
    
    def _extract_pod_name(self, labels: Dict[str, Any]) -> str:
        """Extract pod name from Prometheus metric labels"""
        # Try various label fields that might contain pod name
        pod_name_fields = [
            'pod',
            'pod_name', 
            'kubernetes_pod_name',
            'name',
            'instance'
        ]
        
        for field in pod_name_fields:
            if field in labels and labels[field]:
                pod_name = str(labels[field])
                # Filter for etcd pods only
                if 'etcd' in pod_name.lower():
                    return pod_name
        
        # Try to extract from instance field if it contains pod info
        if 'instance' in labels:
            instance = str(labels['instance'])
            if 'etcd' in instance.lower() and ':' in instance:
                # Extract pod name from instance like "pod-name:port"
                parts = instance.split(':')
                if parts and 'etcd' in parts[0].lower():
                    return parts[0]
        
        return 'unknown'
    
    async def get_etcd_cluster_overview(self, duration: str = "1h") -> Dict[str, Any]:
        """Get comprehensive etcd cluster overview with key metrics per pod"""
        try:
            # Get cluster member information
            cluster_info = await self.tools_utility.get_etcd_cluster_members()
            health_info = await self.tools_utility.get_etcd_cluster_health()
            
            # Get key metrics summary
            metrics_summary = await self.get_key_metrics_summary(duration)
            
            # Get etcd pods information
            etcd_pods = await self.tools_utility.get_etcd_pods('openshift-etcd')
            
            cluster_overview = {
                'status': 'success',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'duration': duration,
                'timezone': 'UTC',
                'cluster': {
                    'members': cluster_info,
                    'health': health_info,
                    'pods_count': len(etcd_pods),
                    'running_pods': len([p for p in etcd_pods if p['phase'] == 'Running'])
                },
                'metrics': metrics_summary.get('key_metrics', {}),
                'pods': {pod['name']: {
                    'phase': pod['phase'],
                    'node': pod['node_name'],
                    'ready': pod['ready']
                } for pod in etcd_pods}
            }
            
            return cluster_overview
            
        except Exception as e:
            self.logger.error(f"Error getting etcd cluster overview: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }