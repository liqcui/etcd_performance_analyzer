"""
etcd Disk Compact and Defrag Collector Module
Collects database compaction and defragmentation metrics
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pytz
from config.etcd_config import get_config
from tools.ocp_promql_basequery import PrometheusBaseQuery
from tools.etcd_tools_utility import mcpToolsUtility
from ocauth.ocp_auth import OCPAuth


class CompactDefragCollector:
    """Collector for etcd disk compact and defrag metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h"):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.utility = mcpToolsUtility(ocp_auth)
        self.timezone = pytz.UTC
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all compact and defrag metrics"""
        try:
            # Get prometheus configuration
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            # Get metrics configuration for disk_compact_defrag category
            metrics = self.config.get_metrics_by_category("disk_compact_defrag")
            
            if not metrics:
                return {
                    "status": "error",
                    "error": "No disk_compact_defrag metrics found in configuration",
                    "timestamp": datetime.now(self.timezone).isoformat()
                }
            
            # Get pod to node mapping and master nodes
            pod_node_mapping = await self.utility.get_pod_to_node_mapping()
            node_exporter_mapping = await self.utility.get_node_exporter_to_node_mapping()
            master_nodes = await self.utility.get_master_nodes()
            
            self.logger.info(f"Master nodes found: {master_nodes}")
            self.logger.info(f"Node-exporter mapping: {list(node_exporter_mapping.keys())[:5]}...")  # Show first 5
            
            result = {
                "status": "success",
                "timestamp": datetime.now(self.timezone).isoformat(),
                "duration": self.duration,
                "category": "disk_compact_defrag",
                "metrics": {},
                "summary": {
                    "total_metrics": len(metrics),
                    "collected_metrics": 0,
                    "failed_metrics": 0
                },
                "master_nodes": master_nodes
            }
            
            # Collect each metric
            async with PrometheusBaseQuery(prometheus_config) as prom:
                for metric_config in metrics:
                    metric_name = metric_config['name']
                    
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                        result["metrics"][metric_name] = metric_result
                        
                        if metric_result["status"] == "success":
                            result["summary"]["collected_metrics"] += 1
                        else:
                            result["summary"]["failed_metrics"] += 1
                    
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        result["metrics"][metric_name] = {
                            "status": "error",
                            "error": str(e),
                            "timestamp": datetime.now(self.timezone).isoformat()
                        }
                        result["summary"]["failed_metrics"] += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }

    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Backward-compatible entrypoint expected by server; delegates to collect_all_metrics."""
        self.duration = duration
        result = await self.collect_all_metrics()
        # Adapt shape to what the server expects: top-level 'data'
        if result.get("status") == "success":
            return {
                "status": "success",
                "data": {
                    "metrics": result.get("metrics", {}),
                    "summary": result.get("summary", {}),
                    "master_nodes": result.get("master_nodes", [])
                },
                "error": None,
                "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                "category": "disk_compact_defrag",
                "duration": self.duration
            }
        else:
            return {
                "status": result.get("status", "error"),
                "data": None,
                "error": result.get("error"),
                "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                "category": "disk_compact_defrag",
                "duration": self.duration
            }
    
    async def _collect_single_metric(self, prom: PrometheusBaseQuery, 
                                   metric_config: Dict[str, Any], 
                                   pod_node_mapping: Dict[str, str],
                                   node_exporter_mapping: Dict[str, str],
                                   master_nodes: List[str]) -> Dict[str, Any]:
        """Collect a single metric with statistics"""
        metric_name = metric_config['name']
        
        try:
            # Query metric data
            query_result = await prom.query_with_stats(metric_config['expr'], self.duration)
            
            if query_result['status'] != 'success':
                return {
                    "status": "error",
                    "error": query_result.get('error', 'Query failed'),
                    "query": metric_config['expr'],
                    "timestamp": datetime.now(self.timezone).isoformat()
                }
            
            # Process the results
            metric_result = {
                "status": "success",
                "name": metric_name,
                "unit": metric_config.get('unit', ''),
                "description": metric_config.get('description', ''),
                "query": metric_config['expr'],
                "timestamp": datetime.now(self.timezone).isoformat(),
                "data": {}
            }
            
            # Process per-pod/instance data
            series_data = query_result.get('series_data', [])
            pods_data = {}
            filtered_data = {}  # For vmstat metrics filtered by master nodes
            
            # Check if this is a vmstat metric that should be filtered by master nodes
            is_vmstat_metric = 'vmstat' in metric_name
            
            for series in series_data:
                labels = series.get('labels', {})
                stats = series.get('statistics', {})
                
                if is_vmstat_metric:
                    # For vmstat metrics, use instance label and filter by master nodes
                    instance = labels.get('instance', '')
                    
                    # Extract node name from instance (format could be IP:port or hostname:port)
                    node_name = self._extract_node_from_instance(instance, master_nodes)
                    
                    # Only include if it's a master node
                    if node_name in master_nodes:
                        # Use instance as key for vmstat metrics
                        filtered_data[instance] = {
                            "node": node_name,
                            "avg": stats.get('avg'),
                            "max": stats.get('max'),
                            "min": stats.get('min'),
                            "latest": stats.get('latest'),
                            "count": stats.get('count', 0)
                        }
                        self.logger.debug(f"Included vmstat metric for master node {node_name} (instance: {instance})")
                    else:
                        self.logger.debug(f"Filtered out non-master node for vmstat metric: {node_name} (instance: {instance})")
                else:
                    # For non-vmstat metrics, use existing logic
                    pod_name = labels.get('pod', labels.get('instance', 'unknown'))
                    
                    # Get node name for this pod
                    node_name = 'unknown'
                    if pod_name in pod_node_mapping:
                        node_name = pod_node_mapping[pod_name]
                    elif pod_name in node_exporter_mapping:
                        node_name = node_exporter_mapping[pod_name]
                    elif 'node-exporter' in pod_name:
                        # Try to find the node name in node_exporter_mapping
                        for ne_pod, ne_node in node_exporter_mapping.items():
                            if ne_pod == pod_name:
                                node_name = ne_node
                                break
                    
                    pods_data[pod_name] = {
                        "node": node_name,
                        "avg": stats.get('avg'),
                        "max": stats.get('max'),
                        "min": stats.get('min'),
                        "latest": stats.get('latest'),
                        "count": stats.get('count', 0)
                    }
            
            # For vmstat metrics, use filtered data; for others, use regular pod data
            if is_vmstat_metric:
                metric_result["data"]["instances"] = filtered_data
                metric_result["data"]["total_instances"] = len(filtered_data)
                
                # Calculate overall statistics only for master nodes
                if filtered_data:
                    all_values = []
                    latest_values = []
                    for instance_data in filtered_data.values():
                        if instance_data.get('avg') is not None:
                            all_values.append(instance_data['avg'])
                        if instance_data.get('latest') is not None:
                            latest_values.append(instance_data['latest'])
                    
                    if all_values:
                        metric_result["overall"] = {
                            "avg": sum(all_values) / len(all_values),
                            "max": max(all_values),
                            "min": min(all_values),
                            "latest": latest_values[-1] if latest_values else None,
                            "count": len(all_values)
                        }
                    else:
                        metric_result["overall"] = {
                            "avg": None,
                            "max": None,
                            "min": None,
                            "latest": None,
                            "count": 0
                        }
                else:
                    metric_result["overall"] = {
                        "avg": None,
                        "max": None,
                        "min": None,
                        "latest": None,
                        "count": 0
                    }
            else:
                # Regular metrics (non-vmstat)
                metric_result["data"]["pods"] = pods_data
                metric_result["data"]["total_pods"] = len(pods_data)
                
                # Overall statistics (use existing from query_result)
                overall_stats = query_result.get('overall_statistics', {})
                metric_result["overall"] = {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest'),
                    "count": overall_stats.get('count', 0)
                }
            
            return metric_result
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "query": metric_config['expr'],
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    def _extract_node_from_instance(self, instance: str, master_nodes: List[str]) -> str:
        """Extract node name from instance string and match with master nodes"""
        if not instance:
            return 'unknown'
        
        # Remove port if present (format: hostname:port or ip:port)
        node_candidate = instance.split(':')[0]
        
        # Direct match with master nodes
        if node_candidate in master_nodes:
            return node_candidate
        
        # Try to match by IP address resolution or partial matching
        for master_node in master_nodes:
            # Check if the instance contains the master node name
            if master_node.lower() in instance.lower() or instance.lower() in master_node.lower():
                return master_node
        
        # If no match found, return the extracted candidate
        self.logger.debug(f"Could not match instance '{instance}' to any master node, using '{node_candidate}'")
        return node_candidate
    
    # Individual metric collection functions
    
    async def collect_compaction_duration(self) -> Dict[str, Any]:
        """Collect compaction duration metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                # Get metrics for compaction duration
                compaction_metrics = [
                    'debugging_mvcc_db_compaction_duration_sum_delta',
                    'debugging_mvcc_db_compaction_duration_sum'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = await self.utility.get_pod_to_node_mapping()
                node_exporter_mapping = await self.utility.get_node_exporter_to_node_mapping()
                master_nodes = await self.utility.get_master_nodes()
                
                for metric_name in compaction_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_defrag_duration(self) -> Dict[str, Any]:
        """Collect defragmentation duration metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                # Get metrics for defrag duration
                defrag_metrics = [
                    'defrag_inflight_duration_sum', 
                    'disk_backend_defrag_duration_sum_rate',
                    'disk_backend_defrag_duration_sum'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = await self.utility.get_pod_to_node_mapping()
                node_exporter_mapping = await self.utility.get_node_exporter_to_node_mapping()
                master_nodes = await self.utility.get_master_nodes()
                
                for metric_name in defrag_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_vmstat_pgfault(self) -> Dict[str, Any]:
        """Collect VMStat page fault metrics (filtered by master nodes)"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                # Get metrics for VMStat page faults
                vmstat_metrics = [
                    'vmstat_pgmajfault_rate',
                    'vmstat_pgmajfault_total'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = await self.utility.get_pod_to_node_mapping()
                node_exporter_mapping = await self.utility.get_node_exporter_to_node_mapping()
                master_nodes = await self.utility.get_master_nodes()
                
                self.logger.info(f"Collecting vmstat metrics filtered by master nodes: {master_nodes}")
                
                for metric_name in vmstat_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_compacted_keys(self) -> Dict[str, Any]:
        """Collect compacted keys metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                metric_name = 'debugging_mvcc_db_compacted_keys'
                metric_config = self.config.get_metric_by_name(metric_name)
                
                if not metric_config:
                    return {
                        "status": "error",
                        "error": f"Metric {metric_name} not found in configuration",
                        "timestamp": datetime.now(self.timezone).isoformat()
                    }
                
                pod_node_mapping = await self.utility.get_pod_to_node_mapping()
                node_exporter_mapping = await self.utility.get_node_exporter_to_node_mapping()
                master_nodes = await self.utility.get_master_nodes()
                
                result = await self._collect_single_metric(
                    prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                )
                
                return {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metric": result
                }
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def get_summary(self) -> Dict[str, Any]:
        """Get summary of all compact/defrag metrics"""
        try:
            all_metrics = await self.collect_all_metrics()
            
            if all_metrics["status"] != "success":
                return all_metrics
            
            # Create summary
            summary = {
                "status": "success",
                "timestamp": datetime.now(self.timezone).isoformat(),
                "category": "disk_compact_defrag",
                "duration": self.duration,
                "master_nodes": all_metrics.get("master_nodes", []),
                "overview": {
                    "compaction": {},
                    "defragmentation": {},
                    "page_faults": {}
                }
            }
            
            metrics_data = all_metrics.get("metrics", {})
            
            # Summarize compaction metrics
            compaction_metrics = [k for k in metrics_data.keys() if 'compaction' in k.lower()]
            if compaction_metrics:
                compaction_summary = {
                    "total_metrics": len(compaction_metrics),
                    "avg_max": max([
                        metrics_data[m].get("overall", {}).get("max", 0) or 0 
                        for m in compaction_metrics
                    ], default=0),
                    "avg_avg": sum([
                        metrics_data[m].get("overall", {}).get("avg", 0) or 0 
                        for m in compaction_metrics
                    ]) / len(compaction_metrics) if compaction_metrics else 0
                }
                summary["overview"]["compaction"] = compaction_summary
            
            # Summarize defrag metrics
            defrag_metrics = [k for k in metrics_data.keys() if 'defrag' in k.lower()]
            if defrag_metrics:
                defrag_summary = {
                    "total_metrics": len(defrag_metrics),
                    "avg_max": max([
                        metrics_data[m].get("overall", {}).get("max", 0) or 0 
                        for m in defrag_metrics
                    ], default=0),
                    "avg_avg": sum([
                        metrics_data[m].get("overall", {}).get("avg", 0) or 0 
                        for m in defrag_metrics
                    ]) / len(defrag_metrics) if defrag_metrics else 0
                }
                summary["overview"]["defragmentation"] = defrag_summary
            
            # Summarize page fault metrics
            pgfault_metrics = [k for k in metrics_data.keys() if 'pgmajfault' in k.lower()]
            if pgfault_metrics:
                pgfault_summary = {
                    "total_metrics": len(pgfault_metrics),
                    "avg_max": max([
                        metrics_data[m].get("overall", {}).get("max", 0) or 0 
                        for m in pgfault_metrics
                    ], default=0),
                    "avg_avg": sum([
                        metrics_data[m].get("overall", {}).get("avg", 0) or 0 
                        for m in pgfault_metrics
                    ]) / len(pgfault_metrics) if pgfault_metrics else 0,
                    "filtered_by_master_nodes": True
                }
                summary["overview"]["page_faults"] = pgfault_summary
            
            return summary
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }