"""
etcd Disk Backend Commit Collector Module
Collects and analyzes etcd backend commit duration metrics
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pytz

# Import required modules from the project
from config.etcd_config import get_config
from ocauth.ocp_auth import OCPAuth
from tools.ocp_promql_basequery import PrometheusBaseQuery
from tools.etcd_tools_utility import mcpToolsUtility


class DiskBackendCommitCollector:
    """Collector for etcd disk backend commit duration metrics"""
    
    def __init__(self, ocp_auth: OCPAuth):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.utility = mcpToolsUtility(ocp_auth)
        self.timezone = pytz.UTC
        
        # Validate that we have the required metrics category
        if 'disk_backend_commit' not in self.config.get_all_categories():
            raise ValueError("disk_backend_commit metrics not found in configuration")
    
    async def collect_all_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all disk backend commit metrics"""
        try:
            self.logger.info("Starting disk backend commit metrics collection")
            
            # Get Prometheus configuration
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            # Initialize result structure
            result = {
                "status": "success",
                "timestamp": datetime.now(self.timezone).isoformat(),
                "duration": duration,
                "category": "disk_backend_commit",
                "metrics": {}
            }
            
            # Get all metrics for the disk_backend_commit category
            metrics = self.config.get_metrics_by_category('disk_backend_commit')
            
            if not metrics:
                self.logger.warning("No disk_backend_commit metrics found in configuration")
                result["status"] = "warning"
                result["error"] = "No metrics found in configuration"
                return result
            
            # Use Prometheus query client
            async with PrometheusBaseQuery(prometheus_config) as prom_client:
                # Test connection first
                connection_test = await prom_client.test_connection()
                if connection_test['status'] != 'connected':
                    result["status"] = "error"
                    result["error"] = f"Prometheus connection failed: {connection_test.get('error', 'Unknown error')}"
                    return result
                
                # Collect each metric
                for metric in metrics:
                    metric_name = metric['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        # Get metric data with statistics
                        if metric_name == "disk_backend_commit_duration_seconds_p99":
                            metric_result = await self.collect_p99_backend_commit_duration(prom_client, duration)
                        elif metric_name == "backend_commit_duration_sum_rate":
                            metric_result = await self.collect_backend_commit_duration_sum_rate(prom_client, duration)
                        elif metric_name == "backend_commit_duration_sum":
                            metric_result = await self.collect_backend_commit_duration_sum(prom_client, duration)
                        elif metric_name == "backend_commit_duration_count_rate":
                            metric_result = await self.collect_backend_commit_duration_count_rate(prom_client, duration)
                        elif metric_name == "backend_commit_duration_count":
                            metric_result = await self.collect_backend_commit_duration_count(prom_client, duration)
                        else:
                            # Generic metric collection for any other metrics
                            metric_result = await self.collect_generic_metric(prom_client, metric, duration)
                        
                        result["metrics"][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        result["metrics"][metric_name] = {
                            "status": "error",
                            "error": str(e)
                        }
            
            # Calculate overall summary
            result["summary"] = await self._calculate_summary(result["metrics"])
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }

    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Backward-compatible entrypoint expected by server; wraps collect_all_metrics into {status,data,...}."""
        try:
            result = await self.collect_all_metrics(duration)
            if result.get("status") == "success":
                return {
                    "status": "success",
                    "data": {
                        "pods_metrics": result.get("metrics", {}),
                        "summary": result.get("summary", {})
                    },
                    "error": None,
                    "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                    "category": "disk_backend_commit",
                    "duration": duration
                }
            else:
                return {
                    "status": result.get("status", "error"),
                    "data": None,
                    "error": result.get("error"),
                    "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                    "category": "disk_backend_commit",
                    "duration": duration
                }
        except Exception as e:
            return {
                "status": "error",
                "data": None,
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat(),
                "category": "disk_backend_commit",
                "duration": duration
            }
    
    async def collect_p99_backend_commit_duration(self, prom_client: PrometheusBaseQuery, duration: str = "1h") -> Dict[str, Any]:
        """Collect P99 backend commit duration metrics"""
        try:
            metric_config = self.config.get_metric_by_name("disk_backend_commit_duration_seconds_p99")
            if not metric_config:
                return {"status": "error", "error": "Metric configuration not found"}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing P99 query: {query}")
            
            # Execute query with statistics
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            # Process results by pod
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'seconds'),
                "description": "P99 percentile of backend commit duration",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting P99 backend commit duration: {e}")
            return {"status": "error", "error": str(e)}
    
    async def collect_backend_commit_duration_sum_rate(self, prom_client: PrometheusBaseQuery, duration: str = "1h") -> Dict[str, Any]:
        """Collect backend commit duration sum rate metrics"""
        try:
            metric_config = self.config.get_metric_by_name("backend_commit_duration_sum_rate")
            if not metric_config:
                return {"status": "error", "error": "Metric configuration not found"}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing sum rate query: {query}")
            
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'seconds'),
                "description": "Rate of backend commit duration sum",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting backend commit duration sum rate: {e}")
            return {"status": "error", "error": str(e)}
    
    async def collect_backend_commit_duration_sum(self, prom_client: PrometheusBaseQuery, duration: str = "1h") -> Dict[str, Any]:
        """Collect backend commit duration sum metrics"""
        try:
            metric_config = self.config.get_metric_by_name("backend_commit_duration_sum")
            if not metric_config:
                return {"status": "error", "error": "Metric configuration not found"}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing sum query: {query}")
            
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'seconds'),
                "description": "Cumulative backend commit duration sum",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting backend commit duration sum: {e}")
            return {"status": "error", "error": str(e)}
    
    async def collect_backend_commit_duration_count_rate(self, prom_client: PrometheusBaseQuery, duration: str = "1h") -> Dict[str, Any]:
        """Collect backend commit duration count rate metrics"""
        try:
            metric_config = self.config.get_metric_by_name("backend_commit_duration_count_rate")
            if not metric_config:
                return {"status": "error", "error": "Metric configuration not found"}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing count rate query: {query}")
            
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'count'),
                "description": "Rate of backend commit operations",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting backend commit duration count rate: {e}")
            return {"status": "error", "error": str(e)}
    
    async def collect_backend_commit_duration_count(self, prom_client: PrometheusBaseQuery, duration: str = "1h") -> Dict[str, Any]:
        """Collect backend commit duration count metrics"""
        try:
            metric_config = self.config.get_metric_by_name("backend_commit_duration_count")
            if not metric_config:
                return {"status": "error", "error": "Metric configuration not found"}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing count query: {query}")
            
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'count'),
                "description": "Cumulative count of backend commit operations",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting backend commit duration count: {e}")
            return {"status": "error", "error": str(e)}
    
    async def collect_generic_metric(self, prom_client: PrometheusBaseQuery, metric_config: Dict[str, Any], duration: str = "1h") -> Dict[str, Any]:
        """Collect any generic metric based on configuration"""
        try:
            query = metric_config['expr']
            metric_name = metric_config['name']
            
            self.logger.debug(f"Executing generic query for {metric_name}: {query}")
            
            result = await prom_client.query_with_stats(query, duration)
            
            if result['status'] != 'success':
                return {
                    "status": "error",
                    "error": result.get('error', 'Unknown error'),
                    "query": query
                }
            
            pod_metrics = {}
            overall_stats = result.get('overall_statistics', {})
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', 'unknown')
                
                pod_metrics[pod_name] = {
                    "avg": series.get('statistics', {}).get('avg'),
                    "max": series.get('statistics', {}).get('max'),
                    "min": series.get('statistics', {}).get('min'),
                    "latest": series.get('statistics', {}).get('latest'),
                    "count": series.get('statistics', {}).get('count', 0)
                }
            
            return {
                "status": "success",
                "unit": metric_config.get('unit', 'unknown'),
                "description": f"Generic metric: {metric_name}",
                "overall": {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest')
                },
                "pods": pod_metrics,
                "total_data_points": result.get('total_data_points', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting generic metric {metric_config.get('name', 'unknown')}: {e}")
            return {"status": "error", "error": str(e)}
    
    async def _calculate_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics across all metrics"""
        try:
            summary = {
                "total_metrics": len(metrics),
                "successful_metrics": 0,
                "failed_metrics": 0,
                "total_pods": set(),
                "metrics_overview": {}
            }
            
            for metric_name, metric_data in metrics.items():
                if metric_data.get('status') == 'success':
                    summary["successful_metrics"] += 1
                    
                    # Track unique pods
                    if 'pods' in metric_data:
                        summary["total_pods"].update(metric_data['pods'].keys())
                    
                    # Store key metrics overview
                    overall = metric_data.get('overall', {})
                    summary["metrics_overview"][metric_name] = {
                        "avg": overall.get('avg'),
                        "max": overall.get('max'),
                        "unit": metric_data.get('unit', 'unknown')
                    }
                else:
                    summary["failed_metrics"] += 1
            
            # Convert set to list and get count
            summary["total_pods"] = len(summary["total_pods"])
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error calculating summary: {e}")
            return {"error": str(e)}
    
    async def get_cluster_info(self) -> Dict[str, Any]:
        """Get etcd cluster information"""
        try:
            # Get etcd pods info
            etcd_pods = await self.utility.get_etcd_pods()
            
            # Get etcd cluster members
            cluster_members = await self.utility.get_etcd_cluster_members()
            
            # Get cluster health
            cluster_health = await self.utility.get_etcd_cluster_health()
            
            return {
                "status": "success",
                "pods": etcd_pods,
                "cluster_members": cluster_members,
                "cluster_health": cluster_health,
                "timestamp": datetime.now(self.timezone).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cluster info: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }


async def main():
    """Main function for testing the collector"""
    import sys
    import os
    
    # Add parent directory to path for imports
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize OCP authentication
        ocp_auth = OCPAuth()
        if not await ocp_auth.initialize():
            print("Failed to initialize OCP authentication")
            return
        
        # Create collector
        collector = DiskBackendCommitCollector(ocp_auth)
        
        # Collect metrics
        result = await collector.collect_all_metrics(duration="30m")
        
        # Print results
        import json
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"Error in main: {e}")


if __name__ == "__main__":
    asyncio.run(main())