#!/usr/bin/env python3
"""
Cluster Information ELT Module
Extracts and processes OpenShift cluster information from ocp_cluster_info.py results
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime

try:
    from .etcd_analyzer_elt_utility import UtilityELT
except ImportError:
    from etcd_analyzer_elt_utility import UtilityELT

logger = logging.getLogger(__name__)


@dataclass
class ProcessedClusterMetrics:
    """Processed cluster metrics for analysis"""
    # Basic cluster info
    cluster_name: str
    cluster_version: str
    platform: str
    total_nodes: int
    
    # Node breakdown
    master_nodes_count: int
    worker_nodes_count: int
    infra_nodes_count: int
    
    # Resource counts
    namespaces_count: int
    pods_count: int
    services_count: int
    secrets_count: int
    configmaps_count: int
    
    # Network resources
    networkpolicies_count: int
    adminnetworkpolicies_count: int
    baselineadminnetworkpolicies_count: int
    egressfirewalls_count: int
    egressips_count: int
    clusteruserdefinednetworks_count: int
    userdefinednetworks_count: int
    
    # Hardware metrics
    total_cpu_capacity: float
    total_memory_capacity: float  # in GB
    avg_cpu_per_node: float
    avg_memory_per_node: float  # in GB
    
    # Status metrics
    ready_nodes_count: int
    not_ready_nodes_count: int
    unavailable_operators_count: int
    mcp_updating_count: int
    mcp_degraded_count: int
    
    # Collection info
    collection_timestamp: str


class ClusterInfoELT:
    """Extract, Load, Transform cluster information"""
    
    def __init__(self):
        self.utility = UtilityELT()
        self.raw_data = None
        self.processed_metrics = None
        self.critical_thresholds = {
            'cpu_usage': 80.0,
            'memory_usage': 85.0,
            'disk_usage': 90.0,
            'not_ready_nodes': 1,
            'unavailable_operators': 1,
            'degraded_mcp': 1
        }
    
    def extract_from_json(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster information from JSON result"""
        try:
            # Handle nested structure from tool result
            if 'result' in cluster_data and 'data' in cluster_data['result']:
                self.raw_data = cluster_data['result']['data']
            elif 'data' in cluster_data:
                self.raw_data = cluster_data['data']
            else:
                self.raw_data = cluster_data
                
            logger.info(f"Extracted cluster data for: {self.raw_data.get('cluster_name', 'Unknown')}")
            return self.raw_data
            
        except Exception as e:
            logger.error(f"Failed to extract cluster data: {e}")
            raise
    
    def transform_metrics(self) -> ProcessedClusterMetrics:
        """Transform raw cluster data into processed metrics"""
        if not self.raw_data:
            raise ValueError("No raw data to transform. Call extract_from_json first.")
        
        try:
            # Basic cluster info
            cluster_name = self.raw_data.get('cluster_name', 'Unknown')
            cluster_version = self.raw_data.get('cluster_version', 'Unknown')
            platform = self.raw_data.get('platform', 'Unknown')
            total_nodes = self.raw_data.get('total_nodes', 0)
            
            # Node counts
            master_nodes = self.raw_data.get('master_nodes', [])
            worker_nodes = self.raw_data.get('worker_nodes', [])
            infra_nodes = self.raw_data.get('infra_nodes', [])
            
            master_nodes_count = len(master_nodes)
            worker_nodes_count = len(worker_nodes)
            infra_nodes_count = len(infra_nodes)
            
            # Resource counts
            namespaces_count = self.raw_data.get('namespaces_count', 0)
            pods_count = self.raw_data.get('pods_count', 0)
            services_count = self.raw_data.get('services_count', 0)
            secrets_count = self.raw_data.get('secrets_count', 0)
            configmaps_count = self.raw_data.get('configmaps_count', 0)
            
            # Network resources
            networkpolicies_count = self.raw_data.get('networkpolicies_count', 0)
            adminnetworkpolicies_count = self.raw_data.get('adminnetworkpolicies_count', 0)
            baselineadminnetworkpolicies_count = self.raw_data.get('baselineadminnetworkpolicies_count', 0)
            egressfirewalls_count = self.raw_data.get('egressfirewalls_count', 0)
            egressips_count = self.raw_data.get('egressips_count', 0)
            clusteruserdefinednetworks_count = self.raw_data.get('clusteruserdefinednetworks_count', 0)
            userdefinednetworks_count = self.raw_data.get('userdefinednetworks_count', 0)
            
            # Calculate hardware metrics
            all_nodes = master_nodes + worker_nodes + infra_nodes
            total_cpu_capacity = 0
            total_memory_capacity = 0  # in GB
            ready_nodes_count = 0
            not_ready_nodes_count = 0
            
            for node in all_nodes:
                # CPU capacity
                cpu_str = str(node.get('cpu_capacity', '0'))
                try:
                    cpu_val = float(cpu_str)
                    total_cpu_capacity += cpu_val
                except (ValueError, TypeError):
                    logger.warning(f"Invalid CPU capacity for node {node.get('name')}: {cpu_str}")
                
                # Memory capacity (convert from Ki to GB)
                memory_str = str(node.get('memory_capacity', '0Ki'))
                try:
                    if memory_str.endswith('Ki'):
                        memory_ki = float(memory_str[:-2])
                        memory_gb = memory_ki / (1024 * 1024)  # Ki to GB
                        total_memory_capacity += memory_gb
                    else:
                        logger.warning(f"Unexpected memory format for node {node.get('name')}: {memory_str}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid memory capacity for node {node.get('name')}: {memory_str}")
                
                # Node status
                ready_status = node.get('ready_status', '')
                if 'Ready' in ready_status and 'NotReady' not in ready_status:
                    ready_nodes_count += 1
                else:
                    not_ready_nodes_count += 1
            
            # Calculate averages
            avg_cpu_per_node = total_cpu_capacity / total_nodes if total_nodes > 0 else 0
            avg_memory_per_node = total_memory_capacity / total_nodes if total_nodes > 0 else 0
            
            # Operator status
            unavailable_operators = self.raw_data.get('unavailable_cluster_operators', [])
            unavailable_operators_count = len(unavailable_operators)
            
            # MCP status
            mcp_status = self.raw_data.get('mcp_status', {})
            mcp_updating_count = sum(1 for status in mcp_status.values() if status == 'Updating')
            mcp_degraded_count = sum(1 for status in mcp_status.values() if status == 'Degraded')
            
            # Collection timestamp
            collection_timestamp = self.raw_data.get('collection_timestamp', '')
            
            self.processed_metrics = ProcessedClusterMetrics(
                cluster_name=cluster_name,
                cluster_version=cluster_version,
                platform=platform,
                total_nodes=total_nodes,
                master_nodes_count=master_nodes_count,
                worker_nodes_count=worker_nodes_count,
                infra_nodes_count=infra_nodes_count,
                namespaces_count=namespaces_count,
                pods_count=pods_count,
                services_count=services_count,
                secrets_count=secrets_count,
                configmaps_count=configmaps_count,
                networkpolicies_count=networkpolicies_count,
                adminnetworkpolicies_count=adminnetworkpolicies_count,
                baselineadminnetworkpolicies_count=baselineadminnetworkpolicies_count,
                egressfirewalls_count=egressfirewalls_count,
                egressips_count=egressips_count,
                clusteruserdefinednetworks_count=clusteruserdefinednetworks_count,
                userdefinednetworks_count=userdefinednetworks_count,
                total_cpu_capacity=total_cpu_capacity,
                total_memory_capacity=total_memory_capacity,
                avg_cpu_per_node=avg_cpu_per_node,
                avg_memory_per_node=avg_memory_per_node,
                ready_nodes_count=ready_nodes_count,
                not_ready_nodes_count=not_ready_nodes_count,
                unavailable_operators_count=unavailable_operators_count,
                mcp_updating_count=mcp_updating_count,
                mcp_degraded_count=mcp_degraded_count,
                collection_timestamp=collection_timestamp
            )
            
            logger.info(f"Transformed metrics for cluster: {cluster_name}")
            return self.processed_metrics
            
        except Exception as e:
            logger.error(f"Failed to transform cluster metrics: {e}")
            raise
    
    def load_and_process(self, cluster_data: Dict[str, Any]) -> ProcessedClusterMetrics:
        """Complete ELT process: Extract, Transform, and return processed metrics"""
        self.extract_from_json(cluster_data)
        return self.transform_metrics()
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster summary metrics"""
        if not self.processed_metrics:
            raise ValueError("No processed metrics available. Run load_and_process first.")
        
        metrics = self.processed_metrics
        return {
            'cluster_info': {
                'name': metrics.cluster_name,
                'version': metrics.cluster_version,
                'platform': metrics.platform,
                'collection_time': metrics.collection_timestamp
            },
            'node_metrics': {
                'total_nodes': metrics.total_nodes,
                'master_nodes': metrics.master_nodes_count,
                'worker_nodes': metrics.worker_nodes_count,
                'infra_nodes': metrics.infra_nodes_count,
                'ready_nodes': metrics.ready_nodes_count,
                'not_ready_nodes': metrics.not_ready_nodes_count
            },
            'resource_metrics': {
                'namespaces': metrics.namespaces_count,
                'pods': metrics.pods_count,
                'services': metrics.services_count,
                'secrets': metrics.secrets_count,
                'configmaps': metrics.configmaps_count
            },
            'network_metrics': {
                'network_policies': metrics.networkpolicies_count,
                'admin_network_policies': metrics.adminnetworkpolicies_count,
                'baseline_admin_network_policies': metrics.baselineadminnetworkpolicies_count,
                'egress_firewalls': metrics.egressfirewalls_count,
                'egress_ips': metrics.egressips_count,
                'cluster_user_defined_networks': metrics.clusteruserdefinednetworks_count,
                'user_defined_networks': metrics.userdefinednetworks_count
            },
            'hardware_metrics': {
                'total_cpu_cores': metrics.total_cpu_capacity,
                'total_memory_gb': round(metrics.total_memory_capacity, 2),
                'avg_cpu_per_node': round(metrics.avg_cpu_per_node, 2),
                'avg_memory_per_node_gb': round(metrics.avg_memory_per_node, 2)
            },
            'status_metrics': {
                'unavailable_operators': metrics.unavailable_operators_count,
                'mcp_updating': metrics.mcp_updating_count,
                'mcp_degraded': metrics.mcp_degraded_count
            }
        }
    
    def get_critical_metrics(self) -> Dict[str, Any]:
        """Get critical metrics that need attention"""
        if not self.processed_metrics:
            raise ValueError("No processed metrics available. Run load_and_process first.")
        
        metrics = self.processed_metrics
        critical_issues = []
        
        # Check for critical issues
        if metrics.not_ready_nodes_count > 0:
            critical_issues.append(f"{metrics.not_ready_nodes_count} nodes not ready")
        
        if metrics.unavailable_operators_count > 0:
            critical_issues.append(f"{metrics.unavailable_operators_count} operators unavailable")
        
        if metrics.mcp_degraded_count > 0:
            critical_issues.append(f"{metrics.mcp_degraded_count} machine config pools degraded")
        
        if metrics.mcp_updating_count > 0:
            critical_issues.append(f"{metrics.mcp_updating_count} machine config pools updating")
        
        return {
            'has_critical_issues': len(critical_issues) > 0,
            'critical_issues': critical_issues,
            'health_score': self._calculate_health_score()
        }
    
    def _calculate_health_score(self) -> float:
        """Calculate basic health score (0-100)"""
        if not self.processed_metrics:
            return 0.0
        
        metrics = self.processed_metrics
        score = 100.0
        
        # Deduct points for issues
        if metrics.not_ready_nodes_count > 0:
            score -= (metrics.not_ready_nodes_count / metrics.total_nodes) * 30
        
        if metrics.unavailable_operators_count > 0:
            score -= metrics.unavailable_operators_count * 10
        
        if metrics.mcp_degraded_count > 0:
            score -= metrics.mcp_degraded_count * 20
        
        if metrics.mcp_updating_count > 0:
            score -= metrics.mcp_updating_count * 5
        
        return max(0.0, min(100.0, score))

    def cluster_info_to_html_table(self, cluster_data: Dict[str, Any], 
                                  show_summary: bool = True,
                                  show_details: bool = True) -> str:
        """
        Convert cluster information to formatted HTML table with metrics analysis
        
        Args:
            cluster_data: Cluster information from ocp_cluster_info.py
            show_summary: Show summary metrics table
            show_details: Show detailed breakdown tables
        
        Returns:
            Complete HTML with styled tables
        """
        try:
            # Extract data from nested structure if needed
            if 'result' in cluster_data and 'data' in cluster_data['result']:
                data = cluster_data['result']['data']
            elif 'data' in cluster_data:
                data = cluster_data['data']
            else:
                data = cluster_data
            
            html_output = """
            <div class="cluster-info-dashboard">
                <div class="dashboard-header">
                    <h2>🏢 OpenShift Cluster Information Dashboard</h2>
                    <div class="collection-info">
                        <span>📅 Last Updated: {timestamp}</span>
                    </div>
                </div>
            """.format(
                timestamp=self._format_timestamp(data.get('collection_timestamp', 'Unknown'))
            )
            
            if show_summary:
                html_output += self._create_cluster_summary_section(data)
            
            if show_details:
                html_output += self._create_cluster_details_sections(data)
            
            html_output += "</div>"
            html_output += self._get_cluster_dashboard_styles()
            
            return html_output
            
        except Exception as e:
            logger.error(f"Failed to convert cluster info to HTML: {e}")
            return f"<p>Error generating cluster dashboard: {e}</p>"
    
    def _create_cluster_summary_section(self, data: Dict) -> str:
        """Create cluster summary section with key metrics"""
        
        # Calculate summary metrics
        total_cpu = 0
        total_memory_gb = 0
        all_nodes = data.get('master_nodes', []) + data.get('worker_nodes', []) + data.get('infra_nodes', [])
        
        for node in all_nodes:
            cpu_val = self.utility.parse_cpu_value(str(node.get('cpu_capacity', '0')))
            total_cpu += cpu_val
            
            memory_str = str(node.get('memory_capacity', '0Ki'))
            memory_bytes = self.utility.convert_memory_to_bytes(memory_str)
            total_memory_gb += memory_bytes / (1024**3)  # Convert to GB
        
        summary_metrics = {
            'Cluster Name': data.get('cluster_name', 'Unknown'),
            'Version': data.get('cluster_version', 'Unknown'),
            'Platform': data.get('platform', 'Unknown'),
            'Total Nodes': data.get('total_nodes', 0),
            'Total CPU Cores': f"{total_cpu:.1f}",
            'Total Memory': self.utility.convert_bytes_to_readable(total_memory_gb * 1024**3),
            'Namespaces': data.get('namespaces_count', 0),
            'Pods': data.get('pods_count', 0),
            'Services': data.get('services_count', 0),
            'Ready Nodes': len([n for n in all_nodes if 'Ready' in n.get('ready_status', '') and 'NotReady' not in n.get('ready_status', '')]),
            'Unavailable Operators': len(data.get('unavailable_cluster_operators', []))
        }
        
        html = """
        <div class="summary-section">
            <h3>📊 Cluster Summary</h3>
            <div class="summary-grid">
        """
        
        for key, value in summary_metrics.items():
            css_class = self._get_summary_metric_class(key, value)
            icon = self._get_metric_icon(key)
            
            html += f"""
                <div class="summary-card {css_class}">
                    <div class="metric-icon">{icon}</div>
                    <div class="metric-content">
                        <div class="metric-label">{key}</div>
                        <div class="metric-value">{value}</div>
                    </div>
                </div>
            """
        
        html += """
            </div>
        </div>
        """
        
        return html
    
    def _create_cluster_details_sections(self, data: Dict) -> str:
        """Create detailed sections for cluster information"""
        
        html = """
        <div class="details-sections">
        """
        
        # Node Information Section
        html += self._create_nodes_section(data)
        
        # Resource Counts Section
        html += self._create_resources_section(data)
        
        # Network Resources Section
        html += self._create_network_section(data)
        
        # Status Section
        html += self._create_status_section(data)
        
        html += """
        </div>
        """
        
        return html
    
    def _create_nodes_section(self, data: Dict) -> str:
        """Create nodes information section"""
        
        html = """
        <div class="detail-section">
            <h3>🖥️ Node Information</h3>
        """
        
        # Node summary table
        node_summary = {
            'Master Nodes': len(data.get('master_nodes', [])),
            'Worker Nodes': len(data.get('worker_nodes', [])),
            'Infrastructure Nodes': len(data.get('infra_nodes', [])),
            'Total Nodes': data.get('total_nodes', 0)
        }
        
        html += self._create_metrics_table("Node Distribution", node_summary)
        
        # Detailed node information
        all_nodes = []
        for node_type in ['master_nodes', 'worker_nodes', 'infra_nodes']:
            for node in data.get(node_type, []):
                node_info = node.copy()
                node_info['Node Type'] = node_type.split('_')[0].title()
                all_nodes.append(node_info)
        
        if all_nodes:
            html += self._create_node_details_table(all_nodes)
        
        html += "</div>"
        return html
    
    def _create_node_details_table(self, nodes: List[Dict]) -> str:
        """Create detailed node information table"""
        
        html = """
        <div class="node-details">
            <h4>Node Details</h4>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Type</th>
                        <th>CPU</th>
                        <th>Memory</th>
                        <th>Status</th>
                        <th>OS</th>
                        <th>Kubelet</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for node in nodes:
            status_class = "critical" if "NotReady" in node.get('ready_status', '') else "healthy"
            memory_readable = self.utility.convert_bytes_to_readable(
                self.utility.convert_memory_to_bytes(str(node.get('memory_capacity', '0Ki')))
            )
            
            html += f"""
                <tr>
                    <td class="node-name">{node.get('name', 'Unknown')}</td>
                    <td class="node-type">{node.get('Node Type', node.get('node_type', 'Unknown'))}</td>
                    <td>{node.get('cpu_capacity', 'Unknown')} cores</td>
                    <td>{memory_readable}</td>
                    <td class="{status_class}">{node.get('ready_status', 'Unknown')}</td>
                    <td class="os-info">{node.get('os_image', 'Unknown')[:30]}...</td>
                    <td>{node.get('kubelet_version', 'Unknown')}</td>
                </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        """
        
        return html
    
    def _create_resources_section(self, data: Dict) -> str:
        """Create resource counts section"""
        
        resource_metrics = {
            'Namespaces': data.get('namespaces_count', 0),
            'Pods': data.get('pods_count', 0),
            'Services': data.get('services_count', 0),
            'Secrets': data.get('secrets_count', 0),
            'ConfigMaps': data.get('configmaps_count', 0)
        }
        
        html = """
        <div class="detail-section">
            <h3>📦 Resource Counts</h3>
        """
        
        html += self._create_metrics_table("Kubernetes Resources", resource_metrics)
        html += "</div>"
        return html
    
    def _create_network_section(self, data: Dict) -> str:
        """Create network resources section"""
        
        network_metrics = {
            'Network Policies': data.get('networkpolicies_count', 0),
            'Admin Network Policies': data.get('adminnetworkpolicies_count', 0),
            'Baseline Admin Network Policies': data.get('baselineadminnetworkpolicies_count', 0),
            'Egress Firewalls': data.get('egressfirewalls_count', 0),
            'Egress IPs': data.get('egressips_count', 0),
            'Cluster User Defined Networks': data.get('clusteruserdefinednetworks_count', 0),
            'User Defined Networks': data.get('userdefinednetworks_count', 0)
        }
        
        html = """
        <div class="detail-section">
            <h3>🌐 Network Resources</h3>
        """
        
        html += self._create_metrics_table("Network Components", network_metrics)
        html += "</div>"
        return html
    
    def _create_status_section(self, data: Dict) -> str:
        """Create cluster status section"""
        
        html = """
        <div class="detail-section">
            <h3>⚠️ Cluster Status</h3>
        """
        
        # Operator Status
        unavailable_ops = data.get('unavailable_cluster_operators', [])
        if unavailable_ops:
            html += """
            <div class="status-alert critical">
                <h4>🚨 Unavailable Cluster Operators</h4>
                <ul>
            """
            for op in unavailable_ops:
                html += f"<li>{op}</li>"
            html += "</ul></div>"
        else:
            html += """
            <div class="status-alert healthy">
                <h4>✅ All Cluster Operators Available</h4>
                <p>No unavailable operators detected.</p>
            </div>
            """
        
        # MCP Status
        mcp_status = data.get('mcp_status', {})
        if mcp_status:
            html += """
            <div class="mcp-status">
                <h4>🔧 Machine Config Pool Status</h4>
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Pool Name</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for pool, status in mcp_status.items():
                status_class = self._get_mcp_status_class(status)
                html += f"""
                        <tr>
                            <td>{pool}</td>
                            <td class="{status_class}">{status}</td>
                        </tr>
                """
            html += """
                    </tbody>
                </table>
            </div>
            """
        
        html += "</div>"
        return html
    
    def _create_metrics_table(self, title: str, metrics: Dict[str, Any]) -> str:
        """Create a simple metrics table"""
        
        html = f"""
        <div class="metrics-table">
            <h4>{title}</h4>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Metric</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Find top metric for highlighting
        numeric_values = {k: v for k, v in metrics.items() if isinstance(v, (int, float)) and v > 0}
        top_metric_key = max(numeric_values.keys(), key=lambda k: numeric_values[k]) if numeric_values else None
        
        for key, value in metrics.items():
            row_class = ""
            if key == top_metric_key:
                row_class = "top-metric"
            elif isinstance(value, (int, float)) and value > 0:
                if any(critical in key.lower() for critical in ['unavailable', 'error', 'failed', 'degraded']):
                    row_class = "critical"
            
            formatted_value = self._format_value(value)
            
            html += f"""
                    <tr class="{row_class}">
                        <td class="metric-name">{key}</td>
                        <td class="metric-value">{formatted_value}</td>
                    </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        """
        
        return html
    
    def _get_mcp_status_class(self, status: str) -> str:
        """Get CSS class for MCP status"""
        status_lower = status.lower()
        
        if status_lower == 'degraded':
            return "critical"
        elif status_lower == 'updating':
            return "warning"
        elif status_lower == 'updated':
            return "healthy"
        else:
            return ""
    
    def _format_timestamp(self, timestamp: str) -> str:
        """Format timestamp for display"""
        return self.utility.format_timestamp(timestamp)
    
    def _format_value(self, value: Any) -> str:
        """Format value for display with appropriate units"""
        if value is None:
            return "N/A"
        
        if isinstance(value, bool):
            return "✓" if value else "✗"
        
        if isinstance(value, (int, float)):
            return self._format_numeric_value(value)
        
        if isinstance(value, str):
            # Handle timestamp formatting
            if 'timestamp' in str(value).lower() or value.endswith('+00:00'):
                return self.utility.format_timestamp(value)
            
            # Handle memory units
            if value.endswith('Ki') or value.endswith('Mi') or value.endswith('Gi'):
                bytes_val = self.utility.convert_memory_to_bytes(value)
                return self.utility.convert_bytes_to_readable(bytes_val)
        
        if isinstance(value, list):
            return f"[{len(value)} items]" if len(value) > 5 else str(value)
        
        return str(value)
    
    def _format_numeric_value(self, value: Union[int, float]) -> str:
        """Format numeric values with appropriate units"""
        if isinstance(value, float):
            # Memory values (assuming GB)
            if value > 1000:
                return f"{value/1000:.1f} TB"
            elif value > 1:
                return f"{value:.1f} GB"
            elif value > 0.001:
                return f"{value*1000:.0f} MB"
            else:
                return f"{value:.3f}"
        else:
            # Integer values
            if value > 1000000:
                return f"{value/1000000:.1f}M"
            elif value > 1000:
                return f"{value/1000:.1f}K"
            else:
                return str(value)
    
    def _get_summary_metric_class(self, key: str, value: Any) -> str:
        """Get CSS class for summary metric card"""
        key_lower = key.lower()
        
        if 'unavailable' in key_lower and isinstance(value, (int, float)) and value > 0:
            return "critical"
        
        if key_lower in ['total nodes', 'total cpu cores', 'total memory']:
            return "highlight"
        
        if isinstance(value, (int, float)) and value > 0:
            if key_lower in ['pods', 'services', 'namespaces']:
                return "info"
        
        return ""
    
    def _get_metric_icon(self, key: str) -> str:
        """Get icon for metric"""
        key_lower = key.lower()
        
        icon_map = {
            'cluster name': '🏢',
            'version': '📋',
            'platform': '☁️',
            'total nodes': '🖥️',
            'total cpu cores': '⚡',
            'total memory': '💾',
            'namespaces': '📁',
            'pods': '🐳',
            'services': '🔗',
            'ready nodes': '✅',
            'unavailable operators': '⚠️'
        }
        
        return icon_map.get(key_lower, '📊')
    
    def _get_cluster_dashboard_styles(self) -> str:
        """Get CSS styles for cluster dashboard"""
        return """
        <style>
        .cluster-info-dashboard {
            max-width: 1200px;
            margin: 0 auto;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
        }
        
        .dashboard-header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .dashboard-header h2 {
            margin: 0 0 10px 0;
            font-size: 2em;
        }
        
        .collection-info {
            opacity: 0.9;
            font-size: 0.9em;
        }
        
        .summary-section {
            margin-bottom: 30px;
        }
        
        .summary-section h3 {
            color: #495057;
            border-bottom: 3px solid #007bff;
            padding-bottom: 8px;
            margin-bottom: 20px;
        }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .summary-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex;
            align-items: center;
            transition: transform 0.2s;
        }
        
        .summary-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }
        
        .summary-card.critical {
            border-left: 4px solid #dc3545;
            background: #fff5f5;
        }
        
        .summary-card.highlight {
            border-left: 4px solid #28a745;
            background: #f8fff9;
        }
        
        .summary-card.info {
            border-left: 4px solid #007bff;
            background: #f8fbff;
        }
        
        .metric-icon {
            font-size: 2em;
            margin-right: 15px;
            opacity: 0.8;
        }
        
        .metric-content {
            flex: 1;
        }
        
        .metric-label {
            font-size: 0.9em;
            color: #6c757d;
            margin-bottom: 4px;
        }
        
        .metric-value {
            font-size: 1.4em;
            font-weight: bold;
            color: #212529;
        }
        
        .details-sections {
            display: grid;
            gap: 25px;
        }
        
        .detail-section {
            background: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .detail-section h3 {
            color: #495057;
            border-bottom: 2px solid #007bff;
            padding-bottom: 8px;
            margin-bottom: 20px;
            font-size: 1.3em;
        }
        
        .metrics-table {
            margin: 15px 0;
        }
        
        .metrics-table h4 {
            color: #495057;
            margin-bottom: 10px;
            font-size: 1.1em;
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
            font-size: 0.9em;
        }
        
        .data-table th {
            background-color: #f1f3f5;
            color: #495057;
            font-weight: 600;
            padding: 12px;
            text-align: left;
            border: 1px solid #dee2e6;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .data-table td {
            padding: 10px 12px;
            border: 1px solid #dee2e6;
            vertical-align: middle;
        }
        
        .data-table tbody tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        
        .data-table tbody tr:hover {
            background-color: #e9ecef;
        }
        
        .data-table .critical {
            background-color: #f8d7da !important;
            color: #721c24;
            font-weight: bold;
        }
        
        .data-table .warning {
            background-color: #fff3cd !important;
            color: #856404;
        }
        
        .data-table .healthy {
            background-color: #d1edff !important;
            color: #0c5460;
        }
        
        .data-table .top-metric {
            background-color: #d4edda !important;
            color: #155724;
            font-weight: bold;
        }
        
        .node-name {
            font-family: monospace;
            font-size: 0.85em;
        }
        
        .node-type {
            text-transform: uppercase;
            font-weight: 600;
            font-size: 0.8em;
        }
        
        .os-info {
            font-size: 0.8em;
            color: #6c757d;
        }
        
        .status-alert {
            padding: 15px;
            border-radius: 5px;
            margin: 15px 0;
        }
        
        .status-alert.critical {
            background-color: #f8d7da;
            border-left: 4px solid #dc3545;
            color: #721c24;
        }
        
        .status-alert.healthy {
            background-color: #d4edda;
            border-left: 4px solid #28a745;
            color: #155724;
        }
        
        .status-alert h4 {
            margin: 0 0 10px 0;
            font-size: 1em;
        }
        
        .status-alert ul {
            margin: 10px 0;
            padding-left: 20px;
        }
        
        .status-alert li {
            margin: 5px 0;
            font-family: monospace;
        }
        
        .mcp-status {
            margin: 20px 0;
        }
        
        .mcp-status h4 {
            color: #495057;
            margin-bottom: 10px;
        }
        
        @media (max-width: 768px) {
            .summary-grid {
                grid-template-columns: 1fr;
            }
            
            .cluster-info-dashboard {
                padding: 15px;
            }
            
            .dashboard-header h2 {
                font-size: 1.5em;
            }
        }
        </style>
        """