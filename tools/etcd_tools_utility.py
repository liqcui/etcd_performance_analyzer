"""
etcd Tools Utility Module
Common utility functions for etcd monitoring and OpenShift operations
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from kubernetes import client
from kubernetes.client.rest import ApiException
import json


class mcpToolsUtility:
    """Utility class for common etcd monitoring operations"""
    
    def __init__(self, ocp_auth):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self._pod_node_cache = {}
        self._nodes_cache = None
        self._master_nodes_cache = None
        self._cache_ttl = 300  # 5 minutes cache
        self._cache_timestamp = 0
    
    async def get_etcd_pods(self, namespace: str = "openshift-etcd") -> List[Dict[str, Any]]:
        """Get list of etcd pods with their details"""
        try:
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            v1 = client.CoreV1Api(self.ocp_auth.k8s_client)
            
            pods = v1.list_namespaced_pod(
                namespace=namespace,
                label_selector="app=etcd"
            )
            
            pod_list = []
            for pod in pods.items:
                pod_info = {
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'phase': pod.status.phase,
                    'node_name': pod.spec.node_name,
                    'pod_ip': pod.status.pod_ip,
                    'host_ip': pod.status.host_ip,
                    'created': pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                    'labels': dict(pod.metadata.labels) if pod.metadata.labels else {},
                    'ready': self._is_pod_ready(pod)
                }
                
                # Get container status
                if pod.status.container_statuses:
                    container_info = []
                    for container_status in pod.status.container_statuses:
                        container_info.append({
                            'name': container_status.name,
                            'ready': container_status.ready,
                            'restart_count': container_status.restart_count,
                            'image': container_status.image
                        })
                    pod_info['containers'] = container_info
                
                pod_list.append(pod_info)
            
            return pod_list
            
        except Exception as e:
            self.logger.error(f"Error getting etcd pods: {e}")
            return []
    
    async def get_master_nodes(self) -> List[str]:
        """Get list of master/control-plane node names"""
        try:
            current_time = asyncio.get_event_loop().time()
            
            # Check cache
            if (self._master_nodes_cache and 
                current_time - self._cache_timestamp < self._cache_ttl):
                return self._master_nodes_cache
            
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            v1 = client.CoreV1Api(self.ocp_auth.k8s_client)
            nodes = v1.list_node()
            
            master_nodes = []
            for node in nodes.items:
                labels = node.metadata.labels or {}
                
                # Check for master/control-plane labels
                is_master = (
                    labels.get('node-role.kubernetes.io/master') == '' or
                    labels.get('node-role.kubernetes.io/control-plane') == '' or
                    labels.get('kubernetes.io/role') == 'master'
                )
                
                if is_master:
                    master_nodes.append(node.metadata.name)
                    self.logger.debug(f"Found master node: {node.metadata.name}")
            
            # Update cache
            self._master_nodes_cache = master_nodes
            self._cache_timestamp = current_time
            
            self.logger.info(f"Found {len(master_nodes)} master nodes: {master_nodes}")
            return master_nodes
            
        except Exception as e:
            self.logger.error(f"Error getting master nodes: {e}")
            return []
    
    async def get_pod_to_node_mapping(self, namespace: str = "openshift-etcd") -> Dict[str, str]:
        """Get mapping of pod names to node names"""
        try:
            cache_key = f"pod_node_{namespace}"
            current_time = asyncio.get_event_loop().time()
            
            # Check cache
            if (cache_key in self._pod_node_cache and 
                current_time - self._cache_timestamp < self._cache_ttl):
                return self._pod_node_cache[cache_key]
            
            # Refresh cache
            pods = await self.get_etcd_pods(namespace)
            mapping = {}
            
            for pod in pods:
                if pod['name'] and pod['node_name']:
                    mapping[pod['name']] = pod['node_name']
            
            # Update cache
            self._pod_node_cache[cache_key] = mapping
            self._cache_timestamp = current_time
            
            return mapping
            
        except Exception as e:
            self.logger.error(f"Error getting pod to node mapping: {e}")
            return {}
    
    async def get_node_exporter_to_node_mapping(self) -> Dict[str, str]:
        """Get mapping of node-exporter pod names to actual node names"""
        try:
            current_time = asyncio.get_event_loop().time()
            cache_key = "node_exporter_mapping"
            
            # Check cache
            if (cache_key in self._pod_node_cache and 
                current_time - self._cache_timestamp < self._cache_ttl):
                return self._pod_node_cache[cache_key]
            
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            v1 = client.CoreV1Api(self.ocp_auth.k8s_client)
            
            # Get node-exporter pods from openshift-monitoring namespace
            try:
                pods = v1.list_namespaced_pod(
                    namespace="openshift-monitoring",
                    label_selector="app.kubernetes.io/name=node-exporter"
                )
            except ApiException:
                # Try alternative namespace or label selector
                pods = v1.list_namespaced_pod(
                    namespace="openshift-monitoring",
                    label_selector="app=node-exporter"
                )
            
            mapping = {}
            for pod in pods.items:
                pod_name = pod.metadata.name
                node_name = pod.spec.node_name
                if pod_name and node_name:
                    mapping[pod_name] = node_name
                    self.logger.debug(f"Node-exporter mapping: {pod_name} -> {node_name}")
            
            # Update cache
            self._pod_node_cache[cache_key] = mapping
            self._cache_timestamp = current_time
            
            self.logger.info(f"Found {len(mapping)} node-exporter to node mappings")
            return mapping
            
        except Exception as e:
            self.logger.error(f"Error getting node-exporter to node mapping: {e}")
            return {}
    
    async def get_node_for_pod(self, pod_name: str, namespace: str = "openshift-etcd") -> str:
        """Get node name for a specific pod"""
        try:
            # First try etcd pod mapping
            mapping = await self.get_pod_to_node_mapping(namespace)
            if pod_name in mapping:
                return mapping[pod_name]
            
            # If it's a node-exporter pod, use node-exporter mapping
            if "node-exporter" in pod_name:
                node_exporter_mapping = await self.get_node_exporter_to_node_mapping()
                return node_exporter_mapping.get(pod_name, 'unknown')
            
            return mapping.get(pod_name, 'unknown')
            
        except Exception as e:
            self.logger.error(f"Error getting node for pod {pod_name}: {e}")
            return 'unknown'
    
    async def get_nodes_info(self) -> List[Dict[str, Any]]:
        """Get detailed information about cluster nodes"""
        try:
            current_time = asyncio.get_event_loop().time()
            
            # Check cache
            if (self._nodes_cache and 
                current_time - self._cache_timestamp < self._cache_ttl):
                return self._nodes_cache
            
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            v1 = client.CoreV1Api(self.ocp_auth.k8s_client)
            nodes = v1.list_node()
            
            nodes_info = []
            for node in nodes.items:
                node_info = {
                    'name': node.metadata.name,
                    'labels': dict(node.metadata.labels) if node.metadata.labels else {},
                    'annotations': dict(node.metadata.annotations) if node.metadata.annotations else {},
                    'created': node.metadata.creation_timestamp.isoformat() if node.metadata.creation_timestamp else None,
                    'addresses': [],
                    'conditions': [],
                    'capacity': {},
                    'allocatable': {},
                    'node_info': {},
                    'ready': False
                }
                
                # Node addresses
                if node.status.addresses:
                    for addr in node.status.addresses:
                        node_info['addresses'].append({
                            'type': addr.type,
                            'address': addr.address
                        })
                
                # Node conditions
                if node.status.conditions:
                    for condition in node.status.conditions:
                        node_info['conditions'].append({
                            'type': condition.type,
                            'status': condition.status,
                            'reason': condition.reason,
                            'message': condition.message,
                            'last_transition_time': condition.last_transition_time.isoformat() if condition.last_transition_time else None
                        })
                        
                        # Check if node is ready
                        if condition.type == 'Ready' and condition.status == 'True':
                            node_info['ready'] = True
                
                # Resource information
                if node.status.capacity:
                    node_info['capacity'] = dict(node.status.capacity)
                
                if node.status.allocatable:
                    node_info['allocatable'] = dict(node.status.allocatable)
                
                # Node system info
                if node.status.node_info:
                    node_info['node_info'] = {
                        'architecture': node.status.node_info.architecture,
                        'boot_id': node.status.node_info.boot_id,
                        'container_runtime_version': node.status.node_info.container_runtime_version,
                        'kernel_version': node.status.node_info.kernel_version,
                        'kube_proxy_version': node.status.node_info.kube_proxy_version,
                        'kubelet_version': node.status.node_info.kubelet_version,
                        'machine_id': node.status.node_info.machine_id,
                        'operating_system': node.status.node_info.operating_system,
                        'os_image': node.status.node_info.os_image,
                        'system_uuid': node.status.node_info.system_uuid
                    }
                
                nodes_info.append(node_info)
            
            # Update cache
            self._nodes_cache = nodes_info
            self._cache_timestamp = current_time
            
            return nodes_info
            
        except Exception as e:
            self.logger.error(f"Error getting nodes info: {e}")
            return []
    
    async def get_node_by_name(self, node_name: str) -> Optional[Dict[str, Any]]:
        """Get specific node information by name"""
        try:
            nodes = await self.get_nodes_info()
            for node in nodes:
                if node['name'] == node_name:
                    return node
            return None
        except Exception as e:
            self.logger.error(f"Error getting node {node_name}: {e}")
            return None
    
    async def get_etcd_cluster_members(self) -> Dict[str, Any]:
        """Get etcd cluster member information"""
        try:
            # Use OCP auth to execute etcdctl command
            result = await self.ocp_auth.execute_etcd_command("member list")
            
            if 'error' in result:
                return {
                    'status': 'error',
                    'error': result['error']
                }
            
            # Parse etcdctl member list output
            members = []
            lines = result['output'].strip().split('\n')
            
            for line in lines:
                if line.strip() and ',' in line:
                    # Parse member line format: ID, status, name, peer-urls, client-urls, is-learner
                    parts = [part.strip() for part in line.split(',')]
                    if len(parts) >= 5:
                        member = {
                            'id': parts[0],
                            'status': parts[1],
                            'name': parts[2],
                            'peer_urls': parts[3].split() if parts[3] != 'unstarted' else [],
                            'client_urls': parts[4].split() if parts[4] != 'unstarted' else [],
                            'is_learner': parts[5] == 'true' if len(parts) > 5 else False
                        }
                        members.append(member)
            
            return {
                'status': 'success',
                'members': members,
                'total_members': len(members),
                'healthy_members': len([m for m in members if m['status'] == 'started'])
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd cluster members: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_etcd_cluster_health(self) -> Dict[str, Any]:
        """Get etcd cluster health status"""
        try:
            # Execute etcdctl endpoint health command
            result = await self.ocp_auth.execute_etcd_command("endpoint health --cluster")
            
            if 'error' in result:
                return {
                    'status': 'error',
                    'error': result['error']
                }
            
            # Parse health output
            endpoints = []
            lines = result['output'].strip().split('\n')
            
            for line in lines:
                if 'is healthy' in line or 'is unhealthy' in line:
                    # Parse endpoint health line
                    parts = line.split()
                    if len(parts) >= 3:
                        endpoint_url = parts[0].rstrip(':')
                        is_healthy = 'healthy' in line and 'unhealthy' not in line
                        
                        endpoint = {
                            'url': endpoint_url,
                            'healthy': is_healthy,
                            'status': 'healthy' if is_healthy else 'unhealthy'
                        }
                        
                        # Extract timing if present
                        if 'took' in line:
                            timing_part = [p for p in parts if 'took' in p]
                            if timing_part:
                                endpoint['response_time'] = timing_part[0]
                        
                        endpoints.append(endpoint)
            
            healthy_count = len([ep for ep in endpoints if ep['healthy']])
            
            return {
                'status': 'success',
                'endpoints': endpoints,
                'total_endpoints': len(endpoints),
                'healthy_endpoints': healthy_count,
                'cluster_healthy': healthy_count == len(endpoints) and len(endpoints) > 0
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd cluster health: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def _is_pod_ready(self, pod) -> bool:
        """Check if pod is ready"""
        if not pod.status.conditions:
            return False
            
        for condition in pod.status.conditions:
            if condition.type == 'Ready':
                return condition.status == 'True'
        
        return False
    
    async def get_namespace_pods(self, namespace: str, label_selector: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all pods in a namespace with optional label selector"""
        try:
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            v1 = client.CoreV1Api(self.ocp_auth.k8s_client)
            
            kwargs = {'namespace': namespace}
            if label_selector:
                kwargs['label_selector'] = label_selector
                
            pods = v1.list_namespaced_pod(**kwargs)
            
            pod_list = []
            for pod in pods.items:
                pod_info = {
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'phase': pod.status.phase,
                    'node_name': pod.spec.node_name,
                    'pod_ip': pod.status.pod_ip,
                    'labels': dict(pod.metadata.labels) if pod.metadata.labels else {},
                    'ready': self._is_pod_ready(pod)
                }
                pod_list.append(pod_info)
            
            return pod_list
            
        except Exception as e:
            self.logger.error(f"Error getting pods in namespace {namespace}: {e}")
            return []
    
    async def clear_cache(self):
        """Clear all cached data"""
        self._pod_node_cache.clear()
        self._nodes_cache = None
        self._master_nodes_cache = None
        self._cache_timestamp = 0
        self.logger.info("Cache cleared")
    
    def format_resource_value(self, value: str) -> Dict[str, Any]:
        """Format Kubernetes resource values (e.g., memory, CPU) to readable format"""
        try:
            if not value:
                return {'raw': value, 'formatted': 'N/A'}
            
            # Handle memory values (Ki, Mi, Gi)
            if value.endswith('Ki'):
                bytes_val = int(value[:-2]) * 1024
                return {
                    'raw': value,
                    'bytes': bytes_val,
                    'formatted': f"{bytes_val / (1024**2):.2f} MiB"
                }
            elif value.endswith('Mi'):
                bytes_val = int(value[:-2]) * 1024 * 1024
                return {
                    'raw': value,
                    'bytes': bytes_val,
                    'formatted': f"{int(value[:-2])} MiB"
                }
            elif value.endswith('Gi'):
                bytes_val = int(value[:-2]) * 1024 * 1024 * 1024
                return {
                    'raw': value,
                    'bytes': bytes_val,
                    'formatted': f"{int(value[:-2])} GiB"
                }
            
            # Handle CPU values (m = millicores)
            elif value.endswith('m'):
                millicores = int(value[:-1])
                return {
                    'raw': value,
                    'millicores': millicores,
                    'formatted': f"{millicores/1000:.2f} cores"
                }
            
            # Handle plain numbers
            elif value.isdigit():
                return {
                    'raw': value,
                    'numeric': int(value),
                    'formatted': value
                }
            
            # Return as-is for other formats
            return {'raw': value, 'formatted': value}
            
        except Exception as e:
            self.logger.warning(f"Error formatting resource value {value}: {e}")
            return {'raw': value, 'formatted': value}

    async def get_network_interfaces_for_nodes(self) -> Dict[str, List[str]]:
        """Get network interfaces for each node"""
        try:
            if not self.ocp_auth.k8s_client:
                raise RuntimeError("Kubernetes client not initialized")
                
            # This would require node-exporter metrics or direct node access
            # For now, return a mapping based on common interface patterns
            nodes = await self.get_master_nodes()
            
            interface_mapping = {}
            for node in nodes:
                # Common interface patterns in OpenShift/Kubernetes nodes
                interface_mapping[node] = [
                    'eth0', 'ens3', 'ens4', 'bond0', 'br-ex', 'ovs-system'
                ]
            
            return interface_mapping
            
        except Exception as e:
            self.logger.error(f"Error getting network interfaces: {e}")
            return {}
        
    async def resolve_node_from_instance(self, instance: str) -> str:
        """Resolve node name from Prometheus instance label"""
        try:
            # Instance format can be: IP:port, hostname:port, or just hostname
            if ':' in instance:
                host_part = instance.split(':')[0]
            else:
                host_part = instance
            
            # If it's an IP, try to map it to a node name
            if self._is_ip_address(host_part):
                nodes_info = await self.get_nodes_info()
                for node in nodes_info:
                    for addr in node.get('addresses', []):
                        if addr['address'] == host_part:
                            return node['name']
            
            # If it's already a hostname, try to match against node names
            nodes_info = await self.get_nodes_info()
            for node in nodes_info:
                if node['name'] == host_part or host_part in node['name']:
                    return node['name']
            
            # Return as-is if no mapping found
            return host_part
            
        except Exception as e:
            self.logger.warning(f"Error resolving instance {instance} to node: {e}")
            return instance
    
    def _is_ip_address(self, address: str) -> bool:
        """Check if a string is an IP address"""
        try:
            import ipaddress
            ipaddress.ip_address(address)
            return True
        except ValueError:
            return False
    
    async def get_etcd_network_topology(self) -> Dict[str, Any]:
        """Get etcd network topology information"""
        try:
            etcd_pods = await self.get_etcd_pods()
            master_nodes = await self.get_master_nodes()
            
            topology = {
                'etcd_pods': len(etcd_pods),
                'master_nodes': len(master_nodes),
                'pod_distribution': {},
                'network_segments': []
            }
            
            # Map pods to nodes
            for pod in etcd_pods:
                node = pod.get('node_name', 'unknown')
                if node not in topology['pod_distribution']:
                    topology['pod_distribution'][node] = []
                topology['pod_distribution'][node].append({
                    'pod_name': pod['name'],
                    'pod_ip': pod.get('pod_ip'),
                    'host_ip': pod.get('host_ip')
                })
            
            # Identify potential network segments based on IP ranges
            for node, pods in topology['pod_distribution'].items():
                for pod_info in pods:
                    if pod_info['pod_ip']:
                        # Extract network segment (first 3 octets for /24)
                        try:
                            ip_parts = pod_info['pod_ip'].split('.')
                            if len(ip_parts) == 4:
                                segment = '.'.join(ip_parts[:3]) + '.0/24'
                                if segment not in topology['network_segments']:
                                    topology['network_segments'].append(segment)
                        except:
                            pass
            
            return topology
            
        except Exception as e:
            self.logger.error(f"Error getting etcd network topology: {e}")
            return {
                'error': str(e),
                'etcd_pods': 0,
                'master_nodes': 0
            }