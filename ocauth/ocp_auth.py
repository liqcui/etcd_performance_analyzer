"""
OpenShift Authentication Module
Handles authentication and service discovery for OpenShift clusters
"""

import os
import base64
import logging
import warnings
from typing import Optional, Dict, Any, Tuple
import shlex
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import asyncio

# Suppress urllib3 SSL warnings for self-signed certificates
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class OCPAuth:
    """OpenShift authentication and service discovery"""
    
    def __init__(self):
        self.k8s_client = None
        self.prometheus_url = None
        self.token = None
        self.ca_cert_path = None
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self) -> bool:
        """Initialize Kubernetes client and discover services"""
        try:
            # Load kubeconfig
            config.load_kube_config()
            self.k8s_client = client.ApiClient()
            
            # Get authentication details
            await self._get_auth_details()
            
            # Discover Prometheus service
            prometheus_info = await self._discover_prometheus()
            if prometheus_info:
                self.prometheus_url = prometheus_info['url']
                self.logger.info(f"Discovered Prometheus at: {self.prometheus_url}")
                return True
            else:
                self.logger.error("Failed to discover Prometheus service")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to initialize OCP authentication: {e}")
            return False
    
    async def _get_auth_details(self):
        """Extract authentication details from kubeconfig"""
        try:
            # Get current context and token
            contexts, active_context = config.list_kube_config_contexts()
            if active_context:
                context_name = active_context['name']
                self.logger.info(f"Using context: {context_name}")
                
                # Load configuration to get token
                configuration = client.Configuration()
                config.load_kube_config(client_configuration=configuration)
                
                # Get token from configuration
                if hasattr(configuration, 'api_key') and configuration.api_key:
                    token = configuration.api_key.get('authorization', '')
                    if token.startswith('Bearer '):
                        self.token = token[7:]  # Remove 'Bearer ' prefix
                    else:
                        self.token = token
                
                # Set CA cert path if available
                if hasattr(configuration, 'ssl_ca_cert') and configuration.ssl_ca_cert:
                    self.ca_cert_path = configuration.ssl_ca_cert
                    
        except Exception as e:
            self.logger.warning(f"Could not extract auth details: {e}")
    
    async def _discover_prometheus(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus service in OpenShift monitoring namespace - routes first, then services"""
        try:
            # Common monitoring namespaces to check
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring',
                'monitoring',
                'kube-system'
            ]
            
            # First, try to find Prometheus through OpenShift routes (preferred)
            for namespace in monitoring_namespaces:
                try:
                    route_info = await self._find_prometheus_route(namespace)
                    if route_info:
                        self.logger.info(f"Found Prometheus route in namespace: {namespace}")
                        return {
                            'url': route_info['url'],
                            'namespace': namespace,
                            'access_method': 'route',
                            'route_host': route_info['host'],
                            'tls_enabled': route_info['tls_enabled']
                        }
                except Exception as e:
                    self.logger.debug(f"No route found in namespace {namespace}: {e}")
                    continue
            
            # If no routes found, fall back to service discovery
            self.logger.info("No Prometheus routes found, trying service discovery...")
            return await self._discover_prometheus_service()
            
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus: {e}")
            return None
    
    async def _find_prometheus_route(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Find OpenShift route for Prometheus"""
        try:
            # Use dynamic client for routes (OpenShift specific)
            from kubernetes import dynamic
            from kubernetes.client import api_client
            
            dyn_client = dynamic.DynamicClient(api_client.ApiClient())
            route_api = dyn_client.resources.get(api_version='route.openshift.io/v1', kind='Route')
            
            # Try different label selectors for Prometheus routes
            label_selectors = [
                "app.kubernetes.io/name=prometheus",
                "app=prometheus",
                "component=prometheus"
            ]
            
            for label_selector in label_selectors:
                try:
                    routes = route_api.get(namespace=namespace, label_selector=label_selector)
                    
                    if routes.items:
                        route = routes.items[0]
                        host = route.spec.host
                        tls = route.spec.get('tls')
                        tls_enabled = bool(tls)
                        scheme = 'https' if tls_enabled else 'http'
                        
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': route.metadata.name
                        }
                except Exception as e:
                    self.logger.debug(f"No routes found with selector '{label_selector}' in {namespace}: {e}")
                    continue
            
            # Also try to find routes by name patterns
            try:
                all_routes = route_api.get(namespace=namespace)
                for route in all_routes.items:
                    route_name = route.metadata.name.lower()
                    if any(name in route_name for name in ['prometheus', 'monitoring']):
                        host = route.spec.host
                        tls = route.spec.get('tls')
                        tls_enabled = bool(tls)
                        scheme = 'https' if tls_enabled else 'http'
                        
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': route.metadata.name
                        }
            except Exception as e:
                self.logger.debug(f"Error checking route names in {namespace}: {e}")
                
        except Exception as e:
            self.logger.debug(f"Could not find route in namespace {namespace}: {e}")
        
        return None
    
    async def _discover_prometheus_service(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus through service discovery"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            # Common monitoring namespaces to check
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring',
                'monitoring',
                'kube-system'
            ]
            
            for namespace in monitoring_namespaces:
                try:
                    # Look for Prometheus service
                    services = v1.list_namespaced_service(
                        namespace=namespace,
                        label_selector="app.kubernetes.io/name=prometheus"
                    )
                    
                    if not services.items:
                        # Try alternative labels
                        services = v1.list_namespaced_service(
                            namespace=namespace,
                            label_selector="app=prometheus"
                        )
                    
                    if services.items:
                        service = services.items[0]
                        service_name = service.metadata.name
                        
                        # Get service details
                        port = None
                        for svc_port in service.spec.ports:
                            if svc_port.name in ['web', 'http', 'prometheus'] or svc_port.port in [9090, 9091]:
                                port = svc_port.port
                                break
                        
                        if not port and service.spec.ports:
                            port = service.spec.ports[0].port
                        
                        # Build Prometheus URL based on service type
                        prometheus_url = None
                        access_method = None
                        
                        if service.spec.type == 'LoadBalancer' and service.status.load_balancer.ingress:
                            host = service.status.load_balancer.ingress[0].ip or service.status.load_balancer.ingress[0].hostname
                            prometheus_url = f"http://{host}:{port}"
                            access_method = 'loadbalancer'
                        elif service.spec.type == 'NodePort':
                            # Get any node IP
                            nodes = v1.list_node()
                            if nodes.items:
                                node_ip = None
                                for address in nodes.items[0].status.addresses:
                                    if address.type in ['ExternalIP', 'InternalIP']:
                                        node_ip = address.address
                                        break
                                if node_ip:
                                    prometheus_url = f"http://{node_ip}:{service.spec.ports[0].node_port}"
                                    access_method = 'nodeport'
                        
                        if not prometheus_url:
                            # Use cluster internal URL as fallback
                            prometheus_url = f"http://{service_name}.{namespace}.svc.cluster.local:{port}"
                            access_method = 'cluster_internal'
                        
                        self.logger.info(f"Found Prometheus service in namespace: {namespace}")
                        return {
                            'url': prometheus_url,
                            'namespace': namespace,
                            'service_name': service_name,
                            'port': port,
                            'access_method': access_method
                        }
                        
                except ApiException as e:
                    if e.status != 404:  # Ignore namespace not found errors
                        self.logger.warning(f"Error checking namespace {namespace}: {e}")
                    continue
            
            # If no service found, try to find Prometheus pods directly
            self.logger.info("No Prometheus services found, trying pod discovery...")
            return await self._discover_prometheus_pods()
            
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus services: {e}")
            return None
    
    async def _discover_prometheus_pods(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus through pods if service discovery fails"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring', 
                'monitoring'
            ]
            
            for namespace in monitoring_namespaces:
                try:
                    pods = v1.list_namespaced_pod(
                        namespace=namespace,
                        label_selector="app.kubernetes.io/name=prometheus"
                    )
                    
                    if not pods.items:
                        # Try alternative labels
                        pods = v1.list_namespaced_pod(
                            namespace=namespace,
                            label_selector="app=prometheus"
                        )
                    
                    if pods.items:
                        pod = pods.items[0]
                        pod_ip = pod.status.pod_ip
                        
                        # Default Prometheus port
                        port = 9090
                        
                        # Try to get port from container
                        for container in pod.spec.containers:
                            if container.ports:
                                for container_port in container.ports:
                                    if container_port.name in ['web', 'http', 'prometheus']:
                                        port = container_port.container_port
                                        break
                        
                        prometheus_url = f"http://{pod_ip}:{port}"
                        
                        self.logger.info(f"Found Prometheus pod in namespace: {namespace}")
                        return {
                            'url': prometheus_url,
                            'namespace': namespace,
                            'pod_name': pod.metadata.name,
                            'port': port,
                            'access_method': 'direct_pod'
                        }
                        
                except ApiException as e:
                    if e.status != 404:
                        self.logger.warning(f"Error checking pods in namespace {namespace}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus pods: {e}")
        
        return None
    
    async def get_etcd_endpoints(self) -> Dict[str, Any]:
        """Get etcd endpoints from the cluster"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            # Look for etcd pods in openshift-etcd namespace
            etcd_namespace = "openshift-etcd"
            pods = v1.list_namespaced_pod(
                namespace=etcd_namespace,
                label_selector="app=etcd"
            )
            
            endpoints = []
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_ip = pod.status.pod_ip
                    endpoints.append({
                        'name': pod.metadata.name,
                        'ip': pod_ip,
                        'endpoint': f"{pod_ip}:2379"
                    })
            
            return {
                'namespace': etcd_namespace,
                'endpoints': endpoints,
                'total_members': len(endpoints)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd endpoints: {e}")
            return {'error': str(e)}
    
    async def execute_etcd_command(self, command: str) -> Dict[str, Any]:
        """Execute etcdctl command in etcd pod"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            # Find first running etcd pod
            etcd_namespace = "openshift-etcd"
            pods = v1.list_namespaced_pod(
                namespace=etcd_namespace,
                label_selector="app=etcd"
            )
            
            if not pods.items:
                return {'error': 'No etcd pods found'}
            
            # Use first running pod
            pod_name = None
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_name = pod.metadata.name
                    break
            
            if not pod_name:
                return {'error': 'No running etcd pods found'}
            
            # Determine container name to exec into (prefer 'etcdctl' then 'etcd')
            container_name = None
            try:
                for c in pods.items[0].spec.containers:
                    if c.name == "etcdctl":
                        container_name = c.name
                        break
                if container_name is None:
                    for c in pods.items[0].spec.containers:
                        if c.name == "etcd":
                            container_name = c.name
                            break
                if container_name is None and pods.items[0].spec.containers:
                    container_name = pods.items[0].spec.containers[0].name
            except Exception:
                pass

            # Execute command in pod
            from kubernetes.stream import stream
            
            base_cmd = [
                'etcdctl',
                '--cacert=/etc/etcd/tls/etcd-ca/ca.crt',
                '--cert=/etc/etcd/tls/etcd-peer/peer.crt', 
                '--key=/etc/etcd/tls/etcd-peer/peer.key',
                '--endpoints=https://localhost:2379'
            ] + command.split()

            # Build a shell-wrapped command to unset conflicting env vars and force ETCDCTL_API=3
            unsafe_envs = [
                'ETCDCTL_KEY','ETCDCTL_CERT','ETCDCTL_CACERT','ETCDCTL_ENDPOINTS','ETCDCTL_USER',
                'ETCDCTL_PASSWORD','ETCDCTL_TOKEN','ETCDCTL_INSECURE_SKIP_TLS_VERIFY'
            ]
            unset_parts = ' '.join([f"-u {name}" for name in unsafe_envs])
            cmd_str = ' '.join(shlex.quote(p) for p in base_cmd)
            shell_cmd = f"env {unset_parts} ETCDCTL_API=3 {cmd_str}"

            exec_command = ['/bin/sh', '-c', shell_cmd]

            resp = stream(
                v1.connect_get_namespaced_pod_exec,
                pod_name,
                etcd_namespace,
                command=exec_command,
                container=container_name,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False
            )
            
            return {
                'pod_name': pod_name,
                'command': ' '.join(exec_command),
                'output': resp
            }
            
        except Exception as e:
            self.logger.error(f"Error executing etcd command: {e}")
            return {'error': str(e)}
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests"""
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers
    
    def get_prometheus_config(self) -> Dict[str, Any]:
        """Get Prometheus connection configuration"""
        return {
            'url': self.prometheus_url,
            'headers': self.get_auth_headers(),
            'verify': self.ca_cert_path if self.ca_cert_path else False
        }