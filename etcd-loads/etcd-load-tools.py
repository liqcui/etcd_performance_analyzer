#!/usr/bin/env python3
"""
OpenShift Resource Creation and Testing Script
Async Python version using Kubernetes client library with asyncio for improved performance
"""

import argparse
import asyncio
import base64
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import re

# Kubernetes imports
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import yaml

# For certificate generation
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime as dt


class Colors:
    """ANSI color codes for terminal output"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class Config:
    """Configuration class with default values"""
    def __init__(self):
        # Environment variable defaults
        self.max_namespaces = int(os.getenv('MAX_NAMESPACES', '100'))
        self.max_images = int(os.getenv('MAX_IMAGES', '100'))
        self.multi_images_ns = os.getenv('MULTI_IMAGES_NS', 'multi-image')
        self.images_per_ns = int(os.getenv('IMAGES_PER_NS', '10'))
        self.images_max_ns = int(os.getenv('IMAGES_MAX_NS', '10'))
        
        self.secrets_per_ns = int(os.getenv('SECRETS_PER_NS', '100'))
        self.secrets_max_ns = int(os.getenv('SECRETS_MAX_NS', '100'))
        self.secrets_ns = os.getenv('SECRETS_NS', 'project-sec')
        
        self.large_secrets_per_ns = int(os.getenv('LARGE_SECRETS_PER_NS', '10'))
        self.large_secrets_max_ns = int(os.getenv('LARGE_SECRETS_NAX_NS', '100'))
        self.large_secrets_ns = os.getenv('LARGE_SECRETS_NS', 'project-lsec')
        
        self.configmap_size_gb = int(os.getenv('CONFIGMAP_SIZE_GB', '1'))
        self.configmap_namespace = os.getenv('CONFIGMAP_NAMESPACE', 'project-cfg')
        self.configmap_payload_size = int(os.getenv('CONFIGMAP_PAYLOAD_SIZE', '2'))
        self.configmaps_per_ns = int(os.getenv('CONFIGMAPS_PER_NS', '20'))
        
        self.batch_size = int(os.getenv('BATCH_SIZE', '50'))
        self.etcd_namespace = os.getenv('ETCD_NAMESPACE', 'openshift-etcd')
        self.namespace_parallel = int(os.getenv('NAMESPACE_PARALLEL', '10'))
        
        self.log_file = os.getenv('LOG_FILE', 'resource_creation.log')
        self.cleanup_target_namespace = os.getenv('CLEANUP_TARGET_NAMESPACE', 'project|multi-image')


class EtcdLoadToolsAsync:
    """Main class for OpenShift resource creation and testing with async support"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        self.setup_kubernetes_clients()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config.log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_kubernetes_clients(self):
        """Setup Kubernetes client configuration"""
        try:
            # Try to load in-cluster config first
            config.load_incluster_config()
            self.log_info("Using in-cluster Kubernetes configuration")
        except config.ConfigException:
            try:
                # Fall back to kubeconfig
                config.load_kube_config()
                self.log_info("Using kubeconfig file")
            except config.ConfigException as e:
                self.log_error(f"Failed to load Kubernetes configuration: {e}")
                sys.exit(1)
                
        # Create API clients
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_objects = client.CustomObjectsApi()
        
        # Create async-compatible configuration
        self.api_config = client.Configuration.get_default_copy()
        
    def log_info(self, message: str):
        """Log info message with colors"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {timestamp} - {message}")
        self.logger.info(message)
        
    def log_warn(self, message: str):
        """Log warning message with colors"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{Colors.YELLOW}[WARN]{Colors.NC} {timestamp} - {message}")
        self.logger.warning(message)
        
    def log_error(self, message: str):
        """Log error message with colors"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{Colors.RED}[ERROR]{Colors.NC} {timestamp} - {message}")
        self.logger.error(message)
        
    def log_debug(self, message: str):
        """Log debug message with colors"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{Colors.BLUE}[DEBUG]{Colors.NC} {timestamp} - {message}")
        self.logger.debug(message)
        
    async def run_subprocess(self, cmd: List[str], capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run subprocess asynchronously"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            future = loop.run_in_executor(
                executor,
                lambda: subprocess.run(cmd, capture_output=capture_output, text=True)
            )
            return await future
            
    async def check_prerequisites(self):
        """Check if required tools and API access are available"""
        self.log_info("Checking prerequisites...")
        
        # Test Kubernetes API connectivity
        try:
            await asyncio.to_thread(self.core_v1.list_namespace, limit=1)
        except ApiException as e:
            self.log_error(f"Failed to connect to Kubernetes API: {e}")
            return False
            
        # Check external tools
        tools = ['ssh-keygen', 'openssl', 'git']
        for tool in tools:
            try:
                await self.run_subprocess(['which', tool])
            except subprocess.CalledProcessError:
                self.log_warn(f"{tool} command not found - some features may not work")
                
        self.log_info("Prerequisites check completed")
        return True
        
    async def check_etcd_health(self):
        """Check etcd endpoint health"""
        self.log_info("Checking etcd endpoint health...")
        
        try:
            pods = await asyncio.to_thread(
                self.core_v1.list_namespaced_pod,
                namespace=self.config.etcd_namespace,
                label_selector="app=etcd"
            )
            
            if not pods.items:
                self.log_warn("No etcd pods found with 'app=etcd' label")
                return False
                
            health_tasks = []
            for pod in pods.items:
                self.log_debug(f"Checking health for pod: {pod.metadata.name}")
                task = self.check_pod_health(pod.metadata.name)
                health_tasks.append(task)
                
            results = await asyncio.gather(*health_tasks, return_exceptions=True)
            
            healthy_count = sum(1 for result in results if result is True)
            self.log_info(f"Etcd health check completed: {healthy_count}/{len(pods.items)} pods healthy")
            
        except ApiException as e:
            self.log_warn(f"Failed to get etcd pods: {e}")
            return False
            
        return True
        
    async def check_pod_health(self, pod_name: str) -> bool:
        """Check health of a specific etcd pod"""
        try:
            # Use oc exec for health check - this could be improved with native K8s exec
            result = await self.run_subprocess([
                'oc', 'exec', '-n', self.config.etcd_namespace, pod_name,
                '--', 'etcdctl', 'endpoint', 'health'
            ])
            
            if result.returncode == 0:
                self.log_info(f"Pod {pod_name} is healthy")
                return True
            else:
                self.log_warn(f"Pod {pod_name} health check failed")
                return False
                
        except Exception as e:
            self.log_warn(f"Failed to check health for pod {pod_name}: {e}")
            return False
            
    async def ensure_namespace(self, namespace_name: str) -> bool:
        """Ensure a namespace exists, create if it doesn't"""
        try:
            await asyncio.to_thread(
                self.core_v1.read_namespace,
                name=namespace_name
            )
            return True
        except ApiException as e:
            if e.status == 404:  # Not found
                try:
                    namespace_body = client.V1Namespace(
                        metadata=client.V1ObjectMeta(name=namespace_name)
                    )
                    await asyncio.to_thread(
                        self.core_v1.create_namespace,
                        body=namespace_body
                    )
                    self.log_debug(f"Created namespace: {namespace_name}")
                    return True
                except ApiException as create_e:
                    if create_e.status == 409:  # Already exists
                        return True
                    self.log_error(f"Failed to create namespace {namespace_name}: {create_e}")
                    return False
            else:
                self.log_error(f"Failed to check namespace {namespace_name}: {e}")
                return False
                
    async def create_configmap(self, namespace: str, name: str, data: Dict[str, str]) -> bool:
        """Create a ConfigMap"""
        try:
            configmap_body = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                data=data
            )
            await asyncio.to_thread(
                self.core_v1.create_namespaced_config_map,
                namespace=namespace,
                body=configmap_body
            )
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.log_debug(f"ConfigMap {name} already exists in {namespace}")
                return True
            self.log_warn(f"Failed to create ConfigMap {name} in {namespace}: {e}")
            return False
            
    async def create_configmap_binary(self, namespace: str, name: str, binary_data: Dict[str, bytes]) -> bool:
        """Create a ConfigMap with binary data"""
        try:
            configmap_body = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                binary_data=binary_data
            )
            await asyncio.to_thread(
                self.core_v1.create_namespaced_config_map,
                namespace=namespace,
                body=configmap_body
            )
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.log_debug(f"ConfigMap {name} already exists in {namespace}")
                return True
            self.log_warn(f"Failed to create ConfigMap {name} in {namespace}: {e}")
            return False
            
    async def create_secret(self, namespace: str, name: str, data: Dict[str, str], secret_type: str = "Opaque") -> bool:
        """Create a Secret"""
        try:
            secret_body = client.V1Secret(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                type=secret_type,
                string_data=data
            )
            await asyncio.to_thread(
                self.core_v1.create_namespaced_secret,
                namespace=namespace,
                body=secret_body
            )
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.log_debug(f"Secret {name} already exists in {namespace}")
                return True
            self.log_warn(f"Failed to create Secret {name} in {namespace}: {e}")
            return False
            
    async def create_openshift_image(self, namespace: str, name: str) -> bool:
        """Create an OpenShift Image object using custom API"""
        try:
            image_body = {
                "apiVersion": "image.openshift.io/v1",
                "kind": "Image",
                "metadata": {
                    "name": name,
                    "namespace": namespace
                },
                "dockerImageReference": "registry.redhat.io/ubi8/ruby-27:latest",
                "dockerImageMetadata": {
                    "kind": "DockerImage",
                    "apiVersion": "1.0",
                    "Id": "",
                    "ContainerConfig": {},
                    "Config": {}
                },
                "dockerImageLayers": [],
                "dockerImageMetadataVersion": "1.0"
            }
            
            await asyncio.to_thread(
                self.custom_objects.create_namespaced_custom_object,
                group="image.openshift.io",
                version="v1",
                namespace=namespace,
                plural="images",
                body=image_body
            )
            return True
            
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.log_debug(f"Image {name} already exists in {namespace}")
                return True
            self.log_warn(f"Failed to create Image {name} in {namespace}: {e}")
            return False
            
    async def create_namespace_with_configmaps(self, namespace_name: str):
        """Create a namespace with initial configmaps"""
        # Ensure namespace exists
        if not await self.ensure_namespace(namespace_name):
            return
            
        # Create 10 initial configmaps concurrently
        tasks = []
        for i in range(1, 11):
            cm_name = f"init-cm-{i}"
            data = {"dummy-key": f"dummy-value-{i}"}
            task = self.create_configmap(namespace_name, cm_name, data)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful = sum(1 for result in results if result is True)
        self.log_debug(f"Created {successful}/10 ConfigMaps in {namespace_name}")
        
    async def create_namespaces(self, max_namespaces: int):
        """Create namespaces with configmaps using async concurrency"""
        self.log_info(f"Creating {max_namespaces} namespaces with ConfigMaps (concurrency: {self.config.namespace_parallel})")
        
        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(self.config.namespace_parallel)
        
        async def create_with_semaphore(ns_idx: int):
            async with semaphore:
                namespace_name = f"project-{ns_idx}"
                await self.create_namespace_with_configmaps(namespace_name)
                return ns_idx
                
        # Create all namespace tasks
        tasks = [create_with_semaphore(i) for i in range(1, max_namespaces + 1)]
        
        # Process in batches for progress reporting
        batch_size = self.config.namespace_parallel
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*batch, return_exceptions=True)
            
            completed = i + len(batch)
            self.log_info(f"Processed {min(completed, max_namespaces)}/{max_namespaces} namespaces")
            
        self.log_info("Namespace creation completed")
        await self.check_etcd_health()
        
    async def create_configmaps(self, size_gb: int, ns_prefix: str):
        """Create large ConfigMaps for etcd load testing"""
        # Calculate sizes
        cm_blocks = self.config.configmap_payload_size
        if cm_blocks <= 0:
            cm_blocks = 1
            
        computed_bytes = 512 * 1024 * cm_blocks
        if computed_bytes > 1500 * 1024:
            self.log_warn(f"ConfigMap payload ~{computed_bytes} bytes may exceed API limits; capping to 512KiB")
            cm_blocks = 1
            computed_bytes = 512 * 1024
            
        per_cm_bytes = computed_bytes
        total_size_bytes = size_gb * 1024 * 1024 * 1024
        num_objects = (total_size_bytes + per_cm_bytes - 1) // per_cm_bytes
        per_ns = self.config.configmaps_per_ns if self.config.configmaps_per_ns > 0 else 10
        num_namespaces = (num_objects + per_ns - 1) // per_ns
        
        self.log_info(f"Creating ConfigMaps: target=~{size_gb}GB, per_cm={per_cm_bytes//1024}KiB, "
                     f"total_objects={num_objects}, per_namespace={per_ns}, namespaces={num_namespaces}")
        
        self.log_warn(f"This will create {num_objects} ConfigMaps across {num_namespaces} namespaces. "
                     "Ensure this is not a production cluster!")
        
        # Safety pause
        self.log_info("Starting in 5 seconds... Press Ctrl+C to cancel")
        await asyncio.sleep(5)
        
        # Create semaphore for batch processing
        semaphore = asyncio.Semaphore(self.config.batch_size)
        
        async def create_large_configmap_with_semaphore(namespace: str, name: str, size_bytes: int):
            async with semaphore:
                # Generate random data
                data = secrets.token_bytes(size_bytes)
                return await self.create_configmap_binary(namespace, name, {"data": data})
                
        created_total = 0
        idx = 1
        
        for ns_idx in range(1, num_namespaces + 1):
            ns_name = f"{ns_prefix}-{ns_idx}"
            
            # Ensure namespace exists
            if not await self.ensure_namespace(ns_name):
                continue
                
            remaining = num_objects - created_total
            to_create = min(per_ns, remaining)
            
            self.log_info(f"Creating {to_create} ConfigMaps in {ns_name} (remaining: {remaining})")
            
            # Create ConfigMaps for this namespace
            tasks = []
            for cm in range(to_create):
                cm_name = f"etcd-load-test-cm-{idx + cm}"
                task = create_large_configmap_with_semaphore(ns_name, cm_name, per_cm_bytes)
                tasks.append(task)
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for result in results if result is True)
            
            created_total += successful
            idx += to_create
            self.log_info(f"Namespace {ns_name} complete ({created_total}/{num_objects} objects)")
            
        self.log_info(f"ConfigMap creation completed: {created_total}/{num_objects} objects")
        await self.check_etcd_health()
        
    async def create_images(self, per_ns: int, max_ns: int, ns_prefix: str):
        """Create multiple OpenShift images"""
        self.log_info(f"Creating {per_ns} images per namespace across {max_ns} namespaces (prefix: {ns_prefix})")
        
        # Create semaphore for batch processing
        semaphore = asyncio.Semaphore(self.config.batch_size)
        
        async def create_image_with_semaphore(namespace: str, name: str):
            async with semaphore:
                return await self.create_openshift_image(namespace, name)
                
        for ns_idx in range(1, max_ns + 1):
            ns_name = f"{ns_prefix}-{ns_idx}"
            
            # Ensure namespace exists
            if not await self.ensure_namespace(ns_name):
                continue
                
            # Create images for this namespace
            tasks = []
            for i in range(1, per_ns + 1):
                img_name = f"testImage-{ns_idx}-{i}"
                task = create_image_with_semaphore(ns_name, img_name)
                tasks.append(task)
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for result in results if result is True)
            
            self.log_info(f"Completed images in namespace {ns_name}: {successful}/{per_ns} ({ns_idx}/{max_ns})")
            
        self.log_info("Image creation completed")
        await self.check_etcd_health()
        
    async def create_secrets(self, max_secrets: int, max_namespaces: int, sec_namespace: str):
        """Create secrets in multiple namespaces"""
        self.log_info(f"Creating {max_secrets} secrets per namespace across {max_namespaces} namespaces")
        
        # Create semaphore for batch processing
        semaphore = asyncio.Semaphore(self.config.batch_size)
        
        async def create_small_secret_with_semaphore(namespace: str, name: str):
            async with semaphore:
                data = {
                    "key1": "supersecret",
                    "key2": "topsecret"
                }
                return await self.create_secret(namespace, name, data)
                
        for i in range(1, max_namespaces + 1):
            namespace = f"{sec_namespace}-{i}"
            
            # Ensure namespace exists
            if not await self.ensure_namespace(namespace):
                continue
                
            # Create secrets for this namespace
            tasks = []
            for j in range(1, max_secrets + 1):
                sec_name = f"small-secret-{j}"
                task = create_small_secret_with_semaphore(namespace, sec_name)
                tasks.append(task)
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for result in results if result is True)
            
            self.log_info(f"Completed secrets for namespace {namespace}: {successful}/{max_secrets} ({i}/{max_namespaces})")
            
        self.log_info("Secret creation completed")
        
    async def generate_ssh_keypair(self) -> tuple[str, str]:
        """Generate SSH keypair asynchronously"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            future = loop.run_in_executor(executor, self._generate_ssh_keypair_sync)
            return await future
            
    def _generate_ssh_keypair_sync(self) -> tuple[str, str]:
        """Synchronous SSH keypair generation"""
        # Generate RSA private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        
        # Serialize private key
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Generate public key in SSH format
        public_key = private_key.public_key()
        public_ssh = public_key.public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH
        )
        
        return (
            base64.b64encode(private_pem).decode('utf-8'),
            base64.b64encode(public_ssh).decode('utf-8')
        )
        
    async def generate_self_signed_cert(self) -> tuple[str, str]:
        """Generate self-signed certificate asynchronously"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            future = loop.run_in_executor(executor, self._generate_cert_sync)
            return await future
            
    def _generate_cert_sync(self) -> tuple[str, str]:
        """Synchronous certificate generation"""
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        
        # Create certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "mydomain.com"),
        ])
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            dt.datetime.utcnow()
        ).not_valid_after(
            dt.datetime.utcnow() + dt.timedelta(days=365)
        ).sign(private_key, hashes.SHA256())
        
        # Serialize certificate and key
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return (
            base64.b64encode(cert_pem).decode('utf-8'),
            base64.b64encode(key_pem).decode('utf-8')
        )
        
    async def create_large_secrets(self, max_secrets: int, max_namespaces: int, sec_namespace: str):
        """Create large secrets with certificates and keys"""
        self.log_info(f"Creating {max_secrets} large secrets per namespace across {max_namespaces} namespaces")
        
        # Generate materials once
        self.log_info("Generating SSH keypair...")
        ssh_private_key, ssh_public_key = await self.generate_ssh_keypair()
        
        self.log_info("Generating token...")
        token_value = base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
        
        self.log_info("Generating self-signed certificate...")
        certificate, private_key = await self.generate_self_signed_cert()
        
        # Create semaphore for batch processing
        semaphore = asyncio.Semaphore(self.config.batch_size)
        
        async def create_large_secret_with_semaphore(namespace: str, name: str):
            async with semaphore:
                data = {
                    "ssh-private-key": ssh_private_key,
                    "ssh-public-key": ssh_public_key,
                    "token": token_value,
                    "tls.crt": certificate,
                    "tls.key": private_key
                }
                return await self.create_secret(namespace, name, data)
                
        for ns_idx in range(1, max_namespaces + 1):
            namespace = f"{sec_namespace}-{ns_idx}"
            
            # Ensure namespace exists
            if not await self.ensure_namespace(namespace):
                continue
                
            # Create large secrets for this namespace
            tasks = []
            for i in range(1, max_secrets + 1):
                sec_name = f"large-secret-{i}"
                task = create_large_secret_with_semaphore(namespace, sec_name)
                tasks.append(task)
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for result in results if result is True)
            
            self.log_info(f"Completed large secrets for namespace {namespace}: {successful}/{max_secrets} ({ns_idx}/{max_namespaces})")
            
        self.log_info("Large secret creation completed")
        await self.check_etcd_health()
        
    async def cleanup_project_namespace(self, ns_prefix: Optional[str] = None):
        """Cleanup namespaces with given prefix"""
        ns_prefix = ns_prefix or self.config.cleanup_target_namespace
        if not ns_prefix:
            return
            
        try:
            # List all namespaces
            namespaces = await asyncio.to_thread(self.core_v1.list_namespace)
            
            # Find matching namespaces
            pattern = re.compile(f'^{ns_prefix}')
            matching_namespaces = [
                ns.metadata.name for ns in namespaces.items 
                if pattern.match(ns.metadata.name)
            ]
            
            self.log_info(f"Found {len(matching_namespaces)} namespaces with prefix {ns_prefix}")
            
            if not matching_namespaces:
                self.log_info(f"No namespaces to clean for prefix {ns_prefix}")
                return
                
            # Delete namespaces
            for ns_name in matching_namespaces:
                self.log_info(f"Deleting namespace: {ns_name}")
                try:
                    await asyncio.to_thread(
                        self.core_v1.delete_namespace,
                        name=ns_name,
                        grace_period_seconds=0
                    )
                    
                    # Wait for termination
                    self.log_info(f"Waiting for namespace {ns_name} to terminate...")
                    for _ in range(120):  # Wait up to 4 minutes
                        try:
                            await asyncio.to_thread(
                                self.core_v1.read_namespace,
                                name=ns_name
                            )
                            await asyncio.sleep(2)
                        except ApiException as e:
                            if e.status == 404:  # Not found
                                self.log_info(f"Namespace {ns_name} deleted")
                                break
                            
                except ApiException as e:
                    self.log_warn(f"Failed to delete namespace {ns_name}: {e}")
                    
        except ApiException as e:
            self.log_warn(f"Failed to list namespaces for cleanup: {e}")
            
    async def clean_images(self):
        """Clean up test images"""
        try:
            # List all images using custom objects API
            images = await asyncio.to_thread(
                self.custom_objects.list_cluster_custom_object,
                group="image.openshift.io",
                version="v1",
                plural="images"
            )
            
            delete_tasks = []
            for image in images.get('items', []):
                name = image.get('metadata', {}).get('name', '')
                if 'testImage' in name:
                    task = self._delete_image(name)
                    delete_tasks.append(task)
                    
            if delete_tasks:
                results = await asyncio.gather(*delete_tasks, return_exceptions=True)
                successful = sum(1 for result in results if result is True)
                self.log_info(f"Cleaned up {successful}/{len(delete_tasks)} test images")
                
        except ApiException as e:
            self.log_warn(f"Failed to list images for cleanup: {e}")
            
    async def _delete_image(self, name: str) -> bool:
        """Delete a single image"""
        try:
            await asyncio.to_thread(
                self.custom_objects.delete_cluster_custom_object,
                group="image.openshift.io",
                version="v1",
                plural="images",
                name=name
            )
            self.log_debug(f"Deleted image: {name}")
            return True
        except ApiException as e:
            self.log_warn(f"Failed to delete image {name}: {e}")
            return False
            
    async def setup_etcd_tools(self) -> bool:
        """Clone etcd-tools repository if needed"""
        if not Path("etcd-tools").exists():
            self.log_info("Cloning etcd-tools repository...")
            try:
                result = await self.run_subprocess([
                    'git', 'clone', 'https://github.com/peterducai/etcd-tools.git'
                ])
                if result.returncode == 0:
                    self.log_info("etcd-tools cloned successfully")
                    await asyncio.sleep(10)
                    return True
                else:
                    self.log_error("Failed to clone etcd-tools repository")
                    return False
            except Exception as e:
                self.log_error(f"Failed to clone etcd-tools repository: {e}")
                return False
        else:
            self.log_info("etcd-tools directory already exists")
            return True
            
    async def run_etcd_analyzer(self) -> bool:
        """Run etcd analyzer script"""
        analyzer_path = Path("etcd-tools/etcd-analyzer.sh")
        if not analyzer_path.exists():
            self.log_warn("etcd-analyzer.sh not found")
            return False
            
        self.log_info("Running etcd analyzer...")
        try:
            result = await self.run_subprocess(['bash', str(analyzer_path)], capture_output=False)
            return result.returncode == 0
        except Exception as e:
            self.log_error(f"Failed to run etcd analyzer: {e}")
            return False
            
    async def run_fio_test(self) -> bool:
        """Run FIO performance test"""
        self.log_info("Starting FIO test...")
        
        try:
            # Get master nodes
            nodes = await asyncio.to_thread(self.core_v1.list_node)
            master_node = None
            
            for node in nodes.items:
                labels = node.metadata.labels or {}
                for label in labels:
                    if 'master' in label.lower() or 'control-plane' in label.lower():
                        master_node = node.metadata.name
                        break
                if master_node:
                    break
                    
            if not master_node:
                self.log_error("No master node found")
                return False
                
            self.log_info(f"Running FIO test on master node: {master_node}")
            
            result = await self.run_subprocess([
                'oc', 'debug', '-n', self.config.etcd_namespace, '--quiet=true',
                f'node/{master_node}', '--',
                'chroot', 'host', 'bash', '-c',
                'podman run --privileged --volume /var/lib/etcd:/test '
                'quay.io/peterducai/openshift-etcd-suite:latest fio'
            ], capture_output=False)
            
            return result.returncode == 0
            
        except Exception as e:
            self.log_error(f"Failed to run FIO test: {e}")
            return False


async def create_argument_parser():
    """Create and configure argument parser"""
    parser = argparse.ArgumentParser(
        description='OpenShift Resource Creation and Testing Script (Async Kubernetes Client)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 1                                # Create default number of namespaces
  %(prog)s --max-namespaces 200 1           # Create 200 namespaces
  %(prog)s --max-images 5000 2              # Create 5000 images
  %(prog)s --configmap-size 10 5            # Create 10GB of ConfigMaps
  %(prog)s all                              # Run all test cases

Environment Variables:
  MAX_NAMESPACES, MAX_IMAGES, IMAGES_PER_NS, IMAGES_MAX_NS,
  SECRETS_PER_NS, SECRETS_MAX_NS, LARGE_SECRETS_PER_NS, LARGE_SECRETS_NAX_NS,
  CONFIGMAP_SIZE_GB, CONFIGMAP_NAMESPACE, BATCH_SIZE, ETCD_NAMESPACE,
  MULTI_IMAGES_NS, SECRETS_NS, LARGE_SECRETS_NS, LOG_FILE
        """
    )
    
    # Positional argument for case number
    parser.add_argument(
        'case_number',
        choices=['1', '2', '3', '4', '5', '6', '7', 'all'],
        help='Test case to run: '
             '1=namespaces, 2=images, 3=secrets, 4=large-secrets, '
             '5=configmaps, 6=etcd-analyzer, 7=fio-test, all=run-all'
    )
    
    # Optional arguments
    parser.add_argument('-p', '--max-namespaces', type=int, help='Maximum number of namespaces')
    parser.add_argument('-i', '--max-images', type=int, help='Maximum number of images')
    parser.add_argument('--images-per-ns', type=int, help='Images per namespace')
    parser.add_argument('--images-max-ns', type=int, help='Maximum image namespaces')
    parser.add_argument('-s', '--secrets-per-ns', type=int, help='Secrets per namespace')
    parser.add_argument('-n', '--secrets-max-ns', type=int, help='Maximum secret namespaces')
    parser.add_argument('--large-secrets-per-ns', type=int, help='Large secrets per namespace')
    parser.add_argument('--large-secrets-max-ns', type=int, help='Maximum large secret namespaces')
    parser.add_argument('-c', '--configmap-size', type=int, help='ConfigMap total size in GB')
    parser.add_argument('--configmap-namespace', help='ConfigMap namespace prefix')
    parser.add_argument('--configmap-payload-size', type=int, help='ConfigMap payload size (in 512KiB blocks)')
    parser.add_argument('--configmaps-per-ns', type=int, help='ConfigMaps per namespace')
    parser.add_argument('-b', '--batch-size', type=int, help='Batch size for parallel operations')
    parser.add_argument('--namespace-parallel', type=int, help='Parallel namespace creation batch size')
    parser.add_argument('-o', '--log-file', help='Log file path')
    parser.add_argument('--etcd-namespace', help='etcd namespace')
    parser.add_argument('--cleanup-target-namespace', help='Namespace prefix pattern for cleanup')
    
    return parser


async def main():
    """Main execution function"""
    parser = await create_argument_parser()
    args = parser.parse_args()
    
    # Create configuration
    config = Config()
    
    # Override config with command line arguments
    if args.max_namespaces is not None:
        config.max_namespaces = args.max_namespaces
    if args.max_images is not None:
        config.max_images = args.max_images
    if args.images_per_ns is not None:
        config.images_per_ns = args.images_per_ns
    if args.images_max_ns is not None:
        config.images_max_ns = args.images_max_ns
    if args.secrets_per_ns is not None:
        config.secrets_per_ns = args.secrets_per_ns
    if args.secrets_max_ns is not None:
        config.secrets_max_ns = args.secrets_max_ns
    if args.large_secrets_per_ns is not None:
        config.large_secrets_per_ns = args.large_secrets_per_ns
    if args.large_secrets_max_ns is not None:
        config.large_secrets_max_ns = args.large_secrets_max_ns
    if args.configmap_size is not None:
        config.configmap_size_gb = args.configmap_size
    if args.configmap_namespace is not None:
        config.configmap_namespace = args.configmap_namespace
    if args.configmap_payload_size is not None:
        config.configmap_payload_size = args.configmap_payload_size
    if args.configmaps_per_ns is not None:
        config.configmaps_per_ns = args.configmaps_per_ns
    if args.batch_size is not None:
        config.batch_size = args.batch_size
    if args.namespace_parallel is not None:
        config.namespace_parallel = args.namespace_parallel
    if args.log_file is not None:
        config.log_file = args.log_file
    if args.etcd_namespace is not None:
        config.etcd_namespace = args.etcd_namespace
    if args.cleanup_target_namespace is not None:
        config.cleanup_target_namespace = args.cleanup_target_namespace
    
    # Create main application instance
    app = EtcdLoadToolsAsync(config)
    
    try:
        app.log_info("Starting OpenShift resource creation script (Async)")
        app.log_info(f"Configuration: Namespaces={config.max_namespaces}, "
                    f"Images={config.max_images}, ConfigMapSize={config.configmap_size_gb}GB, "
                    f"BatchSize={config.batch_size}")
        
        # Check prerequisites
        if not await app.check_prerequisites():
            app.log_error("Prerequisites check failed")
            sys.exit(1)
        
        case_number = args.case_number
        
        if case_number == '1':
            await app.create_namespaces(config.max_namespaces)
        elif case_number == '2':
            await app.create_images(config.images_per_ns, config.images_max_ns, config.multi_images_ns)
        elif case_number == '3':
            await app.create_secrets(config.secrets_per_ns, config.secrets_max_ns, config.secrets_ns)
        elif case_number == '4':
            await app.create_large_secrets(config.large_secrets_per_ns, config.large_secrets_max_ns, config.large_secrets_ns)
        elif case_number == '5':
            await app.create_configmaps(config.configmap_size_gb, config.configmap_namespace)
        elif case_number == '6':
            if await app.setup_etcd_tools():
                await app.run_etcd_analyzer()
        elif case_number == '7':
            await app.run_fio_test()
        elif case_number == 'all':
            app.log_info("Running all test cases...")
            
            # Run all tests with error handling
            test_results = {}
            
            try:
                await app.cleanup_project_namespace()
                test_results['cleanup_initial'] = True
            except Exception as e:
                app.log_error(f"Initial cleanup failed: {e}")
                test_results['cleanup_initial'] = False
            
            try:
                await app.create_namespaces(config.max_namespaces)
                test_results['namespaces'] = True
            except Exception as e:
                app.log_error(f"Namespace creation failed: {e}")
                test_results['namespaces'] = False
                
            try:
                await app.create_images(config.images_per_ns, config.images_max_ns, config.multi_images_ns)
                test_results['images'] = True
            except Exception as e:
                app.log_error(f"Image creation failed: {e}")
                test_results['images'] = False
                
            try:
                await app.create_secrets(config.secrets_per_ns, config.secrets_max_ns, config.secrets_ns)
                test_results['secrets'] = True
            except Exception as e:
                app.log_error(f"Secret creation failed: {e}")
                test_results['secrets'] = False
                
            try:
                await app.create_large_secrets(config.large_secrets_per_ns, config.large_secrets_max_ns, config.large_secrets_ns)
                test_results['large_secrets'] = True
            except Exception as e:
                app.log_error(f"Large secret creation failed: {e}")
                test_results['large_secrets'] = False
                
            try:
                await app.create_configmaps(config.configmap_size_gb, config.configmap_namespace)
                test_results['configmaps'] = True
            except Exception as e:
                app.log_error(f"ConfigMap creation failed: {e}")
                test_results['configmaps'] = False
                
            try:
                await app.run_fio_test()
                test_results['fio_test'] = True
            except Exception as e:
                app.log_error(f"FIO test failed: {e}")
                test_results['fio_test'] = False
                
            try:
                if await app.setup_etcd_tools():
                    await app.run_etcd_analyzer()
                    test_results['etcd_analyzer'] = True
                else:
                    test_results['etcd_analyzer'] = False
            except Exception as e:
                app.log_error(f"ETCD analyzer failed: {e}")
                test_results['etcd_analyzer'] = False
                
            try:
                await app.cleanup_project_namespace()
                await app.clean_images()
                test_results['cleanup_final'] = True
            except Exception as e:
                app.log_error(f"Final cleanup failed: {e}")
                test_results['cleanup_final'] = False
            
            # Report results
            successful_tests = sum(1 for result in test_results.values() if result)
            total_tests = len(test_results)
            app.log_info(f"Test suite completed: {successful_tests}/{total_tests} tests successful")
            
            for test_name, result in test_results.items():
                status = "PASS" if result else "FAIL"
                color = Colors.GREEN if result else Colors.RED
                print(f"  {color}{test_name}: {status}{Colors.NC}")
        else:
            app.log_error(f"Invalid case number: {case_number}")
            sys.exit(1)
        
        app.log_info("Script execution completed successfully")
        
        # Final cleanup
        try:
            await app.cleanup_project_namespace()
            await app.clean_images()
        except Exception as e:
            app.log_warn(f"Final cleanup encountered errors: {e}")
        
    except KeyboardInterrupt:
        app.log_warn("Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        app.log_error(f"Script failed with error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())