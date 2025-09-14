"""
etcd Cluster Status Collector
Collects etcd cluster status and health information using oc rsh commands
"""

import asyncio
import json
import logging
import subprocess
from typing import Dict, Any, List, Optional
from datetime import datetime
import pytz


class ClusterStatCollector:
    """Collector for etcd cluster status information"""
    
    def __init__(self, ocp_auth=None):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
        self.etcd_namespace = "openshift-etcd"
    
    async def _get_etcd_pod_name(self) -> Dict[str, Any]:
        """Get the first available etcd pod name"""
        try:
            cmd = [
                "oc", "get", "pod", 
                "-n", self.etcd_namespace, 
                "-l", "app=etcd", 
                "-o", "jsonpath={.items[0].metadata.name}"
            ]
            
            result = await self._run_command(cmd)
            
            if result['returncode'] != 0:
                return {
                    'status': 'error',
                    'error': f"Failed to get etcd pod: {result['stderr']}"
                }
            
            pod_name = result['stdout'].strip()
            if not pod_name:
                return {
                    'status': 'error',
                    'error': "No etcd pods found"
                }
            
            return {
                'status': 'success',
                'pod_name': pod_name
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd pod name: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _run_command(self, cmd: List[str]) -> Dict[str, Any]:
        """Run a shell command asynchronously"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            return {
                'returncode': process.returncode,
                'stdout': stdout.decode('utf-8'),
                'stderr': stderr.decode('utf-8')
            }
            
        except Exception as e:
            return {
                'returncode': -1,
                'stdout': '',
                'stderr': str(e)
            }
    
    async def _execute_etcd_command(self, etcd_cmd: str, pod_name: str) -> Dict[str, Any]:
        """Execute etcdctl command in the etcd pod using oc rsh"""
        try:
            cmd = [
                "oc", "rsh", 
                "-n", self.etcd_namespace, 
                "-c", "etcd", 
                pod_name,
                "sh", "-c", 
                f"unset ETCDCTL_ENDPOINTS; {etcd_cmd}"
            ]
            
            result = await self._run_command(cmd)
            
            if result['returncode'] != 0:
                return {
                    'status': 'error',
                    'error': result['stderr'],
                    'output': result['stdout']
                }
            
            return {
                'status': 'success',
                'output': result['stdout']
            }
            
        except Exception as e:
            self.logger.error(f"Error executing etcd command: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive etcd cluster status"""
        try:
            # Get etcd pod name
            pod_result = await self._get_etcd_pod_name()
            if pod_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': f"Failed to get etcd pod: {pod_result.get('error')}",
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            pod_name = pod_result['pod_name']
            
            cluster_info = {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'etcd_pod': pod_name,
                'cluster_health': await self._get_cluster_health(pod_name),
                'member_status': await self._get_member_status(pod_name),
                'endpoint_status': await self._get_endpoint_status(pod_name),
                'leader_info': await self._get_leader_info(pod_name),
                'cluster_metrics': await self._get_basic_metrics(pod_name)
            }
            
            return {
                'status': 'success',
                'data': cluster_info
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cluster status: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _get_cluster_health(self, pod_name: str) -> Dict[str, Any]:
        """Get cluster health information"""
        try:
            health_result = await self._execute_etcd_command(
                "etcdctl endpoint health --cluster",
                pod_name
            )
            
            if health_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': health_result.get('error')
                }
            
            # Parse health output
            health_lines = health_result.get('output', '').strip().split('\n')
            healthy_endpoints = []
            unhealthy_endpoints = []
            
            for line in health_lines:
                if line.strip():
                    if 'is healthy' in line.lower():
                        endpoint = line.split()[0]
                        healthy_endpoints.append(endpoint)
                    elif 'is unhealthy' in line.lower() or 'unhealthy' in line.lower():
                        endpoint = line.split()[0] if line.split() else 'unknown'
                        unhealthy_endpoints.append(endpoint)
            
            total_endpoints = len(healthy_endpoints) + len(unhealthy_endpoints)
            health_percentage = (len(healthy_endpoints) / max(total_endpoints, 1)) * 100
            
            return {
                'status': 'healthy' if len(unhealthy_endpoints) == 0 else 'degraded',
                'healthy_endpoints': healthy_endpoints,
                'unhealthy_endpoints': unhealthy_endpoints,
                'total_endpoints': total_endpoints,
                'health_percentage': round(health_percentage, 2),
                'raw_output': health_result.get('output')
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cluster health: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_member_status(self, pod_name: str) -> Dict[str, Any]:
        """Get member status information"""
        try:
            member_result = await self._execute_etcd_command(
                "etcdctl member list -w json",
                pod_name
            )
            
            if member_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': member_result.get('error')
                }
            
            # Parse JSON output
            output = member_result.get('output', '').strip()
            if not output:
                return {'status': 'no_data', 'members': []}
            
            try:
                members_data = json.loads(output)
                members = members_data.get('members', [])
                
                active_members = [m for m in members if not m.get('isLearner', False)]
                learner_members = [m for m in members if m.get('isLearner', False)]
                
                return {
                    'status': 'success',
                    'total_members': len(members),
                    'active_members': len(active_members),
                    'learner_members': len(learner_members),
                    'members': [
                        {
                            'id': member.get('ID'),
                            'name': member.get('name'),
                            'peer_urls': member.get('peerURLs', []),
                            'client_urls': member.get('clientURLs', []),
                            'is_learner': member.get('isLearner', False)
                        }
                        for member in members
                    ]
                }
                
            except json.JSONDecodeError as e:
                return {
                    'status': 'parse_error',
                    'error': f'Failed to parse member list JSON: {e}',
                    'raw_output': output
                }
            
        except Exception as e:
            self.logger.error(f"Error getting member status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_endpoint_status(self, pod_name: str) -> Dict[str, Any]:
        """Get endpoint status information using the exact command you specified"""
        try:
            status_result = await self._execute_etcd_command(
                "etcdctl endpoint status -w table --cluster",
                pod_name
            )
            
            if status_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': status_result.get('error')
                }
            
            # Parse table output
            output = status_result.get('output', '').strip()
            lines = output.split('\n')
            
            endpoints_info = []
            
            # Look for table data (skip header and separator lines)
            for line in lines:
                line = line.strip()
                if line and '|' in line and not line.startswith('+') and 'ENDPOINT' not in line.upper():
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if len(parts) >= 4:
                        endpoints_info.append({
                            'endpoint': parts[0],
                            'id': parts[1],
                            'version': parts[2] if len(parts) > 2 else 'unknown',
                            'db_size': parts[3] if len(parts) > 3 else 'unknown',
                            'is_leader': parts[4].lower() == 'true' if len(parts) > 4 else False,
                            'raft_term': parts[5] if len(parts) > 5 else 'unknown',
                            'raft_index': parts[6] if len(parts) > 6 else 'unknown'
                        })
            
            # Find leader
            leader_endpoint = None
            for endpoint in endpoints_info:
                if endpoint.get('is_leader'):
                    leader_endpoint = endpoint['endpoint']
                    break
            
            return {
                'status': 'success',
                'endpoints': endpoints_info,
                'total_endpoints': len(endpoints_info),
                'leader_endpoint': leader_endpoint,
                'raw_output': output
            }
            
        except Exception as e:
            self.logger.error(f"Error getting endpoint status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_leader_info(self, pod_name: str) -> Dict[str, Any]:
        """Get leader information"""
        try:
            endpoint_status = await self._get_endpoint_status(pod_name)
            
            if endpoint_status['status'] != 'success':
                return endpoint_status
            
            leader_info = {
                'has_leader': False,
                'leader_endpoint': None,
                'term': None,
                'leader_id': None
            }
            
            for endpoint in endpoint_status.get('endpoints', []):
                if endpoint.get('is_leader'):
                    leader_info.update({
                        'has_leader': True,
                        'leader_endpoint': endpoint['endpoint'],
                        'term': endpoint.get('raft_term'),
                        'leader_id': endpoint.get('id')
                    })
                    break
            
            return {
                'status': 'success',
                'leader_info': leader_info
            }
            
        except Exception as e:
            self.logger.error(f"Error getting leader info: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_basic_metrics(self, pod_name: str) -> Dict[str, Any]:
        """Get basic cluster metrics"""
        try:
            # Get endpoint status for basic metrics
            endpoint_status = await self._get_endpoint_status(pod_name)
            
            if endpoint_status['status'] != 'success':
                return endpoint_status
            
            endpoints = endpoint_status.get('endpoints', [])
            total_db_size = 0
            
            # Try to calculate total DB size (if format allows)
            for endpoint in endpoints:
                db_size_str = endpoint.get('db_size', '0')
                try:
                    # Extract numeric part (assuming format like "25 MB" or "1.2 GB")
                    if 'MB' in db_size_str.upper():
                        size_mb = float(db_size_str.upper().replace('MB', '').strip())
                        total_db_size += size_mb
                    elif 'GB' in db_size_str.upper():
                        size_gb = float(db_size_str.upper().replace('GB', '').strip())
                        total_db_size += size_gb * 1024
                    elif 'KB' in db_size_str.upper():
                        size_kb = float(db_size_str.upper().replace('KB', '').strip())
                        total_db_size += size_kb / 1024
                except (ValueError, AttributeError):
                    pass
            
            metrics = {
                'namespace': self.etcd_namespace,
                'etcd_pod': pod_name,
                'total_endpoints': len(endpoints),
                'leader_count': sum(1 for e in endpoints if e.get('is_leader')),
                'estimated_total_db_size_mb': round(total_db_size, 2) if total_db_size > 0 else None,
                'endpoints_summary': [
                    {
                        'endpoint': e.get('endpoint'),
                        'is_leader': e.get('is_leader'),
                        'version': e.get('version'),
                        'db_size': e.get('db_size')
                    }
                    for e in endpoints
                ]
            }
            
            return {
                'status': 'success',
                'metrics': metrics
            }
            
        except Exception as e:
            self.logger.error(f"Error getting basic metrics: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_etcd_cluster_table_status(self) -> Dict[str, Any]:
        """Get etcd cluster status in table format (equivalent to the exact command you specified)"""
        try:
            # Get etcd pod name
            pod_result = await self._get_etcd_pod_name()
            if pod_result['status'] != 'success':
                return pod_result
            
            pod_name = pod_result['pod_name']
            
            # Execute the exact command you specified
            status_result = await self._execute_etcd_command(
                "etcdctl endpoint status -w table --cluster",
                pod_name
            )
            
            if status_result['status'] != 'success':
                return status_result
            
            # Parse the table output
            output = status_result.get('output', '').strip()
            lines = output.split('\n')
            
            table_data = []
            headers = []
            
            # Extract headers and data
            for line in lines:
                line = line.strip()
                if line and '|' in line:
                    if 'ENDPOINT' in line.upper():
                        # This is the header line
                        headers = [h.strip() for h in line.split('|') if h.strip()]
                    elif not line.startswith('+') and 'ENDPOINT' not in line.upper():
                        # This is a data line
                        row = [r.strip() for r in line.split('|') if r.strip()]
                        if row:  # Only add non-empty rows
                            table_data.append(row)
            
            # If no headers were found, use default ones
            if not headers:
                headers = ['ENDPOINT', 'ID', 'VERSION', 'DB SIZE', 'IS LEADER', 'RAFT TERM', 'RAFT INDEX']
            
            return {
                'status': 'success',
                'command_used': f"oc rsh -n {self.etcd_namespace} -c etcd {pod_name} sh -c 'unset ETCDCTL_ENDPOINTS; etcdctl endpoint status -w table --cluster'",
                'etcd_pod': pod_name,
                'table_format': {
                    'headers': headers,
                    'rows': table_data
                },
                'raw_output': output,
                'summary': {
                    'total_endpoints': len(table_data),
                    'leader_count': sum(1 for row in table_data if len(row) > 4 and row[4].lower() == 'true')
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

    async def quick_status_check(self) -> Dict[str, Any]:
        """Quick status check using the exact command format you provided"""
        return await self.get_etcd_cluster_table_status()