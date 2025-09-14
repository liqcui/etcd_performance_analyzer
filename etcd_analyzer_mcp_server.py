#!/usr/bin/env python3
"""
OpenShift etcd Analyzer MCP Server
Main server implementation using FastMCP
"""

import os
import sys
import asyncio
import logging
import warnings
from typing import Any, Dict, Optional
from datetime import datetime
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set timezone to UTC
os.environ['TZ'] = 'UTC'

# Suppress deprecation warnings from websockets/uvicorn (imported internally by uvicorn)
# Use regex to include submodules like websockets.legacy and uvicorn.protocols.websockets.websockets_impl
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^websockets(\..*)?$")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^uvicorn\.protocols\.websockets(\..*)?$")

try:
    from fastmcp import FastMCP
    from pydantic import BaseModel, Field, ConfigDict
    import uvicorn
except ImportError as e:
    logger.error(f"Required dependencies not installed: {e}")
    logger.error("Please install: pip install fastmcp>=1.12.4 pydantic uvicorn")
    sys.exit(1)

# Import our modules
try:
    from ocauth.ocp_auth import OCPAuth
    from tools.etcd_cluster_status import ClusterStatCollector
    from tools.etcd_general_info import GeneralInfoCollector
    from tools.etcd_disk_compact_defrag import CompactDefragCollector
    from tools.etcd_disk_wal_fsync import DiskWALFsyncCollector
    from tools.etcd_disk_backend_commit import DiskBackendCommitCollector
    from tools.etcd_network_io import NetworkIOCollector
    from tools.etcd_disk_io import DiskIOCollector
    from tools.ocp_cluster_info import ClusterInfoCollector
    from config.etcd_config import get_config
except ImportError as e:
    logger.error(f"Failed to import local modules: {e}")
    logger.error("Please ensure all modules are in the correct directory structure")
    sys.exit(1)


# Pydantic models for MCP tools using ConfigDict
class MCPBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class DurationInput(MCPBaseModel):
    duration: str = Field(
        default="1h",
        description="Time duration for metrics collection. Supported formats: '1h' (1 hour), '30m' (30 minutes), '1d' (1 day), '2h' (2 hours), etc. Common values: '15m', '30m', '1h', '2h', '6h', '12h', '24h', '1d'"
    )


class ETCDClusterStatusResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str


class ETCDMetricsResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None


# Initialize MCP server
mcp = FastMCP("OpenShift etcd Analyzer")

# Global variables for collectors
ocp_auth = None
cluster_collector = None
general_collector = None
compact_defrag_collector = None
wal_fsync_collector = None
backend_commit_collector = None
network_collector = None
disk_io_collector = None
cluster_info_collector = None


async def initialize_collectors():
    """Initialize all collectors with authentication"""
    global ocp_auth, cluster_collector, general_collector, compact_defrag_collector
    global wal_fsync_collector, backend_commit_collector, network_collector, disk_io_collector
    global cluster_info_collector
    
    try:
        # Initialize OpenShift authentication
        ocp_auth = OCPAuth()
        auth_success = await ocp_auth.initialize()
        
        if not auth_success:
            logger.error("Failed to initialize OpenShift authentication")
            return False
        
        logger.info("OpenShift authentication initialized successfully")
        
        # Initialize all collectors
        cluster_collector = ClusterStatCollector(ocp_auth)
        general_collector = GeneralInfoCollector(ocp_auth)
        compact_defrag_collector = CompactDefragCollector(ocp_auth)
        wal_fsync_collector = DiskWALFsyncCollector(ocp_auth)
        backend_commit_collector = DiskBackendCommitCollector(ocp_auth)
        network_collector = NetworkIOCollector(ocp_auth)
        disk_io_collector = DiskIOCollector(ocp_auth)
        
        # Initialize OCP cluster info collector (uses OCPAuth client)
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("All collectors initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize collectors: {e}")
        return False


# MCP Tool Definitions
# Health check tool (instead of HTTP endpoint)
@mcp.tool()
async def get_server_health() -> Dict[str, Any]:
    """
    Get server health status and collector initialization status.
    
    Checks the health and readiness of the etcd analyzer server and all its components:
    - Overall server health status
    - Individual collector initialization status
    - OpenShift authentication status
    - Timestamp of health check
    
    Use this tool to verify that the etcd analyzer is properly configured and ready to collect metrics.
    
    Returns:
        Dict: Health status information including:
        - status: 'healthy' or 'unhealthy' overall status
        - collectors_initialized: Boolean indicating if all collectors are ready
        - details: Individual status of each collector component
        - timestamp: When the health check was performed
    """
    collectors_initialized = all([
        ocp_auth is not None,
        cluster_collector is not None,
        general_collector is not None,
        compact_defrag_collector is not None,
        wal_fsync_collector is not None,
        backend_commit_collector is not None,
        network_collector is not None,
        disk_io_collector is not None
    ])
    
    return {
        "status": "healthy" if collectors_initialized else "unhealthy",
        "timestamp": datetime.now(pytz.UTC).isoformat(),
        "collectors_initialized": collectors_initialized,
        "details": {
            "ocp_auth": ocp_auth is not None,
            "cluster_collector": cluster_collector is not None,
            "general_collector": general_collector is not None,
            "compact_defrag_collector": compact_defrag_collector is not None,
            "wal_fsync_collector": wal_fsync_collector is not None,
            "backend_commit_collector": backend_commit_collector is not None,
            "network_collector": network_collector is not None,
            "disk_io_collector": disk_io_collector is not None,
            "cluster_info_collector": cluster_info_collector is not None
        }
    }


@mcp.tool()
async def get_ocp_cluster_info() -> Dict[str, Any]:
    """
    Get comprehensive OpenShift cluster information and infrastructure details.
    
    Collects detailed information about the OpenShift cluster hosting the etcd cluster:
    - Cluster identification (name, version, platform - AWS/Azure/GCP/etc.)
    - Node information (master, infra, worker nodes with specs and status)
    - Resource counts (namespaces, pods, services, secrets, configmaps)
    - Network policy counts (NetworkPolicies, AdminNetworkPolicies, etc.)
    - Network resources (EgressFirewalls, EgressIPs, UserDefinedNetworks)
    - Cluster operator status (unavailable operators)
    - Machine Config Pool (MCP) status
    
    This provides context for etcd performance by showing the cluster environment.
    
    No parameters required - collects current cluster state.
    
    Returns:
        Dict: Comprehensive cluster information including:
        - cluster_name, cluster_version, platform details
        - Complete node inventory with specifications and health
        - Resource counts across all namespaces
        - Network policy and resource statistics
        - Operator and MCP status information
        - Collection timestamp
    """
    try:
        if cluster_info_collector is None:
            return {
                "status": "error",
                "error": "ClusterInfoCollector not initialized",
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }

        info = await cluster_info_collector.collect_cluster_info()
        return {
            "status": "success",
            "data": cluster_info_collector.to_dict(info),
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }
    except Exception as e:
        logger.error(f"Error collecting OCP cluster info: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }

@mcp.tool()
async def get_etcd_cluster_status() -> ETCDClusterStatusResponse:
    """
    Get comprehensive etcd cluster status including health, member information, and leadership details.
    
    This tool provides real-time etcd cluster status by executing etcdctl commands to check:
    - Cluster health status (healthy/degraded endpoints)
    - Member list with active and learner members
    - Endpoint status including leader information, database sizes, and Raft terms
    - Leadership information and changes
    - Basic cluster metrics
    
    No parameters required - uses current cluster configuration.
    
    Returns:
        ETCDClusterStatusResponse: Complete cluster status including:
        - cluster_health: Health status of all endpoints
        - member_status: List of all cluster members with their roles
        - endpoint_status: Detailed endpoint information including leader
        - leader_info: Current leader details and Raft information
        - cluster_metrics: Basic cluster metrics and configuration
    """
    try:
        if not cluster_collector:
            return ETCDClusterStatusResponse(
                status="error",
                error="Cluster collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        result = await cluster_collector.get_cluster_status()
        
        return ETCDClusterStatusResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat())
        )
        
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        return ETCDClusterStatusResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )


@mcp.tool()
async def get_etcd_general_info(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get general etcd cluster information including resource usage and operational metrics.
    
    Collects comprehensive etcd performance and health metrics including:
    - CPU and memory usage patterns
    - Database size metrics (physical and logical sizes, space utilization)
    - Proposal metrics (commit rates, failures, pending proposals)
    - Leadership metrics (leader changes, elections, has_leader status)
    - Performance metrics (slow applies, read indexes, operation rates)
    - Health metrics (heartbeat failures, total keys, compacted keys)
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: General cluster information including:
        - Resource usage statistics (CPU, memory, disk space)
        - Operational performance metrics
        - Proposal and leadership statistics
        - Database size and utilization data
    """
    try:
        if not general_collector:
            return ETCDMetricsResponse(
                status="error",
                error="General info collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="general_info",
                duration=duration
            )
        
        result = await general_collector.collect_metrics(duration)
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="general_info",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting general info: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="general_info",
            duration=duration
        )


@mcp.tool()
async def get_etcd_disk_compact_defrag(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get etcd database compaction and defragmentation performance metrics.
    
    Monitors database maintenance operations that are critical for etcd performance:
    - Compaction duration and rates (time spent compacting old revisions)
    - Defragmentation duration and rates (database defragmentation operations)
    - Page fault metrics (vmstat pgmajfault rates indicating memory pressure)
    - Operation efficiency analysis and performance recommendations
    
    These metrics help identify database maintenance bottlenecks and storage performance issues.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: Compaction and defragmentation metrics including:
        - Compaction operation duration and frequency
        - Defragmentation operation performance
        - Page fault statistics and memory pressure indicators
        - Performance analysis and efficiency recommendations
    """
    try:
        if not compact_defrag_collector:
            return ETCDMetricsResponse(
                status="error",
                error="Compact/defrag collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="disk_compact_defrag",
                duration=duration
            )
        
        result = await compact_defrag_collector.collect_metrics(duration)
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="disk_compact_defrag",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting compact/defrag metrics: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="disk_compact_defrag",
            duration=duration
        )

@mcp.tool()
async def get_etcd_disk_wal_fsync(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get etcd Write-Ahead Log (WAL) fsync performance metrics.
    
    Monitors WAL fsync operations that are critical for etcd data durability and write performance:
    - WAL fsync P99 latency (99th percentile fsync duration - target <10ms for good performance)
    - WAL fsync operation rates and counts (operations per second)
    - WAL fsync duration sum statistics (cumulative fsync time)
    - Cluster-wide WAL fsync performance analysis and health scoring
    
    WAL fsync performance directly impacts write latency. High fsync times (>100ms) indicate storage bottlenecks
    that can cause cluster instability and performance degradation.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: WAL fsync performance metrics including:
        - P99 fsync latency per etcd pod with performance evaluation
        - Fsync operation rates and throughput statistics
        - Cumulative fsync duration and efficiency analysis
        - Cluster-wide performance summary with health indicators
        - Storage performance recommendations and bottleneck identification
    """
    try:
        if not wal_fsync_collector:
            return ETCDMetricsResponse(
                status="error",
                error="WAL fsync collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="disk_wal_fsync",
                duration=duration
            )
        
        result = await wal_fsync_collector.collect_all_metrics()
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="disk_wal_fsync",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting WAL fsync metrics: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="disk_wal_fsync",
            duration=duration
        )

@mcp.tool()
async def get_etcd_disk_backend_commit(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get etcd backend commit operation performance metrics.
    
    Monitors backend database commit operations that handle data persistence:
    - Backend commit duration P99 latency (99th percentile response times)
    - Commit operation rates and counts  
    - Commit duration statistics and efficiency analysis
    - Performance recommendations for write optimization
    
    Backend commit latency affects overall write performance. High latency (>25ms) indicates storage bottlenecks.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: Backend commit performance metrics including:
        - P99 commit latency statistics and performance grades
        - Commit operation throughput and efficiency scores
        - Performance analysis with latency and rate evaluation
        - Storage optimization recommendations
    """
    try:
        if not backend_commit_collector:
            return ETCDMetricsResponse(
                status="error",
                error="Backend commit collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="disk_backend_commit",
                duration=duration
            )
        
        result = await backend_commit_collector.collect_metrics(duration)
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="disk_backend_commit",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting backend commit metrics: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="disk_backend_commit",
            duration=duration
        )

@mcp.tool()
async def get_etcd_network_io(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get etcd network I/O performance and utilization metrics.
    
    Monitors comprehensive network performance metrics for etcd cluster operations:
    - Container network metrics (receive/transmit bytes for etcd pods)
    - Peer network metrics (peer-to-peer communication latency and throughput)
    - Client gRPC network metrics (client communication bandwidth)
    - Node network utilization (network interface utilization and packet rates)
    - Network drops and errors (packet loss and network issues)
    - gRPC stream metrics (active watch and lease streams)
    - Snapshot transfer duration (cluster synchronization network performance)
    
    Network performance directly impacts etcd cluster stability, client response times, and peer synchronization.
    High network latency or packet loss can cause cluster instability and performance degradation.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: Network I/O performance metrics including:
        - Container network throughput per etcd pod and aggregated by node
        - Peer round-trip time P99 latency and data transfer rates
        - Client gRPC network bandwidth utilization
        - Node-level network interface utilization and packet statistics
        - Network error rates and packet drop statistics
        - Active gRPC stream counts for watches and leases
        - Network health assessment and performance recommendations
    """
    try:
        global ocp_auth, network_collector
        if not network_collector:
            # Lazy initialize if startup initialization didn't complete
            if ocp_auth is None:
                ocp_auth = OCPAuth()
                auth_success = await ocp_auth.initialize()
                if not auth_success:
                    return ETCDMetricsResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for network I/O",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        category="network_io",
                        duration=duration
                    )
            try:
                network_collector = NetworkIOCollector(ocp_auth)
            except Exception as e:
                return ETCDMetricsResponse(
                    status="error",
                    error=f"Failed to initialize NetworkIOCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    category="network_io",
                    duration=duration
                )
        
        result = await network_collector.collect_all_metrics(duration)
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('collection_time', datetime.now(pytz.UTC).isoformat()),
            category="network_io",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting network I/O metrics: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="network_io",
            duration=duration
        )

@mcp.tool()
async def get_etcd_disk_io(duration: str = "1h") -> ETCDMetricsResponse:
    """
    Get etcd disk I/O performance metrics including throughput and IOPS.
    
    Monitors comprehensive disk I/O performance metrics that directly impact etcd performance:
    - Container disk write metrics (etcd pod disk write throughput and patterns)
    - Node disk read/write throughput (bytes per second for storage devices)  
    - Node disk read/write IOPS (input/output operations per second)
    - Device-level I/O statistics aggregated by master node
    - Storage performance analysis and bottleneck identification
    
    Disk I/O performance is critical for etcd operations including:
    - WAL (Write-Ahead Log) write operations
    - Database snapshot creation and transfers
    - Compaction and defragmentation operations
    - Overall cluster stability and response times
    
    Poor disk I/O performance can cause etcd timeouts, leader elections, and cluster instability.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
                 Default: '1h' (1 hour)
    
    Returns:
        ETCDMetricsResponse: Disk I/O performance metrics including:
        - Container disk write rates per etcd pod aggregated by master node
        - Node disk read/write throughput (bytes/second) by storage device
        - Node disk read/write IOPS by storage device and master node
        - Device inventory and performance characteristics
        - I/O performance analysis with bottleneck identification
        - Storage optimization recommendations
    """
    try:
        global ocp_auth, disk_io_collector
        if not disk_io_collector:
            # Lazy initialize if startup initialization didn't complete
            if ocp_auth is None:
                ocp_auth = OCPAuth()
                auth_success = await ocp_auth.initialize()
                if not auth_success:
                    return ETCDMetricsResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for disk I/O",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        category="disk_io",
                        duration=duration
                    )
            try:
                disk_io_collector = DiskIOCollector(ocp_auth, duration)
            except Exception as e:
                return ETCDMetricsResponse(
                    status="error",
                    error=f"Failed to initialize DiskIOCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    category="disk_io",
                    duration=duration
                )
        
        # Update duration for this collection
        disk_io_collector.duration = duration
        result = await disk_io_collector.collect_all_metrics()
        
        return ETCDMetricsResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="disk_io",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting disk I/O metrics: {e}")
        return ETCDMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="disk_io",
            duration=duration
        )

# Server startup and initialization
async def startup_event():
    """Startup event handler"""
    logger.info("Starting OpenShift etcd Analyzer MCP Server...")
    
    # Initialize collectors
    init_success = await initialize_collectors()
    if not init_success:
        logger.error("Failed to initialize collectors. Server may not function properly.")
    
    # Load and validate configuration
    config = get_config()
    if not config.validate_config():
        logger.warning("Configuration validation failed. Some metrics may not work properly.")
    
    logger.info("OpenShift etcd Analyzer MCP Server started successfully!")


def main():
    """Main function to run the MCP server"""
    async def run_server():
        try:
            # Perform startup initialization
            await startup_event()
            
            # Run the server using streamable-http transport
            # Use wsproto for websockets to avoid deprecated websockets.legacy usage in uvicorn
            await mcp.run_async(
                transport="streamable-http",
                port=8000,
                host="0.0.0.0",
                uvicorn_config={
                    # Disable websockets to avoid deprecated websockets.legacy imports and extra deps
                    "ws": "none"
                }
            )
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            sys.exit(1)
    
    try:
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_running_loop()
            logger.warning("Already running in an event loop. Creating new task.")
            # If we're already in a loop, create a task instead
            task = loop.create_task(run_server())
            return task
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            asyncio.run(run_server())
            
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()