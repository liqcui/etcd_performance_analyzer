#!/usr/bin/env python3
"""
ETCD Analyzer MCP Agent with LangGraph StateGraph and DuckDB ELT
Updated version with General Info support
"""

import asyncio
import json
import logging
import uuid
import subprocess
from datetime import datetime, timezone
import sys
from typing import Any, Dict, List, Optional, TypedDict

from langchain.schema import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from storage.etcd_analyzer_stor_cluster_info import ClusterInfoStoreELT
from storage.etcd_analyzer_stor_disk_wal_fsync import DiskWalFsyncStorELT, print_wal_fsync_summary_tables
from storage.etcd_analyzer_stor_utility import TimeRangeUtilityELT
from storage.etcd_analyzer_stor_disk_io import DiskIOStorELT, print_disk_io_summary_tables
from storage.etcd_analyzer_stor_network_io import NetworkIOStorELT, print_network_io_summary_tables
from storage.etcd_analyzer_stor_backend_commit import BackendCommitStorELT, print_backend_commit_summary_tables
from storage.etcd_analyzer_stor_compact_defrag import CompactDefragStorELT, print_compact_defrag_summary_tables
from storage.etcd_analyzer_stor_general_info import GeneralInfoStorELT, print_general_info_summary_tables

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Update the AgentState TypedDict to include general info
class AgentState(TypedDict):
    """State for the ETCD Analyzer Agent"""
    messages: List[BaseMessage]
    cluster_info: Optional[Dict[str, Any]]
    cluster_info_data: Dict[str, Any]
    wal_fsync_data: Optional[Dict[str, Any]]
    disk_io_data: Optional[Dict[str, Any]]
    network_io_data: Optional[Dict[str, Any]]
    backend_commit_data: Optional[Dict[str, Any]]
    compact_defrag_data: Optional[Dict[str, Any]]
    general_info_data: Optional[Dict[str, Any]]  # Added general info data
    duration: str
    start_time: Optional[str]
    end_time: Optional[str]
    testing_id: str
    query_params: Dict[str, Any]
    results: Dict[str, Any]
    error: Optional[str]

class ETCDAnalyzerStorDBMCPAgent:
    """ETCD Analyzer Agent using LangGraph StateGraph and MCP integration with General Info support"""
    def __init__(self, mcp_server_url: str = "http://localhost:8000", db_path: str = "etcd_analyzer.duckdb"):
        self.mcp_server_url = mcp_server_url
        self.db_path = db_path
        # Global run UUID for this agent instance; used as a single testing run ID
        self.run_uuid = str(uuid.uuid4())
        self.cluster_store = ClusterInfoStoreELT(db_path)
        self.wal_fsync_store = DiskWalFsyncStorELT(db_path)
        self.disk_io_store = DiskIOStorELT(db_path)
        self.network_io_store = NetworkIOStorELT(db_path)
        self.backend_commit_store = BackendCommitStorELT(db_path)
        self.compact_defrag_store = CompactDefragStorELT(db_path)
        self.general_info_store = GeneralInfoStorELT(db_path)  # Added general info store
        self.graph = self._create_graph()

    def _create_graph(self) -> StateGraph:
        """Create the LangGraph StateGraph with General Info workflow"""
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("initialize", self._initialize_node)
        workflow.add_node("collect_cluster_info", self._collect_cluster_info_node)
        workflow.add_node("collect_general_info", self._collect_general_info_node)  # Added general info node
        workflow.add_node("collect_wal_fsync", self._collect_wal_fsync_node)
        workflow.add_node("collect_disk_io", self._collect_disk_io_node)
        workflow.add_node("collect_network_io", self._collect_network_io_node)
        workflow.add_node("collect_backend_commit", self._collect_backend_commit_node)
        workflow.add_node("collect_compact_defrag", self._collect_compact_defrag_node)
        workflow.add_node("store_data", self._store_data_node)
        workflow.add_node("finalize", self._finalize_node)
        workflow.add_node("handle_error", self._handle_error_node)
        
        # Add conditional edges
        workflow.add_conditional_edges(
            "collect_cluster_info",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_general_info"  # Updated flow
            }
        )
        
        workflow.add_conditional_edges(
            "collect_general_info",  # Added general info conditional edges
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_wal_fsync"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_wal_fsync",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_disk_io"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_disk_io",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_network_io"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_network_io",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_backend_commit"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_backend_commit",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_compact_defrag"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_compact_defrag",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "store_data"
            }
        )
        
        # Set workflow path
        workflow.set_entry_point("initialize")
        workflow.add_edge("initialize", "collect_cluster_info")
        workflow.add_edge("store_data", "finalize")
        workflow.add_edge("handle_error", END)
        workflow.add_edge("finalize", END)
        
        return workflow.compile()

    def _should_handle_error(self, state: AgentState) -> str:
        """Check if there's an error to handle"""
        return "error" if state.get("error") else "continue"
    
    async def _initialize_node(self, state: AgentState) -> AgentState:
        """Initialize the agent state"""
        # Use a global testing UUID for the entire agent run. Allow override via query_params if provided.
        query_params = state.get("query_params", {})
        testing_id = query_params.get("testing_id") or self.run_uuid
        
        # Parse query parameters
        duration = query_params.get("duration", "1h")
        start_time = query_params.get("start_time")
        end_time = query_params.get("end_time")
        
        # Validate time range if provided
        if start_time and end_time:
            validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
            if not validation["valid"]:
                return {
                    **state,
                    "error": f"Invalid time range: {validation['error']}",
                    "testing_id": testing_id,
                    "messages": state.get("messages", []) + [
                        AIMessage(content=f"Time range validation failed: {validation['error']}")
                    ]
                }
            # Convert to duration format if time range is valid
            duration = TimeRangeUtilityELT.format_duration_string(start_time, end_time) or duration
        
        logger.info(f"Initializing agent with testing_id (global run UUID): {testing_id}")
        
        return {
            **state,
            "testing_id": testing_id,
            "duration": duration,
            "start_time": start_time,
            "end_time": end_time,
            "cluster_info_data": {},
            "results": {},
            "messages": state.get("messages", []) + [
                AIMessage(content=f"Initialized ETCD analysis session: {testing_id}")
            ]
        }
    
    async def _collect_cluster_info_node(self, state: AgentState) -> AgentState:
        """Collect cluster information via MCP"""
        try:
            logger.info("Collecting cluster information...")
            
            cluster_info = await self._call_mcp_tool("get_ocp_cluster_info", {})
            
            if cluster_info.get("status") == "success":
                return {
                    **state,
                    "cluster_info": cluster_info.get("data"),
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected cluster information")
                    ]
                }
            else:
                error_msg = f"Failed to collect cluster info: {cluster_info.get('error')}"
                return {
                    **state,
                    "error": error_msg,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting cluster info: {str(e)}"
            logger.error(error_msg)
            return {
                **state,
                "error": error_msg,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _collect_general_info_node(self, state: AgentState) -> AgentState:
        """Collect general info metrics via MCP"""
        try:
            logger.info("Collecting general info metrics...")
            
            general_info_params = {"duration": state["duration"]}
            general_info_data = await self._call_mcp_tool("get_etcd_general_info", general_info_params)
            
            if general_info_data.get("status") == "success":
                return {
                    **state,
                    "general_info_data": general_info_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected general info metrics")
                    ]
                }
            else:
                error_msg = f"Failed to collect general info metrics: {general_info_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without general info data
                return {
                    **state,
                    "general_info_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting general info metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without general info data
            return {
                **state,
                "general_info_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }
    
    async def _collect_wal_fsync_node(self, state: AgentState) -> AgentState:
        """Collect WAL fsync metrics via MCP"""
        try:
            logger.info("Collecting WAL fsync metrics...")
            
            wal_fsync_params = {"duration": state["duration"]}
            wal_fsync_data = await self._call_mcp_tool("get_etcd_disk_wal_fsync", wal_fsync_params)
            
            if wal_fsync_data.get("status") == "success":
                return {
                    **state,
                    "wal_fsync_data": wal_fsync_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected WAL fsync metrics")
                    ]
                }
            else:
                # Surface auth guidance if available
                hint = wal_fsync_data.get('hint')
                error_msg = f"Failed to collect WAL fsync metrics: {wal_fsync_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without WAL fsync data
                return {
                    **state,
                    "wal_fsync_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg + (f" Hint: {hint}" if hint else ""))]
                }
                
        except Exception as e:
            error_msg = f"Error collecting WAL fsync metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without WAL fsync data
            return {
                **state,
                "wal_fsync_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }
      
    async def _collect_disk_io_node(self, state: AgentState) -> AgentState:
        """Collect disk I/O metrics via MCP"""
        try:
            logger.info("Collecting disk I/O metrics...")
            
            disk_io_params = {"duration": state["duration"]}
            disk_io_data = await self._call_mcp_tool("get_etcd_disk_io", disk_io_params)
            
            if disk_io_data.get("status") == "success":
                return {
                    **state,
                    "disk_io_data": disk_io_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected disk I/O metrics")
                    ]
                }
            else:
                error_msg = f"Failed to collect disk I/O metrics: {disk_io_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without disk I/O data
                return {
                    **state,
                    "disk_io_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting disk I/O metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without disk I/O data
            return {
                **state,
                "disk_io_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _collect_network_io_node(self, state: AgentState) -> AgentState:
        """Collect network I/O metrics via MCP"""
        try:
            logger.info("Collecting network I/O metrics...")
            
            network_io_params = {"duration": state["duration"]}
            network_io_data = await self._call_mcp_tool("get_etcd_network_io", network_io_params)
            
            if network_io_data.get("status") == "success":
                return {
                    **state,
                    "network_io_data": network_io_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected network I/O metrics")
                    ]
                }
            else:
                error_msg = f"Failed to collect network I/O metrics: {network_io_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without network I/O data
                return {
                    **state,
                    "network_io_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting network I/O metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without network I/O data
            return {
                **state,
                "network_io_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _collect_backend_commit_node(self, state: AgentState) -> AgentState:
        """Collect backend commit metrics via MCP"""
        try:
            logger.info("Collecting backend commit metrics...")
            
            backend_commit_params = {"duration": state["duration"]}
            backend_commit_data = await self._call_mcp_tool("get_etcd_disk_backend_commit", backend_commit_params)
            
            if backend_commit_data.get("status") == "success":
                return {
                    **state,
                    "backend_commit_data": backend_commit_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected backend commit metrics")
                    ]
                }
            else:
                error_msg = f"Failed to collect backend commit metrics: {backend_commit_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without backend commit data
                return {
                    **state,
                    "backend_commit_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting backend commit metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without backend commit data
            return {
                **state,
                "backend_commit_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _collect_compact_defrag_node(self, state: AgentState) -> AgentState:
        """Collect compact defrag metrics via MCP"""
        try:
            logger.info("Collecting compact defrag metrics...")
            
            compact_defrag_params = {"duration": state["duration"]}
            compact_defrag_data = await self._call_mcp_tool("get_etcd_disk_compact_defrag", compact_defrag_params)
            
            if compact_defrag_data.get("status") == "success":
                return {
                    **state,
                    "compact_defrag_data": compact_defrag_data,
                    "messages": state["messages"] + [
                        AIMessage(content="Successfully collected compact defrag metrics")
                    ]
                }
            else:
                error_msg = f"Failed to collect compact defrag metrics: {compact_defrag_data.get('error')}"
                logger.warning(error_msg)
                # Don't treat as fatal error, continue without compact defrag data
                return {
                    **state,
                    "compact_defrag_data": None,
                    "messages": state["messages"] + [AIMessage(content=error_msg)]
                }
                
        except Exception as e:
            error_msg = f"Error collecting compact defrag metrics: {str(e)}"
            logger.error(error_msg)
            # Don't treat as fatal error, continue without compact defrag data
            return {
                **state,
                "compact_defrag_data": None,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _store_data_node(self, state: AgentState) -> AgentState:
        """Store collected data in DuckDB including general info metrics"""
        try:
            logger.info("Storing data in DuckDB...")
            storage_results = {}
            
            # Store cluster info
            if state.get("cluster_info"):
                try:
                    await self.cluster_store.store_cluster_info(
                        cluster_info=state["cluster_info"],
                        testing_id=state["testing_id"]
                    )
                    storage_results["cluster_info"] = "success"
                    logger.info("Stored cluster information in DuckDB")
                except Exception as e:
                    storage_results["cluster_info"] = f"error: {str(e)}"
                    logger.error(f"Failed to store cluster info: {str(e)}")
            
            # Store general info data (NEW)
            if state.get("general_info_data"):
                try:
                    general_info_result = await self.general_info_store.store_general_info_metrics(
                        state["testing_id"], 
                        state["general_info_data"]
                    )
                    if general_info_result.get("status") == "success":
                        storage_results["general_info"] = "success"
                        storage_results["general_info_details"] = general_info_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        if "summary_report" in general_info_result:
                            print_general_info_summary_tables(general_info_result["summary_report"])
                        logger.info("Stored general info metrics in DuckDB")
                    else:
                        storage_results["general_info"] = f"error: {general_info_result.get('error')}"
                        logger.error(f"Failed to store general info data: {general_info_result.get('error')}")
                except Exception as e:
                    storage_results["general_info"] = f"error: {str(e)}"
                    logger.error(f"Failed to store general info data: {str(e)}")
            
            # Store WAL fsync data
            if state.get("wal_fsync_data"):
                try:
                    wal_result = await self.wal_fsync_store.store_wal_fsync_metrics(
                        state["testing_id"], 
                        state["wal_fsync_data"]
                    )
                    if wal_result.get("status") == "success":
                        storage_results["wal_fsync"] = "success"
                        storage_results["wal_fsync_details"] = wal_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        if "summary_report" in wal_result:
                            print_wal_fsync_summary_tables(wal_result["summary_report"])
                        logger.info("Stored WAL fsync metrics in DuckDB")
                    else:
                        storage_results["wal_fsync"] = f"error: {wal_result.get('error')}"
                        logger.error(f"Failed to store WAL fsync data: {wal_result.get('error')}")
                except Exception as e:
                    storage_results["wal_fsync"] = f"error: {str(e)}"
                    logger.error(f"Failed to store WAL fsync data: {str(e)}")
            
            # Store disk I/O data
            if state.get("disk_io_data"):
                try:
                    disk_io_result = await self.disk_io_store.store_disk_io_metrics(
                        state["testing_id"],
                        state["disk_io_data"]
                    )
                    if disk_io_result.get("status") == "success":
                        storage_results["disk_io"] = "success"
                        storage_results["disk_io_details"] = disk_io_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        summary_data = await self.disk_io_store.get_disk_io_summary(state["testing_id"])
                        if summary_data.get("status") == "success":
                            print_disk_io_summary_tables(summary_data)
                        logger.info("Stored disk I/O metrics in DuckDB")
                    else:
                        storage_results["disk_io"] = f"error: {disk_io_result.get('error')}"
                        logger.error(f"Failed to store disk I/O data: {disk_io_result.get('error')}")
                except Exception as e:
                    storage_results["disk_io"] = f"error: {str(e)}"
                    logger.error(f"Failed to store disk I/O data: {str(e)}")
            
            # Store network I/O data
            if state.get("network_io_data"):
                try:
                    network_io_result = await self.network_io_store.store_network_io_metrics(
                        state["testing_id"],
                        state["network_io_data"]
                    )
                    if network_io_result.get("status") == "success":
                        storage_results["network_io"] = "success"
                        storage_results["network_io_details"] = network_io_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        summary_data = await self.network_io_store.get_network_io_summary(state["testing_id"])
                        if summary_data.get("status") == "success":
                            print_network_io_summary_tables(summary_data)
                        logger.info("Stored network I/O metrics in DuckDB")
                    else:
                        storage_results["network_io"] = f"error: {network_io_result.get('error')}"
                        logger.error(f"Failed to store network I/O data: {network_io_result.get('error')}")
                except Exception as e:
                    storage_results["network_io"] = f"error: {str(e)}"
                    logger.error(f"Failed to store network I/O data: {str(e)}")
            
            # Store backend commit data
            if state.get("backend_commit_data"):
                try:
                    backend_commit_result = await self.backend_commit_store.store_backend_commit_metrics(
                        state["testing_id"],
                        state["backend_commit_data"]
                    )
                    if backend_commit_result.get("status") == "success":
                        storage_results["backend_commit"] = "success"
                        storage_results["backend_commit_details"] = backend_commit_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        summary_data = await self.backend_commit_store.get_backend_commit_summary(state["testing_id"])
                        if summary_data.get("status") == "success":
                            print_backend_commit_summary_tables(summary_data)
                        logger.info("Stored backend commit metrics in DuckDB")
                    else:
                        storage_results["backend_commit"] = f"error: {backend_commit_result.get('error')}"
                        logger.error(f"Failed to store backend commit data: {backend_commit_result.get('error')}")
                except Exception as e:
                    storage_results["backend_commit"] = f"error: {str(e)}"
                    logger.error(f"Failed to store backend commit data: {str(e)}")
            
            # Store compact defrag data
            if state.get("compact_defrag_data"):
                try:
                    compact_defrag_result = await self.compact_defrag_store.store_compact_defrag_metrics(
                        state["testing_id"],
                        state["compact_defrag_data"]
                    )
                    if compact_defrag_result.get("status") == "success":
                        storage_results["compact_defrag"] = "success"
                        storage_results["compact_defrag_details"] = compact_defrag_result.get("storage_results", {})
                        # Print the summary tables in terminal
                        summary_data = await self.compact_defrag_store.get_compact_defrag_summary(state["testing_id"])
                        if summary_data.get("status") == "success":
                            print_compact_defrag_summary_tables(summary_data)
                        logger.info("Stored compact defrag metrics in DuckDB")
                    else:
                        storage_results["compact_defrag"] = f"error: {compact_defrag_result.get('error')}"
                        logger.error(f"Failed to store compact defrag data: {compact_defrag_result.get('error')}")
                except Exception as e:
                    storage_results["compact_defrag"] = f"error: {str(e)}"
                    logger.error(f"Failed to store compact defrag data: {str(e)}")
            
            return {
                **state,
                "results": {**state.get("results", {}), "storage_results": storage_results},
                "messages": state["messages"] + [
                    AIMessage(content="Successfully stored data in DuckDB")
                ]
            }
            
        except Exception as e:
            error_msg = f"Error storing data: {str(e)}"
            logger.error(error_msg)
            return {
                **state,
                "error": error_msg,
                "messages": state["messages"] + [AIMessage(content=error_msg)]
            }

    async def _finalize_node(self, state: AgentState) -> AgentState:
        """Finalize results with all metrics analysis summaries including general info"""
        try:
            # Get cluster analysis summary
            cluster_summary = None
            if state.get("cluster_info"):
                cluster_summary = await self.cluster_store.get_cluster_info(
                    state["testing_id"]
                )
            
            # Get general info summary (NEW)
            general_info_summary = None
            if state.get("general_info_data"):
                general_info_summary = await self.general_info_store.get_general_info_summary(
                    state["testing_id"]
                )
            
            # Get WAL fsync summary
            wal_fsync_summary = None
            if state.get("wal_fsync_data"):
                wal_fsync_summary = await self.wal_fsync_store.generate_summary_report(
                    state["testing_id"]
                )
            
            # Get disk I/O summary
            disk_io_summary = None
            if state.get("disk_io_data"):
                disk_io_summary = await self.disk_io_store.get_disk_io_summary(
                    state["testing_id"]
                )
            
            # Get network I/O summary
            network_io_summary = None
            if state.get("network_io_data"):
                network_io_summary = await self.network_io_store.get_network_io_summary(
                    state["testing_id"]
                )
            
            # Get backend commit summary
            backend_commit_summary = None
            if state.get("backend_commit_data"):
                backend_commit_summary = await self.backend_commit_store.get_backend_commit_summary(
                    state["testing_id"]
                )
            
            # Get compact defrag summary
            compact_defrag_summary = None
            if state.get("compact_defrag_data"):
                compact_defrag_summary = await self.compact_defrag_store.get_compact_defrag_summary(
                    state["testing_id"]
                )
            
            results = {
                "testing_id": state["testing_id"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration": state["duration"],
                "start_time": state.get("start_time"),
                "end_time": state.get("end_time"),
                "cluster_summary": cluster_summary,
                "general_info_summary": general_info_summary,  # Added general info summary
                "wal_fsync_summary": wal_fsync_summary,
                "disk_io_summary": disk_io_summary,
                "network_io_summary": network_io_summary,
                "backend_commit_summary": backend_commit_summary,
                "compact_defrag_summary": compact_defrag_summary,
                "storage_results": state.get("results", {}).get("storage_results", {}),
                "status": "success"
            }
            
            return {
                **state,
                "results": results,
                "messages": state["messages"] + [
                    AIMessage(content="Analysis completed successfully")
                ]
            }
            
        except Exception as e:
            error_msg = f"Error finalizing results: {str(e)}"
            logger.error(error_msg)
            return {
                **state,
                "error": error_msg,
                "results": {"status": "error", "error": error_msg}
            }

    async def _handle_error_node(self, state: AgentState) -> AgentState:
        """Handle errors and prepare error response"""
        error_msg = state.get("error", "Unknown error occurred")
        logger.error(f"Handling error: {error_msg}")
        
        return {
            **state,
            "results": {
                "testing_id": state.get("testing_id"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "error", 
                "error": error_msg
            },
            "messages": state["messages"] + [
                AIMessage(content=f"Analysis failed: {error_msg}")
            ]
        }
    
    async def _call_mcp_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call MCP server tool via streamable HTTP"""
        url = f"{self.mcp_server_url}/mcp"
        
        try:
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, params or {})
                    json_data = json.loads(result.content[0].text)
                    return json_data
                    
        except Exception as e:
            logger.error(f"Error calling MCP tool {tool_name}: {str(e)}")
            raise
    
    async def _print_table_info(self):
        """Print table information in the terminal including general info tables"""
        try:
            # Print cluster info tables
            cluster_table_info = await self.cluster_store.get_table_info()
            logger.info("=== Cluster Info DuckDB Tables ===")
            for table_name, info in cluster_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print general info tables (NEW)
            general_info_table_info = await self.general_info_store.get_table_info()
            logger.info("=== General Info DuckDB Tables ===")
            for table_name, info in general_info_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print WAL fsync tables
            wal_fsync_table_info = await self.wal_fsync_store.get_table_info()
            logger.info("=== WAL Fsync DuckDB Tables ===")
            for table_name, info in wal_fsync_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print disk I/O tables
            disk_io_table_info = await self.disk_io_store.get_table_info()
            logger.info("=== Disk I/O DuckDB Tables ===")
            for table_name, info in disk_io_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print network I/O tables
            network_io_table_info = await self.network_io_store.get_table_info()
            logger.info("=== Network I/O DuckDB Tables ===")
            for table_name, info in network_io_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print backend commit tables
            backend_commit_table_info = await self.backend_commit_store.get_table_info()
            logger.info("=== Backend Commit DuckDB Tables ===")
            for table_name, info in backend_commit_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
            
            # Print compact defrag tables
            compact_defrag_table_info = await self.compact_defrag_store.get_table_info()
            logger.info("=== Compact Defrag DuckDB Tables ===")
            for table_name, info in compact_defrag_table_info.items():
                logger.info(f"Table: {table_name}")
                logger.info(f"Columns: {info}")
                logger.info("-" * 50)
                
        except Exception as e:
            logger.warning(f"Could not print table info: {str(e)}")

    async def analyze(self, query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run the complete analysis workflow including general info"""
        initial_state = AgentState(
            messages=[HumanMessage(content="Start ETCD analysis")],
            cluster_info=None,
            cluster_info_data={},
            wal_fsync_data=None,
            disk_io_data=None,
            network_io_data=None,
            backend_commit_data=None,
            compact_defrag_data=None,
            general_info_data=None,  # Added general info data initialization
            duration="1h",
            start_time=None,
            end_time=None,
            testing_id="",
            query_params=query_params or {},
            results={},
            error=None
        )
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            return final_state["results"]
            
        except Exception as e:
            logger.error(f"Analysis workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    async def query_by_duration(self, duration: str = "1h", print_table_info: bool = False, query_stored_only: bool = False) -> Dict[str, Any]:
        """Query all metrics data by duration including general info.
        If query_stored_only is True, returns results from DuckDB without collecting new data.
        """
        if query_stored_only:
            try:
                cluster = await self.cluster_store.query_cluster_info_by_duration(duration)
                general_info = await self.general_info_store.query_general_info_data_by_duration(duration)  # Added general info query
                wal = await self.wal_fsync_store.query_wal_fsync_data_by_duration(duration)
                disk = await self.disk_io_store.query_disk_io_data_by_duration(duration)
                network = await self.network_io_store.query_network_io_data_by_duration(duration)
                backend_commit = await self.backend_commit_store.query_backend_commit_data_by_duration(duration)
                compact_defrag = await self.compact_defrag_store.query_compact_defrag_data_by_duration(duration)
                return {
                    "status": "success",
                    "query_mode": "stored_only",
                    "duration": duration,
                    "cluster_info": cluster,
                    "general_info": general_info,  # Added general info results
                    "wal_fsync": wal,
                    "disk_io": disk,
                    "network_io": network,
                    "backend_commit": backend_commit,
                    "compact_defrag": compact_defrag,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                return {"status": "error", "error": str(e)}
        
        query_params = {
            "duration": duration,
            "print_table_info": print_table_info,
            "query_type": "duration"
        }
        result = await self.analyze(query_params)
        
        if print_table_info:
            await self._print_table_info()
        
        return result

    async def query_by_time_range(self, start_time: str, end_time: str, print_table_info: bool = False, query_stored_only: bool = False) -> Dict[str, Any]:
        """Query all metrics data by time range (UTC timezone) including general info.
        If query_stored_only is True, returns results from DuckDB without collecting new data.
        """
        if query_stored_only:
            try:
                cluster = await self.cluster_store.query_cluster_info_by_time_range(start_time, end_time)
                general_info = await self.general_info_store.query_general_info_data_by_time_range(start_time, end_time)  # Added general info query
                wal = await self.wal_fsync_store.query_wal_fsync_data_by_time_range(start_time, end_time)
                disk = await self.disk_io_store.query_disk_io_data_by_time_range(start_time, end_time)
                network = await self.network_io_store.query_network_io_data_by_time_range(start_time, end_time)
                backend_commit = await self.backend_commit_store.query_backend_commit_data_by_time_range(start_time, end_time)
                compact_defrag = await self.compact_defrag_store.query_compact_defrag_data_by_time_range(start_time, end_time)
                return {
                    "status": "success",
                    "query_mode": "stored_only",
                    "start_time": start_time,
                    "end_time": end_time,
                    "cluster_info": cluster,
                    "general_info": general_info,  # Added general info results
                    "wal_fsync": wal,
                    "disk_io": disk,
                    "network_io": network,
                    "backend_commit": backend_commit,
                    "compact_defrag": compact_defrag,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                return {"status": "error", "error": str(e)}
        
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "print_table_info": print_table_info,
            "query_type": "time_range"
        }
        result = await self.analyze(query_params)
        
        if print_table_info:
            await self._print_table_info()
        return result

async def main():
    """Example usage of the ETCD Analyzer MCP Agent with General Info support"""
    
    # Initialize the agent
    agent = ETCDAnalyzerStorDBMCPAgent()
    
    try:
 
        # Example 2: Query by specific time range (UTC) - collect new data including general info
        logger.info("=== Executing ETCD Workload Testing ===")
        start_time=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        #Run workload script and stream stdout in real-time
        proc = subprocess.Popen(
            ['bash', 'etcd-loads/etcd-load-tools.sh', 'all'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        if proc.stdout is not None:
            for line in proc.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
        return_code = proc.wait()
        if return_code != 0:
            logger.error(f"Workload script exited with code {return_code}")
        end_time=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info("=== Query by Time Range (UTC) - New Data Collection ===")
        result2 = await agent.query_by_time_range(start_time, end_time)
        # result = await agent.query_by_duration(duration="1h")
        # # print(result)
     

        
    except Exception as e:
        logger.error(f"Example execution failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())