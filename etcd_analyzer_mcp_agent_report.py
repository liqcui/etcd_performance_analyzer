#!/usr/bin/env python3
"""
OVNK Analyzer MCP Agent Report
AI agent using LangGraph StateGraph to integrate with MCP server for etcd performance analysis
"""

import asyncio
import logging
import json
from typing import Dict, List, Any, Optional, TypedDict, Annotated
from datetime import datetime
import pytz
from dataclasses import dataclass
import traceback
import os
from dotenv import load_dotenv
# LangGraph imports
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# MCP imports for streamable HTTP
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.session import ClientSession

# Import analysis modules
from analysis.etcd_analyzer_performance_report import etcdReportAnalyzer
from analysis.etcd_analyzer_performance_utility import etcdAnalyzerUtility

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MCPServerConfig:
    """MCP Server configuration"""
    host: str = "localhost"
    port: int = 8000
    base_url: str = None
    
    def __post_init__(self):
        if self.base_url is None:
            self.base_url = f"http://{self.host}:{self.port}"

class AgentState(TypedDict):
    """State for the LangGraph agent"""
    messages: Annotated[list, add_messages]
    metrics_data: Optional[Dict[str, Any]]
    analysis_results: Optional[Dict[str, Any]]
    performance_report: Optional[str]
    error: Optional[str]
    test_id: Optional[str]
    duration: str
    start_time: Optional[datetime]
    end_time: Optional[datetime] 

class OVNKAnalyzerMCPAgent:
    """AI agent for etcd performance analysis using MCP server integration"""
    
    def __init__(self,mcp_server_url: str = "http://localhost:8000"):
        self.utility = etcdAnalyzerUtility()
        self.report_analyzer = etcdReportAnalyzer()
        self.timezone = pytz.UTC
        self.mcp_server_url=mcp_server_url
        
        # Add LLM client for AI-based analysis
        # self.llm = ChatOpenAI(model="gpt-4", temperature=0.1)
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")        

        self.llm = ChatOpenAI(
            model="gemini-2.5-pro",
            base_url=base_url,
            api_key=api_key,
            temperature=0.1,
            streaming=True
        )


        # Initialize LangGraph
        self.graph = self._build_graph() 

    def _build_graph(self) -> StateGraph:
        """Build the LangGraph StateGraph for the agent workflow"""
        
        # Create the graph
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("initialize", self._initialize_node)
        workflow.add_node("collect_metrics", self._collect_metrics_node)
        workflow.add_node("analyze_performance", self._analyze_performance_node)
        workflow.add_node("advanced_analysis", self._advanced_analysis_node)  # New node
        workflow.add_node("generate_report", self._generate_report_node)
        workflow.add_node("display_results", self._display_results_node)
        
        # Add edges
        workflow.set_entry_point("initialize")
        workflow.add_edge("initialize", "collect_metrics")
        workflow.add_edge("collect_metrics", "analyze_performance")
        workflow.add_edge("analyze_performance", "advanced_analysis")  # New edge
        workflow.add_edge("advanced_analysis", "generate_report")
        workflow.add_edge("generate_report", "display_results")
        workflow.add_edge("display_results", END)
        
        return workflow.compile()

    async def _initialize_node(self, state: AgentState) -> AgentState:
        """Initialize the analysis session"""
        logger.info("Initializing etcd performance analysis session...")
        
        test_id = self.utility.generate_test_id()
        
        # Handle time-based parameters - support both modes
        start_time = state.get("start_time")
        end_time = state.get("end_time")
        duration = state.get("duration", "1h")  # Default duration mode
        
        mode_info = ""
        
        # Mode 1: Time range mode (start_time and end_time provided)
        if start_time and end_time:
            # Validate time range
            if end_time <= start_time:
                error_msg = "end_time must be after start_time"
                state["error"] = error_msg
                state["messages"].append(AIMessage(content=error_msg))
                return state
            
            # Calculate duration for display purposes
            calculated_duration = end_time - start_time
            hours = calculated_duration.total_seconds() / 3600
            if hours >= 1:
                duration_display = f"{int(hours)}h"
                if calculated_duration.total_seconds() % 3600 > 0:
                    minutes = int((calculated_duration.total_seconds() % 3600) / 60)
                    if minutes > 0:
                        duration_display += f"{minutes}m"
            else:
                minutes = int(calculated_duration.total_seconds() / 60)
                duration_display = f"{minutes}m"
            
            mode_info = f"Time Range Mode: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')} (duration: {duration_display})"
            logger.info(f"Using time range mode: {mode_info}")
            
        # Mode 2: Duration mode (default)
        else:
            mode_info = f"Duration Mode: {duration}"
            logger.info(f"Using duration mode: {duration}")
        
        state["test_id"] = test_id
        state["duration"] = duration  # Keep original duration for duration mode
        
        state["messages"].append(
            AIMessage(content=f"Starting etcd performance analysis with test ID: {test_id}. {mode_info}")
        )
        
        return state

    async def _collect_metrics_node(self, state: AgentState) -> AgentState:
        """Collect metrics from MCP server"""
        logger.info("Collecting etcd performance metrics via MCP server...")
        
        try:
            # Prepare parameters for MCP call
            params = {"duration": state["duration"]}
            
            # Add time range parameters if available (time range mode)
            if state.get("start_time") and state.get("end_time"):
                params["start_time"] = state["start_time"].isoformat()
                params["end_time"] = state["end_time"].isoformat()
                logger.info(f"Using time range mode: {params['start_time']} to {params['end_time']}")
            else:
                logger.info(f"Using duration mode: {params['duration']}")
            
            # Call MCP server to get performance deep drive data
            metrics_data = await self._call_mcp_tool(
                "get_etcd_performance_deep_drive", 
                params
            )
            
            if metrics_data and metrics_data.get("status") == "success":
                state["metrics_data"] = metrics_data
                state["messages"].append(
                    AIMessage(content="Successfully collected etcd performance metrics")
                )
                logger.info("Metrics collection completed successfully")
            else:
                error_msg = f"Failed to collect metrics: {metrics_data.get('error', 'Unknown error')}"
                state["error"] = error_msg
                state["messages"].append(AIMessage(content=error_msg))
                logger.error(error_msg)
                
        except Exception as e:
            traceback.print_exc()
            error_msg = f"Error collecting metrics: {str(e)}"
            state["error"] = error_msg
            state["messages"].append(AIMessage(content=error_msg))
            logger.error(error_msg)
            
        return state
            
    async def _analyze_performance_node(self, state: AgentState) -> AgentState:
        """Analyze the collected performance metrics"""
        logger.info("Analyzing etcd performance metrics...")
        
        if state.get("error") or not state.get("metrics_data"):
            return state
            
        try:
            metrics_data = state["metrics_data"]
            
            # Perform comprehensive performance analysis
            analysis_results = self.report_analyzer.analyze_performance_metrics(
                metrics_data, 
                state["test_id"]
            )
            
            state["analysis_results"] = analysis_results
            state["messages"].append(
                AIMessage(content="Performance analysis completed")
            )
            logger.info("Performance analysis completed successfully")
            
        except Exception as e:
            error_msg = f"Error analyzing performance: {str(e)}"
            state["error"] = error_msg
            state["messages"].append(AIMessage(content=error_msg))
            logger.error(error_msg)
            
        return state

    async def _advanced_analysis_node(self, state: AgentState) -> AgentState:
        """Perform advanced root cause analysis using AI when thresholds fail"""
        logger.info("Performing advanced root cause analysis...")
        
        if state.get("error") or not state.get("analysis_results"):
            return state
            
        try:
            analysis_results = state["analysis_results"]
            metrics_data = state["metrics_data"]
            
            # Check if any thresholds failed
            failed_thresholds = self._identify_failed_thresholds(analysis_results)
            
            if failed_thresholds:
                # Perform script-based analysis first (from report analyzer)
                script_analysis = await self.report_analyzer.script_based_root_cause_analysis(
                    failed_thresholds, metrics_data
                )
                
                # Perform AI-based analysis
                ai_analysis = await self._ai_based_root_cause_analysis(
                    failed_thresholds, metrics_data, script_analysis
                )
                
                # Add advanced analysis to results
                analysis_results['advanced_root_cause'] = {
                    'failed_thresholds': failed_thresholds,
                    'script_analysis': script_analysis,
                    'ai_analysis': ai_analysis
                }
                
                state["analysis_results"] = analysis_results
                state["messages"].append(
                    AIMessage(content=f"Advanced root cause analysis completed - {len(failed_thresholds)} threshold failures detected")
                )
            else:
                state["messages"].append(
                    AIMessage(content="All thresholds within acceptable ranges - no advanced analysis required")
                )
                
        except Exception as e:
            error_msg = f"Error in advanced analysis: {str(e)}"
            state["messages"].append(AIMessage(content=error_msg))
            logger.error(error_msg)
            
        return state

    def _identify_failed_thresholds(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify which thresholds have failed"""
        failed_thresholds = []
        
        try:
            critical_analysis = analysis_results.get('critical_metrics_analysis', {})
            performance_summary = analysis_results.get('performance_summary', {})
            
            # Check WAL fsync failures
            wal_analysis = critical_analysis.get('wal_fsync_analysis', {})
            if wal_analysis.get('health_status') in ['critical', 'warning']:
                cluster_summary = wal_analysis.get('cluster_summary', {})
                failed_thresholds.append({
                    'metric': 'wal_fsync_p99',
                    'threshold_type': 'latency',
                    'current_value': cluster_summary.get('avg_latency_ms', 0),
                    'threshold_value': 10.0,  # 10ms threshold
                    'severity': wal_analysis.get('health_status'),
                    'pods_affected': cluster_summary.get('pods_with_issues', 0),
                    'total_pods': cluster_summary.get('total_pods', 0)
                })
            
            # Check backend commit failures
            backend_analysis = critical_analysis.get('backend_commit_analysis', {})
            if backend_analysis.get('health_status') in ['critical', 'warning']:
                cluster_summary = backend_analysis.get('cluster_summary', {})
                failed_thresholds.append({
                    'metric': 'backend_commit_p99',
                    'threshold_type': 'latency',
                    'current_value': cluster_summary.get('avg_latency_ms', 0),
                    'threshold_value': 25.0,  # 25ms threshold
                    'severity': backend_analysis.get('health_status'),
                    'pods_affected': cluster_summary.get('pods_with_issues', 0),
                    'total_pods': cluster_summary.get('total_pods', 0)
                })
            
            # Check CPU failures
            cpu_analysis = performance_summary.get('cpu_analysis', {})
            if cpu_analysis.get('health_status') in ['critical', 'warning']:
                cluster_summary = cpu_analysis.get('cluster_summary', {})
                failed_thresholds.append({
                    'metric': 'cpu_usage',
                    'threshold_type': 'utilization',
                    'current_value': cluster_summary.get('avg_usage', 0),
                    'threshold_value': 70.0,
                    'severity': cpu_analysis.get('health_status'),
                    'pods_affected': cluster_summary.get('critical_pods', 0) + cluster_summary.get('warning_pods', 0),
                    'total_pods': cluster_summary.get('total_pods', 0)
                })
                
        except Exception as e:
            logger.error(f"Error identifying failed thresholds: {e}")
            
        return failed_thresholds

    async def _ai_based_root_cause_analysis(self, failed_thresholds: List[Dict[str, Any]], 
                                          metrics_data: Dict[str, Any], 
                                          script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Use AI to perform advanced root cause analysis"""
        try:
            # Prepare context for AI analysis
            context = self._prepare_cluster_overview(metrics_data)
            
            # Create prompt for AI analysis
            prompt = ChatPromptTemplate.from_template("""
            You are an expert etcd performance analyst. Analyze the following etcd cluster performance data and provide root cause analysis.

            Failed Thresholds:
            {failed_thresholds}

            Script-based Analysis:
            {script_analysis}

            Cluster Overview:
            {cluster_overview}

            Please provide:
            1. Primary root cause identification with confidence level (1-10)
            2. Secondary contributing factors
            3. Evidence supporting your analysis
            4. Specific technical recommendations with implementation priority
            5. Risk assessment if issues are not addressed

            Focus on the most likely causes based on etcd best practices:
            - Disk I/O performance (most common)
            - CPU starvation
            - Network connectivity issues
            - Memory pressure
            - Database maintenance issues

            Provide response in JSON format with structured analysis.
            """)
            
            # Get AI analysis
            chain = prompt | self.llm
            response = await chain.ainvoke({
                'failed_thresholds': str(failed_thresholds),
                'script_analysis': str(script_analysis),
                'cluster_overview': str(context)
            })
            
            # Parse AI response
            try:
                import re
                json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
                if json_match:
                    ai_analysis = json.loads(json_match.group())
                else:
                    ai_analysis = {'raw_response': response.content}
            except:
                ai_analysis = {'raw_response': response.content}
                
            return ai_analysis
            
        except Exception as e:
            logger.error(f"Error in AI-based root cause analysis: {e}")
            return {'error': str(e), 'fallback': 'AI analysis unavailable'}

    async def _generate_report_node(self, state: AgentState) -> AgentState:
        """Generate the performance report"""
        logger.info("Generating performance report...")
        
        if state.get("error") or not state.get("analysis_results"):
            return state
            
        try:
            analysis_results = state["analysis_results"]
            
            # Generate comprehensive report
            report = self.report_analyzer.generate_performance_report(
                analysis_results,
                state["test_id"],
                state["duration"]
            )
            
            state["performance_report"] = report
            state["messages"].append(
                AIMessage(content="Performance report generated successfully")
            )
            logger.info("Performance report generation completed")
            
        except Exception as e:
            error_msg = f"Error generating report: {str(e)}"
            state["error"] = error_msg
            state["messages"].append(AIMessage(content=error_msg))
            logger.error(error_msg)
            
        return state
    
    async def _display_results_node(self, state: AgentState) -> AgentState:
        """Display the final results with detailed root cause analysis"""
        logger.info("Displaying performance analysis results...")
        
        if state.get("error"):
            print(f"\n{'='*80}")
            print("ETCD PERFORMANCE ANALYSIS - ERROR REPORT")
            print(f"{'='*80}")
            print(f"Error: {state['error']}")
            print(f"{'='*80}\n")
            return state
            
        if state.get("performance_report"):
            # Display the comprehensive report
            print(state["performance_report"])
            
            # Display detailed root cause analysis if available
            analysis_results = state.get("analysis_results", {})
            advanced_analysis = analysis_results.get('advanced_root_cause', {})
            
            if advanced_analysis:
                print(f"\n{'='*100}")
                print("DETAILED ROOT CAUSE ANALYSIS")
                print(f"{'='*100}")
                
                # Display failed thresholds summary
                failed_thresholds = advanced_analysis.get('failed_thresholds', [])
                if failed_thresholds:
                    print(f"\nFAILED THRESHOLDS SUMMARY:")
                    print("-" * 50)
                    for threshold in failed_thresholds:
                        metric = threshold.get('metric', 'unknown')
                        severity = threshold.get('severity', 'unknown').upper()
                        current = threshold.get('current_value', 0)
                        target = threshold.get('threshold_value', 0)
                        pods_affected = threshold.get('pods_affected', 0)
                        total_pods = threshold.get('total_pods', 0)
                        
                        print(f"[{severity}] {metric.upper()}")
                        print(f"  Current Value: {current}")
                        print(f"  Threshold: {target}")
                        print(f"  Affected Pods: {pods_affected}/{total_pods}")
                        print()
                
                # Display script-based analysis
                script_analysis = advanced_analysis.get('script_analysis', {})
                self._display_script_analysis(script_analysis)
                
                # Display AI analysis
                ai_analysis = advanced_analysis.get('ai_analysis', {})
                self._display_ai_analysis(ai_analysis)
            
            state["messages"].append(
                AIMessage(content="Performance analysis report displayed successfully with detailed root cause analysis")
            )
        else:
            print("No performance report generated")
            
        return state

    async def _call_mcp_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call MCP server tool via streamable HTTP"""
        try:
            url = f"{self.mcp_server_url}/mcp"
            
            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(url) as (
                read_stream,
                write_stream,
                get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
                    # Get session id once connection established
                    session_id = get_session_id()
                    logger.info(f"Session ID: in call_tool {session_id}")
                    
                    logger.info(f"Calling tool {tool_name} with params {params}, type: {type(params)}")
                    
                    # Make a request to the server
                    request_data = params or {}
                    
                    logger.info(f"Calling tool {tool_name} with params {request_data}, type: {type(request_data)}")
                    
                    result = await session.call_tool(tool_name, request_data)
                    
                    # Defensive parsing of response content
                    if not result or not getattr(result, "content", None):
                        logger.error(f"{tool_name} returned empty response content")
                        return {"status": "error", "error": "Empty response from MCP tool"}

                    first_item = result.content[0]
                    text_payload = None
                    if hasattr(first_item, "text"):
                        text_payload = first_item.text
                    elif isinstance(first_item, dict):
                        text_payload = first_item.get("text")

                    if text_payload is None:
                        logger.error(f"{tool_name} returned non-text content: {type(first_item)}")
                        return {"status": "error", "error": "Non-text content received from MCP tool"}

                    logger.info(f"{tool_name} = {text_payload}")

                    try:
                        json_data = json.loads(text_payload)
                    except Exception as parse_err:
                        logger.error(f"Failed to parse MCP tool response as JSON: {parse_err}")
                        return {"status": "error", "error": f"Invalid JSON from MCP tool: {parse_err}", "raw": text_payload}

                    return json_data
                        
        except Exception as e:  # Fixed: removed ExceptionGroup syntax that doesn't exist in standard Python
            logger.error(f"Error calling MCP tool {tool_name}: {str(e)}")
            return {"status": "error", "error": f"MCP tool call failed: {str(e)}"}

    async def run_analysis(self, duration: str = "1h") -> Dict[str, Any]:
        """Run the complete performance analysis workflow"""
        logger.info(f"Starting etcd performance analysis for duration: {duration}")
        
        # Initialize state
        initial_state = {
            "messages": [
                SystemMessage(content="OVNK etcd Performance Analyzer Agent"),
                HumanMessage(content=f"Analyze etcd performance for duration: {duration}")
            ],
            "metrics_data": None,
            "analysis_results": None,
            "performance_report": None,
            "error": None,
            "test_id": None,
            "duration": duration
        }
        
        # Run the graph
        try:
            final_state = await self.graph.ainvoke(initial_state)
            return {
                "success": not bool(final_state.get("error")),
                "test_id": final_state.get("test_id"),
                "error": final_state.get("error"),
                "metrics_collected": bool(final_state.get("metrics_data")),
                "analysis_completed": bool(final_state.get("analysis_results")),
                "report_generated": bool(final_state.get("performance_report"))
            }
        except Exception as e:
            logger.error(f"Error running analysis workflow: {e}")
            print(f"\n{'='*80}")
            print("ETCD PERFORMANCE ANALYSIS - WORKFLOW ERROR")
            print(f"{'='*80}")
            print(f"Error: {str(e)}")
            print(f"{'='*80}\n")
            return {
                "success": False,
                "error": str(e),
                "metrics_collected": False,
                "analysis_completed": False,
                "report_generated": False
            }

    def _prepare_cluster_overview(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare cluster overview for AI analysis"""
        overview = {}
        
        try:
            data = metrics_data.get('data', {})
            
            # WAL fsync overview
            wal_data = data.get('wal_fsync_data', [])
            if wal_data:
                avg_latencies = [m.get('avg', 0) * 1000 for m in wal_data if 'p99' in m.get('metric_name', '')]
                overview['wal_fsync_avg_ms'] = round(sum(avg_latencies) / len(avg_latencies), 3) if avg_latencies else 0
            
            # Backend commit overview
            backend_data = data.get('backend_commit_data', [])
            if backend_data:
                avg_latencies = [m.get('avg', 0) * 1000 for m in backend_data if 'p99' in m.get('metric_name', '')]
                overview['backend_commit_avg_ms'] = round(sum(avg_latencies) / len(avg_latencies), 3) if avg_latencies else 0
            
            # CPU overview
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            if cpu_metrics:
                avg_cpu = sum(m.get('avg', 0) for m in cpu_metrics) / len(cpu_metrics)
                overview['avg_cpu_usage_percent'] = round(avg_cpu, 2)
            
            # Disk I/O overview
            disk_data = data.get('disk_io_data', [])
            write_throughput = [m for m in disk_data if 'write' in m.get('metric_name', '') and 'throughput' in m.get('metric_name', '')]
            if write_throughput:
                avg_throughput = sum(m.get('avg', 0) for m in write_throughput) / len(write_throughput)
                overview['avg_disk_write_mb_s'] = round(avg_throughput / (1024 * 1024), 2)
                
        except Exception as e:
            logger.error(f"Error preparing cluster overview: {e}")
            overview['error'] = str(e)
            
        return overview

    async def run_time_range_analysis(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Run performance analysis for a specific time range"""
        logger.info(f"Starting etcd performance analysis for time range: {start_time} to {end_time}")
        
        # Calculate duration for display
        duration = end_time - start_time
        hours = duration.total_seconds() / 3600
        if hours >= 1:
            duration_display = f"{int(hours)}h"
            if duration.total_seconds() % 3600 > 0:
                minutes = int((duration.total_seconds() % 3600) / 60)
                if minutes > 0:
                    duration_display += f"{minutes}m"
        else:
            minutes = int(duration.total_seconds() / 60)
            duration_display = f"{minutes}m"
        
        # Initialize state with time range
        initial_state = {
            "messages": [
                SystemMessage(content="OVNK etcd Performance Analyzer Agent"),
                HumanMessage(content=f"Analyze etcd performance for time range: {start_time} to {end_time}")
            ],
            "metrics_data": None,
            "analysis_results": None,
            "performance_report": None,
            "error": None,
            "test_id": None,
            "duration": duration_display,
            "start_time": start_time,
            "end_time": end_time
        }
        
        # Run the graph
        try:
            final_state = await self.graph.ainvoke(initial_state)
            return {
                "success": not bool(final_state.get("error")),
                "test_id": final_state.get("test_id"),
                "error": final_state.get("error"),
                "metrics_collected": bool(final_state.get("metrics_data")),
                "analysis_completed": bool(final_state.get("analysis_results")),
                "report_generated": bool(final_state.get("performance_report"))
            }
        except Exception as e:
            logger.error(f"Error running time range analysis workflow: {e}")
            print(f"\n{'='*80}")
            print("ETCD PERFORMANCE ANALYSIS - WORKFLOW ERROR")
            print(f"{'='*80}")
            print(f"Error: {str(e)}")
            print(f"{'='*80}\n")
            return {
                "success": False,
                "error": str(e),
                "metrics_collected": False,
                "analysis_completed": False,
                "report_generated": False
            }

    def _display_script_analysis(self, script_analysis: Dict[str, Any]) -> None:
        """Display detailed script-based root cause analysis"""
        if not script_analysis:
            return
            
        print(f"\nSCRIPT-BASED ROOT CAUSE ANALYSIS:")
        print("=" * 50)
        
        # Display disk I/O analysis
        disk_analysis = script_analysis.get('disk_io_analysis', {})
        if disk_analysis:
            print(f"\n🔍 DISK I/O PERFORMANCE ANALYSIS:")
            print("-" * 40)
            
            # Storage performance assessment
            performance_assessment = disk_analysis.get('disk_performance_assessment', {})
            if performance_assessment:
                write_perf = performance_assessment.get('write_throughput', {})
                if write_perf:
                    cluster_avg = write_perf.get('cluster_avg_mb_s', 0)
                    grade = write_perf.get('performance_grade', 'unknown').upper()
                    total_nodes = write_perf.get('total_nodes', 0)
                    
                    print(f"  Cluster Write Performance: {cluster_avg} MB/s across {total_nodes} nodes")
                    print(f"  Performance Grade: {grade}")
                    
                    if grade == 'POOR':
                        print(f"  ⚠️  LOW THROUGHPUT DETECTED - This is likely the PRIMARY root cause!")
                    elif grade == 'GOOD':
                        print(f"  ✅ Storage throughput appears adequate")
            
            # Bottleneck indicators
            bottlenecks = disk_analysis.get('io_bottleneck_indicators', [])
            if bottlenecks:
                print(f"\n  📊 DETECTED I/O BOTTLENECKS:")
                for bottleneck in bottlenecks:
                    severity = bottleneck.get('severity', 'unknown').upper()
                    node = bottleneck.get('node', 'unknown')
                    value = bottleneck.get('value', 'unknown')
                    description = bottleneck.get('description', 'No description')
                    
                    severity_icon = "🔴" if severity == "HIGH" else "🟡"
                    print(f"    {severity_icon} [{severity}] {node}: {value}")
                    print(f"        {description}")
            
            # Storage recommendations
            storage_recs = disk_analysis.get('storage_recommendations', [])
            if storage_recs:
                print(f"\n  💡 STORAGE OPTIMIZATION RECOMMENDATIONS:")
                for i, rec in enumerate(storage_recs, 1):
                    print(f"    {i}. {rec}")
        
        # Display network analysis
        network_analysis = script_analysis.get('network_analysis', {})
        if network_analysis:
            print(f"\n🌐 NETWORK PERFORMANCE ANALYSIS:")
            print("-" * 40)
            
            # Network health assessment
            health_assessment = network_analysis.get('network_health_assessment', {})
            if health_assessment:
                avg_latency = health_assessment.get('avg_peer_latency_ms', 0)
                network_grade = health_assessment.get('network_grade', 'unknown').upper()
                high_latency_pods = health_assessment.get('high_latency_pods', 0)
                total_pods = health_assessment.get('total_pods', 0)
                
                print(f"  Average Peer Latency: {avg_latency} ms")
                print(f"  Network Grade: {network_grade}")
                print(f"  High Latency Pods: {high_latency_pods}/{total_pods}")
                
                if network_grade == 'POOR':
                    print(f"  ⚠️  HIGH NETWORK LATENCY DETECTED - Contributing factor to performance issues!")
            
            # Connectivity issues
            connectivity_issues = network_analysis.get('connectivity_issues', [])
            if connectivity_issues:
                print(f"\n  📡 NETWORK CONNECTIVITY ISSUES:")
                for issue in connectivity_issues:
                    severity = issue.get('severity', 'unknown').upper()
                    description = issue.get('description', 'No description')
                    severity_icon = "🔴" if severity == "CRITICAL" else "🟡"
                    print(f"    {severity_icon} [{severity}] {description}")
            
            # Network recommendations
            network_recs = network_analysis.get('network_recommendations', [])
            if network_recs:
                print(f"\n  💡 NETWORK OPTIMIZATION RECOMMENDATIONS:")
                for i, rec in enumerate(network_recs, 1):
                    print(f"    {i}. {rec}")
        
        # Display consensus analysis
        consensus_analysis = script_analysis.get('consensus_analysis', {})
        if consensus_analysis:
            print(f"\n⚖️  CONSENSUS HEALTH ANALYSIS:")
            print("-" * 40)
            
            consensus_health = consensus_analysis.get('consensus_health', {})
            if consensus_health:
                risk_level = consensus_health.get('risk_level', 'unknown').upper()
                affected_pods = consensus_health.get('affected_pods', 0)
                
                if risk_level in ['HIGH', 'MEDIUM']:
                    risk_icon = "🔴" if risk_level == "HIGH" else "🟡"
                    print(f"  {risk_icon} CONSENSUS RISK LEVEL: {risk_level}")
                    print(f"  Affected Pods: {affected_pods}")
                    
                    leader_stability = consensus_analysis.get('leader_election_stability', {})
                    if leader_stability.get('potential_instability'):
                        reason = leader_stability.get('reason', 'Unknown reason')
                        affected_pods = leader_stability.get('affected_pods', [])
                        print(f"  ⚠️  LEADER ELECTION INSTABILITY RISK:")
                        print(f"    Reason: {reason}")
                        print(f"    Affected Pods: {', '.join(affected_pods)}")
            
            consensus_recs = consensus_analysis.get('consensus_recommendations', [])
            if consensus_recs:
                print(f"\n  💡 CONSENSUS STABILITY RECOMMENDATIONS:")
                for i, rec in enumerate(consensus_recs, 1):
                    print(f"    {i}. {rec}")
        
        # Display resource contention analysis
        resource_analysis = script_analysis.get('resource_contention_analysis', {})
        if resource_analysis:
            print(f"\n🖥️  RESOURCE CONTENTION ANALYSIS:")
            print("-" * 40)
            
            # CPU contention
            cpu_contention = resource_analysis.get('cpu_contention', {})
            cluster_cpu = cpu_contention.get('cluster_summary', {})
            if cluster_cpu:
                avg_cpu = cluster_cpu.get('avg_cpu_percent', 0)
                critical_pods = cluster_cpu.get('critical_usage_pods', 0)
                high_pods = cluster_cpu.get('high_usage_pods', 0)
                total_pods = cluster_cpu.get('total_pods', 0)
                
                print(f"  Average CPU Usage: {avg_cpu}% across {total_pods} pods")
                print(f"  High Usage Pods: {high_pods}, Critical Usage Pods: {critical_pods}")
                
                if critical_pods > 0:
                    print(f"  🔴 CPU STARVATION DETECTED - {critical_pods} pods in critical state!")
            
            # Resource contention indicators
            contention_indicators = resource_analysis.get('contention_indicators', [])
            if contention_indicators:
                print(f"\n  📊 RESOURCE CONTENTION INDICATORS:")
                for indicator in contention_indicators:
                    indicator_type = indicator.get('type', 'unknown')
                    pod = indicator.get('pod', 'unknown')
                    description = indicator.get('description', 'No description')
                    
                    if 'critical' in indicator_type:
                        print(f"    🔴 [CRITICAL] {pod}: {description}")
                    else:
                        print(f"    🟡 [WARNING] {pod}: {description}")
            
            resource_recs = resource_analysis.get('resource_recommendations', [])
            if resource_recs:
                print(f"\n  💡 RESOURCE OPTIMIZATION RECOMMENDATIONS:")
                for i, rec in enumerate(resource_recs, 1):
                    print(f"    {i}. {rec}")

    def _display_ai_analysis(self, ai_analysis: Dict[str, Any]) -> None:
        """Display AI-based root cause analysis results"""
        if not ai_analysis or ai_analysis.get('error'):
            if ai_analysis.get('fallback'):
                print(f"\n🤖 AI ANALYSIS: {ai_analysis['fallback']}")
            return
        
        print(f"\n🤖 AI-POWERED ROOT CAUSE ANALYSIS:")
        print("=" * 50)
        
        # Handle raw response format
        if 'raw_response' in ai_analysis:
            print(f"\n{ai_analysis['raw_response']}")
            return
        
        # Display structured AI analysis
        if 'primary_root_cause' in ai_analysis:
            primary_cause = ai_analysis['primary_root_cause']
            confidence = primary_cause.get('confidence_level', 'unknown')
            cause = primary_cause.get('cause', 'Not specified')
            
            print(f"\n🎯 PRIMARY ROOT CAUSE (Confidence: {confidence}/10):")
            print(f"  {cause}")
        
        if 'secondary_factors' in ai_analysis:
            secondary_factors = ai_analysis['secondary_factors']
            print(f"\n🔗 CONTRIBUTING FACTORS:")
            for i, factor in enumerate(secondary_factors, 1):
                print(f"  {i}. {factor}")
        
        if 'evidence' in ai_analysis:
            evidence = ai_analysis['evidence']
            print(f"\n🔍 SUPPORTING EVIDENCE:")
            for i, ev in enumerate(evidence, 1):
                print(f"  {i}. {ev}")
        
        if 'technical_recommendations' in ai_analysis:
            tech_recs = ai_analysis['technical_recommendations']
            print(f"\n⚙️  AI-GENERATED TECHNICAL RECOMMENDATIONS:")
            for i, rec in enumerate(tech_recs, 1):
                priority = rec.get('priority', 'medium').upper()
                recommendation = rec.get('recommendation', 'No recommendation')
                priority_icon = "🔴" if priority == "HIGH" else "🟡" if priority == "MEDIUM" else "🟢"
                print(f"  {i}. {priority_icon} [{priority}] {recommendation}")
        
        if 'risk_assessment' in ai_analysis:
            risk = ai_analysis['risk_assessment']
            print(f"\n⚠️  RISK ASSESSMENT:")
            print(f"  {risk}")
        
        # Summary of root cause confidence
        print(f"\n📋 ROOT CAUSE ANALYSIS SUMMARY:")
        print("-" * 40)
        
        # Determine most likely root cause based on analysis
        primary_causes = []
        
        ai_text = str(ai_analysis).lower()
        if ('disk' in ai_text) or ('storage' in ai_text):
            primary_causes.append("Storage I/O Performance (High Confidence)")
        
        if ('network' in ai_text) or ('latency' in ai_text):
            primary_causes.append("Network Connectivity Issues (Medium Confidence)")
        
        if ('cpu' in ai_text) or ('resource' in ai_text):
            primary_causes.append("Resource Contention (Medium Confidence)")
        
        if not primary_causes:
            primary_causes.append("Multiple factors contributing to performance degradation")
        
        for cause in primary_causes:
            print(f"  • {cause}")
        
        print(f"\n🎯 RECOMMENDED IMMEDIATE ACTIONS:")
        print("  1. Focus on storage performance optimization (highest impact)")
        print("  2. Verify network connectivity and latency between etcd nodes")
        print("  3. Ensure adequate CPU/memory resources for etcd pods")
        print("  4. Monitor for improvements after each optimization step")

    def _parse_unstructured_ai_response(self, response_content: str, 
                                    failed_thresholds: List[Dict[str, Any]], 
                                    script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Parse unstructured AI response into structured format"""
        # Fallback parsing for non-JSON AI responses
        response_lower = response_content.lower()
        
        analysis = {
            'primary_root_cause': {
                'cause': 'Analysis based on threshold failures',
                'confidence_level': 6,
                'technical_explanation': 'Extracted from AI response analysis'
            },
            'secondary_factors': [],
            'evidence_analysis': [],
            'technical_recommendations': [],
            'raw_response': response_content
        }
        
        # Try to extract key insights from the response
        if 'disk' in response_lower or 'storage' in response_lower or 'i/o' in response_lower:
            analysis['primary_root_cause']['cause'] = 'Storage I/O performance issues identified by AI analysis'
            analysis['secondary_factors'].append('Disk throughput or latency limitations')
        
        if 'network' in response_lower or 'latency' in response_lower:
            analysis['secondary_factors'].append('Network connectivity or latency issues')
        
        if 'cpu' in response_lower or 'resource' in response_lower:
            analysis['secondary_factors'].append('CPU or resource contention')
        
        # Generate basic recommendations
        analysis['technical_recommendations'] = self._generate_default_recommendations(failed_thresholds)
        
        return analysis

    def _generate_default_recommendations(self, failed_thresholds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate default technical recommendations based on failed thresholds"""
        recommendations = []
        
        # Check for latency threshold failures
        latency_failures = [t for t in failed_thresholds if t['threshold_type'] == 'latency']
        
        if any(t['metric'] == 'wal_fsync_p99' for t in latency_failures):
            recommendations.append({
                'priority': 'high',
                'recommendation': 'Upgrade to NVMe SSD storage with sustained write performance >100MB/s and <10ms latency',
                'implementation_details': 'Replace current storage with enterprise NVMe drives, ensure dedicated storage for etcd',
                'expected_impact': 'Should reduce WAL fsync latency to <5ms under normal load'
            })
        
        if any(t['metric'] == 'backend_commit_p99' for t in latency_failures):
            recommendations.append({
                'priority': 'high', 
                'recommendation': 'Optimize database backend storage and consider etcd defragmentation',
                'implementation_details': 'Schedule regular etcd defragmentation, verify storage IOPS capacity',
                'expected_impact': 'Should reduce backend commit latency to <15ms'
            })
        
        # Check for CPU failures
        cpu_failures = [t for t in failed_thresholds if t['metric'] == 'cpu_usage']
        if cpu_failures:
            recommendations.append({
                'priority': 'medium',
                'recommendation': 'Increase CPU resources and implement resource isolation for etcd pods',
                'implementation_details': 'Increase CPU requests/limits, use node affinity to place etcd on dedicated nodes',
                'expected_impact': 'Should reduce CPU utilization to <50% under normal load'
            })
        
        # General monitoring recommendation
        recommendations.append({
            'priority': 'low',
            'recommendation': 'Implement comprehensive etcd performance monitoring and alerting',
            'implementation_details': 'Set up Prometheus monitoring with etcd-specific dashboards and alerts',
            'expected_impact': 'Proactive detection of performance degradation'
        })
        
        return recommendations

    def _generate_fallback_analysis(self, failed_thresholds: List[Dict[str, Any]], 
                                script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate fallback analysis when AI analysis fails"""
        analysis = {
            'status': 'fallback_analysis',
            'primary_assessment': 'Rule-based analysis of performance issues',
            'identified_issues': [],
            'recommendations': []
        }
        
        # Analyze failed thresholds systematically
        latency_issues = [t for t in failed_thresholds if t['threshold_type'] == 'latency']
        cpu_issues = [t for t in failed_thresholds if t['metric'] == 'cpu_usage']
        
        if latency_issues:
            wal_issues = [t for t in latency_issues if t['metric'] == 'wal_fsync_p99']
            backend_issues = [t for t in latency_issues if t['metric'] == 'backend_commit_p99']
            
            if wal_issues:
                analysis['identified_issues'].append(
                    f"WAL fsync latency threshold exceeded - indicates storage I/O bottleneck"
                )
                analysis['recommendations'].append(
                    "Immediate: Upgrade to high-performance NVMe storage with <10ms write latency"
                )
            
            if backend_issues:
                analysis['identified_issues'].append(
                    f"Backend commit latency threshold exceeded - database write performance issue"
                )
                analysis['recommendations'].append(
                    "Immediate: Check storage IOPS capacity and consider etcd defragmentation"
                )
        
        if cpu_issues:
            analysis['identified_issues'].append(
                f"CPU utilization threshold exceeded - resource contention detected"
            )
            analysis['recommendations'].append(
                "Immediate: Increase CPU resources and implement workload isolation"
            )
        
        # Check script analysis for additional context
        if script_analysis:
            disk_analysis = script_analysis.get('disk_io_analysis', {})
            if disk_analysis.get('io_bottleneck_indicators'):
                analysis['identified_issues'].append(
                    "Storage bottlenecks confirmed by disk I/O analysis"
                )
        
        if not analysis['identified_issues']:
            analysis['identified_issues'].append("Performance degradation detected but specific cause unclear")
            analysis['recommendations'].append("Comprehensive system review recommended")
        
        return analysis

    def _calculate_root_cause_confidence(self, ai_analysis: Dict[str, Any], 
                                    script_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate confidence levels for root cause analysis"""
        confidence_assessment = {
            'overall_confidence': 'medium',
            'confidence_factors': [],
            'certainty_level': 6  # out of 10
        }
        
        try:
            # Check for consistent indicators across analysis methods
            consistent_indicators = 0
            
            # Check if AI and script analysis agree on storage issues
            ai_text = str(ai_analysis).lower()
            ai_mentions_storage = ('disk' in ai_text) or ('storage' in ai_text)
            
            script_has_storage_issues = bool(
                script_analysis.get('disk_io_analysis', {}).get('io_bottleneck_indicators', [])
            )
            
            if ai_mentions_storage and script_has_storage_issues:
                consistent_indicators += 1
                confidence_assessment['confidence_factors'].append(
                    "Storage issues consistently identified by both AI and script analysis"
                )
            
            # Check for multiple threshold failures (higher confidence)
            primary_cause = ai_analysis.get('primary_root_cause', {})
            confidence_level = primary_cause.get('confidence_level', 5)
            
            if confidence_level >= 8:
                consistent_indicators += 1
                confidence_assessment['confidence_factors'].append(
                    f"AI analysis reports high confidence ({confidence_level}/10)"
                )
            
            # Adjust overall confidence
            if consistent_indicators >= 2:
                confidence_assessment['overall_confidence'] = 'high'
                confidence_assessment['certainty_level'] = 8
            elif consistent_indicators == 1:
                confidence_assessment['overall_confidence'] = 'medium'
                confidence_assessment['certainty_level'] = 6
            else:
                confidence_assessment['overall_confidence'] = 'low'
                confidence_assessment['certainty_level'] = 4
                confidence_assessment['confidence_factors'].append(
                    "Limited correlation between different analysis methods"
                )
        
        except Exception as e:
            confidence_assessment['error'] = str(e)
            confidence_assessment['overall_confidence'] = 'uncertain'
        
        return confidence_assessment

    def _infer_secondary_factors(self, failed_thresholds: List[Dict[str, Any]], 
                            script_analysis: Dict[str, Any]) -> List[str]:
        """Infer secondary contributing factors"""
        factors = []
        
        # Check for multiple types of failures
        has_latency_issues = any(t['threshold_type'] == 'latency' for t in failed_thresholds)
        has_cpu_issues = any(t['metric'] == 'cpu_usage' for t in failed_thresholds)
        
        if has_latency_issues:
            factors.append("Disk I/O subsystem performance limitations")
            
            # Check if multiple pods affected
            latency_failures = [t for t in failed_thresholds if t['threshold_type'] == 'latency']
            total_affected_pods = sum(t.get('pods_affected', 0) for t in latency_failures)
            if total_affected_pods > 1:
                factors.append("Cluster-wide storage performance degradation")
        
        if has_cpu_issues:
            factors.append("Resource contention and CPU starvation")
        
        # Check script analysis for network issues
        if script_analysis:
            network_analysis = script_analysis.get('network_analysis', {})
            if network_analysis.get('connectivity_issues'):
                factors.append("Network connectivity or latency contributing to performance issues")
            
            consensus_analysis = script_analysis.get('consensus_analysis', {})
            if consensus_analysis.get('consensus_health', {}).get('risk_level') in ['high', 'medium']:
                factors.append("Potential consensus instability due to performance issues")
        
        # Add general factors if specific ones not identified
        if not factors:
            factors.extend([
                "System resource limitations",
                "Workload characteristics or scaling issues"
            ])
        
        return factors

async def main():
    """Main function to run the analyzer agent"""
    print("OVNK etcd Performance Analyzer Agent")
    print("=====================================")
    
    try:
        # Create and run agent
        agent = OVNKAnalyzerMCPAgent()
        
        # Allow user to specify duration or time range
        print("\nAnalysis Options:")
        print("1. Duration mode (e.g., 1h, 30m, 2h)")
        print("2. Time range mode (start and end times)")
        
        mode = input("Select mode (1 or 2, default: 1): ").strip() or "1"
        
        if mode == "2":
            # Time range mode
            print("\nEnter time range (UTC):")
            start_time_str = input("Start time (YYYY-MM-DD HH:MM:SS): ").strip()
            end_time_str = input("End time (YYYY-MM-DD HH:MM:SS): ").strip()
            
            if start_time_str and end_time_str:
                try:
                    from datetime import datetime
                    import pytz
                    
                    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                    end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                    start_time = pytz.UTC.localize(start_time)
                    end_time = pytz.UTC.localize(end_time)
                    
                    print(f"\nStarting analysis for time range: {start_time} to {end_time}")
                    print("-" * 50)
                    
                    # Run time range analysis
                    result = await agent.run_time_range_analysis(start_time, end_time)
                except ValueError as e:
                    print(f"Invalid time format: {e}")
                    print("Using default duration mode instead...")
                    duration = "1h"
                    result = await agent.run_analysis(duration)
            else:
                print("Missing time range, using default duration mode...")
                duration = "1h"
                result = await agent.run_analysis(duration)
        else:
            # Duration mode (default)
            duration = input("Enter analysis duration (default: 1h): ").strip() or "1h"
            
            print(f"\nStarting analysis for duration: {duration}")
            print("-" * 50)
            
            # Run analysis
            result = await agent.run_analysis(duration)
        
        # Display summary
        print(f"\n{'='*80}")
        print("ANALYSIS SUMMARY")
        print(f"{'='*80}")
        print(f"Success: {result['success']}")
        if result.get('test_id'):
            print(f"Test ID: {result['test_id']}")
        print(f"Metrics Collected: {result['metrics_collected']}")
        print(f"Analysis Completed: {result['analysis_completed']}")
        print(f"Report Generated: {result['report_generated']}")
        
        if result.get('error'):
            print(f"Error: {result['error']}")
        
        print(f"{'='*80}")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"Error in main function: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
