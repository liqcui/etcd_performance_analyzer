#!/usr/bin/env python3
"""
OpenShift etcd Analyzer MCP Client with LangGraph Integration
FastAPI server with streaming LLM responses and dynamic MCP tool integration
"""

import os
import sys
import json
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, AsyncGenerator, Callable
from datetime import datetime,timezone
import pytz
from contextlib import asynccontextmanager
import inspect
import traceback
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress noisy Uvicorn warnings for malformed probes
class _SuppressInvalidHttp(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "Invalid HTTP request received" not in msg

logging.getLogger("uvicorn.error").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.protocols.http.h11_impl").addFilter(_SuppressInvalidHttp())

try:
    from fastapi import FastAPI, HTTPException, Response
    from fastapi.responses import StreamingResponse, HTMLResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel,Field
    import uvicorn
    import httpx
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
    from langchain_core.tools import tool, BaseTool
    from langchain_openai import ChatOpenAI
    from langgraph.prebuilt import create_react_agent
    from langgraph.checkpoint.memory import MemorySaver
    
    # MCP imports
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
        
except ImportError as e:
    logger.error(f"Required dependencies not installed: {e}")
    logger.error("Please install: pip install fastapi uvicorn langchain-openai langgraph httpx")
    sys.exit(1)


# Pydantic models
class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = "default"


class ChatResponse(BaseModel):
    response: str
    conversation_id: str
    timestamp: str

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Check timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")


# Global variables
mcp_client = None
llm_agent = None
memory = None


class DynamicMCPTool(BaseTool):
    """Dynamically created LangChain tool that wraps MCP tool calls"""
    
    def __init__(self, tool_name: str, tool_description: str, mcp_client, parameters: List[Dict[str, Any]] = None):
        self.tool_name = tool_name
        self.mcp_client = mcp_client
        self.parameters = parameters or []
        
        super().__init__(
            name=tool_name,
            description=tool_description,
        )
    
    def _run(self, **kwargs) -> str:
        """Sync wrapper - not used in async context"""
        raise NotImplementedError("Use async version")
    
    async def _arun(self, **kwargs) -> str:
        """Async tool execution"""
        try:
            # Filter kwargs to only include valid parameters for this tool
            valid_params = {}
            for param in self.parameters:
                param_name = param.get('name')
                if param_name and param_name in kwargs:
                    valid_params[param_name] = kwargs[param_name]
            
            logger.info(f"Calling MCP tool {self.tool_name} with params: {valid_params}")
            result = await self.mcp_client.call_tool(self.tool_name, valid_params)
            return json.dumps(result, indent=2) if isinstance(result, dict) else str(result)
        except Exception as e:
            logger.error(f"Error calling tool {self.tool_name}: {e}")
            return f"Error calling tool {self.tool_name}: {str(e)}"


class ETCDMCPClient:
    """MCP Client for etcd analyzer server with dynamic tool discovery"""
    
    def __init__(self, server_url: str = None):
        self.mcp_server_url = server_url or os.getenv("MCP_SERVER_URL", "http://localhost:8000")
        self.tools = {}
        self.connection_timeout = 10.0
    
    async def connect(self):
        """Connect to the MCP server and discover available tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"
            logger.info(f"Connecting to MCP server at {url}")
            
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
                    logger.info(f"MCP Session ID: {session_id}")
            
            # List available tools
                    tools_result = await session.list_tools()
                    logger.info(f"Discovered {len(tools_result.tools)} MCP tools")
            
                    # Store tool information
            for tool_info in tools_result.tools:
                        self.tools[tool_info.name] = {
                            'name': tool_info.name,
                            'description': tool_info.description,
                            'input_schema': tool_info.inputSchema if hasattr(tool_info, 'inputSchema') else None
                        }
                        logger.info(f"Tool discovered: {tool_info.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            raise
    
    async def call_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Call an MCP tool with the provided connection pattern"""
        if tool_name not in self.tools:
            raise ValueError(f"Tool '{tool_name}' not available")
        
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
                    logger.info(f"Session ID in call_tool: {session_id}")
              
                    logger.info(f"Calling tool {tool_name} with params {params}")
                  
                    # Make a request to the server using HTTP, May convert the response to JSON if needed
                    # request_data = {
                    #     "params": params or {}
                    # }
                    request_data =  params or {}
                    
                    logger.info(f"Calling tool {tool_name} with request_data {request_data}")

                    result = await session.call_tool(tool_name, request_data)

                    # Prefer structured content from server if present
                    if getattr(result, "structuredContent", None):
                        return result.structuredContent

                    # Fallback to textual content
                    if result.content and len(result.content) > 0:
                        first = result.content[0]
                        text = getattr(first, "text", None)
                        if isinstance(text, str):
                            try:
                                return json.loads(text)
                            except Exception:
                                return {"result": text}
                    
                    return {"error": "No content returned from tool"}
            
        except Exception as e:
            traceback.print_stack()
            logger.error(f"Error calling tool '{tool_name}': {e}")
            return {"error": str(e)}
    
    async def check_mcp_connectivity_health(self):
        """Connect to MCP server and initialize tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(
                url
                # headers={"accept": "application/json"}
                ) as (
                    read_stream,
                    write_stream,
                    get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
 
                    # Get session id once connection established
                    session_id = get_session_id()
                    print("Session ID: in call_tool", session_id)
                    if session_id:
                       return {
                        "status": "healthy",
                        "mcp_connection": "ok",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    else:
                       return {
                        "status": "unhealthy",
                        "mcp_connection": "disconnected",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
        return {
            "status": "unhealthy",
            "mcp_connection": "disconnected", 
            "error": str(e),
            "last_check": datetime.now(timezone.utc).isoformat()
        }


    def _parse_parameters_from_description(self, description: str) -> List[Dict[str, Any]]:
        """Parse parameter information from tool description"""
        parameters = []
        
        # Look for Args: section in description
        args_match = re.search(r'Args:\s*\n(.*?)(?:\n\n|\nReturns:|$)', description, re.DOTALL)
        if args_match:
            args_text = args_match.group(1)
            # Parse parameter lines like "duration: Time range for metrics collection..."
            param_lines = re.findall(r'^\s*(\w+):\s*(.+?)(?=\n\s*\w+:|$)', args_text, re.MULTILINE | re.DOTALL)
            
            for param_name, param_desc in param_lines:
                param_info = {
                    'name': param_name.strip(),
                    'description': param_desc.strip(),
                    'type': 'string',  # Default to string
                    'required': False   # Default to optional
                }
                
                # Try to infer type from description
                param_desc_lower = param_desc.lower()
                if 'duration' in param_desc_lower or 'time' in param_desc_lower:
                    param_info['default'] = '1h'
                elif 'boolean' in param_desc_lower or 'true/false' in param_desc_lower:
                    param_info['type'] = 'boolean'
                elif 'number' in param_desc_lower or 'integer' in param_desc_lower:
                    param_info['type'] = 'number'
                
                parameters.append(param_info)
        
        return parameters
    
    def create_langchain_tools(self) -> List[BaseTool]:
        """Create LangChain tools from discovered MCP tools"""
        langchain_tools = []
        
        for tool_name, tool_info in self.tools.items():
            description = tool_info['description']
            parameters = self._parse_parameters_from_description(description)
            
            # Create dynamic tool
            dynamic_tool = DynamicMCPTool(
                tool_name=tool_name,
                tool_description=description,
                mcp_client=self,
                parameters=parameters
            )
            
            langchain_tools.append(dynamic_tool)
            logger.info(f"Created LangChain tool: {tool_name}")
        
        return langchain_tools


# Application lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan event handler"""
    # Startup
    logger.info("Starting OpenShift etcd Analyzer MCP Client...")
    
    global mcp_client, llm_agent, memory
    
    # Initialize MCP client
    mcp_client = ETCDMCPClient()
    
    # Connect to MCP server and discover tools
    try:
        await mcp_client.connect()
        logger.info("MCP client connection and tool discovery completed")
    except Exception as e:
        logger.error(f"Failed to initialize MCP client: {e}")
        raise
    
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not found - LLM features will be limited")
        yield
        return
    
    try:
        # Initialize LangGraph agent with memory
        memory = MemorySaver()
        
        # Initialize OpenAI LLM
        # llm = ChatOpenAI(
        #     model="gpt-4o-mini",
        #     temperature=0.1,
        #     streaming=True
        # )

        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")    

        self.llm = ChatOpenAI(
                model="gemini-1.5-flash",
                base_url=base_url,
                api_key=api_key,
                temperature=0.1,
                streaming=True         
            )  

        # Create dynamic tools from MCP server
        tools = mcp_client.create_langchain_tools()
        logger.info(f"Created {len(tools)} dynamic LangChain tools")
        
        llm_agent = create_react_agent(
            llm, 
            tools, 
            checkpointer=memory,
            state_modifier=SystemMessage(
                content="""You are an expert OpenShift etcd cluster analyst. You have access to various tools to collect etcd metrics and status information.

When users ask about etcd performance, health, or issues:
1. Use the appropriate tools to gather relevant metrics
2. Analyze the data and provide insights
3. Suggest recommendations when applicable
4. Format your responses clearly with key findings highlighted

Available time durations for metrics: '1h', '30m', '2h', '1d', etc.

Be proactive in gathering relevant data to provide comprehensive analysis.

The tools are dynamically discovered from the MCP server, so you have access to all available etcd analysis capabilities."""
            )
        )
        
        logger.info("LLM Agent initialized successfully with dynamic tools")
        
    except Exception as e:
        logger.error(f"Failed to initialize LLM agent: {e}")
        logger.warning("Continuing without LLM agent - basic API will still work")
    
    logger.info("Application startup completed")
    
    yield
    
    # Shutdown
    logger.info("Shutting down MCP Client...")


# Initialize FastAPI app
app = FastAPI(
    title="OpenShift etcd Analyzer Chat",
    description="Interactive chat interface for etcd cluster analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Mount webroot files if they exist (for other static assets like CSS, JS, images)
if os.path.isdir("webroot"):
    app.mount("/webroot", StaticFiles(directory="webroot"), name="webroot")

# Helper to load chat HTML from common locations with graceful fallback
def _load_chat_html() -> str:
    candidates = [
        os.getenv("CHAT_HTML_PATH"),
        os.path.join("webroot", "ocp_etcd_analyzer_llm.html"),
        os.path.join("html", "ocp_etcd_analyzer_llm.html"),
        "ocp_etcd_analyzer_llm.html",
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
    # Fallback minimal HTML if file not found
    return (
        """
        <!doctype html>
        <html lang=\"en\">
        <head>
          <meta charset=\"utf-8\">
          <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
          <title>OpenShift etcd Analyzer Chat</title>
          <style>
            body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 2rem; }
            textarea { width: 100%; height: 140px; }
            pre { background: #f6f8fa; padding: 1rem; border-radius: 6px; overflow:auto; }
            .row { margin: 1rem 0; }
          </style>
        </head>
        <body>
          <h2>OpenShift etcd Analyzer Chat</h2>
          <div class=\"row\">
            <label for=\"msg\">Message</label>
            <textarea id=\"msg\" placeholder=\"Ask about etcd status, metrics, or health...\"></textarea>
          </div>
          <div class=\"row\">
            <button id=\"send\">Send</button>
          </div>
          <h3>Response</h3>
          <pre id=\"out\"></pre>
          <script>
            const sendBtn = document.getElementById('send');
            const msgEl = document.getElementById('msg');
            const outEl = document.getElementById('out');
            sendBtn.onclick = async () => {
              outEl.textContent = 'Sending...';
              try {
                const res = await fetch('/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: msgEl.value, conversation_id: 'default' }) });
                const data = await res.json();
                outEl.textContent = data.response || JSON.stringify(data, null, 2);
              } catch (e) {
                outEl.textContent = 'Error: ' + e;
              }
            };
          </script>
        </body>
        </html>
        """
    )

# Serve the main HTML file at clean URLs
@app.get("/index.html", response_class=HTMLResponse)
async def serve_index():
    """Serve the chat interface at /index.html"""
    html_content = _load_chat_html()
    return HTMLResponse(content=html_content)

@app.get("/etcd_analyzer.html", response_class=HTMLResponse)  
async def serve_etcd_analyzer():
    """Serve the chat interface at /etcd_analyzer.html"""
    html_content = _load_chat_html()
    return HTMLResponse(content=html_content)


@app.get("/")
async def root():
    """Root endpoint with status"""
    return {
        "message": "OpenShift etcd Analyzer Chat API", 
        "status": "running",
        "endpoints": {
            "main_interface": "/index.html",
            "etcd_analyzer": "/etcd_analyzer.html", 
            "chat_interface": "/chat-interface",
            "health": "/health",
            "api_health": "/api/mcp/health",
            "cluster_health": "/api/cluster/health", 
            "tools": "/tools",
            "api_chat": "/chat",
            "streaming_chat": "/chat/stream"
        },
        "instructions": "Access the chat interface at /index.html or /etcd_analyzer.html"
    }


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.get("/chat-interface", response_class=HTMLResponse)
async def chat_interface():
    """Serve the chat interface HTML - redirects to index.html for cleaner URL"""
    html_content = _load_chat_html()
    return HTMLResponse(content=html_content)


# Health endpoints
@app.get("/api/mcp/health", response_model=HealthResponse)
async def mcp_health_check():
    """MCP connectivity health check endpoint"""
    health_data = await mcp_client.check_mcp_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )


@app.get("/api/cluster/health")
async def cluster_health_check():
    """Cluster health check endpoint matching HTML expectations"""
    if not mcp_client:
        return {
            "status": "unhealthy",
            "error": "MCP client not initialized",
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }
    
    try:
        # Get both cluster status and server health
        cluster_status = await mcp_client.call_tool("get_server_health")
        
        # Determine overall cluster health
        overall_health = "healthy"
        if isinstance(cluster_status, dict):
            if cluster_status.get("status") != "healthy":
                overall_health = "degraded"
        
        return {
            "status": "healthy",
            "details": {
                "overall_cluster_health": overall_health,
                "etcd_status": cluster_status,
                "health_details": {
                    "overall_cluster_health": overall_health
                }
            },
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "details": {
                "overall_cluster_health": "unknown",
                "health_details": {
                    "overall_cluster_health": "unknown"
                }
            },
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }


@app.post("/chat")
async def chat(request: ChatRequest) -> ChatResponse:
    """Non-streaming chat endpoint"""
    try:
        if not llm_agent:
            # Fallback response when LLM is not available
            if "status" in request.message.lower():
                result = await mcp_client.call_tool("get_etcd_cluster_status")
                response_text = f"Here's the current etcd cluster status:\n\n{json.dumps(result, indent=2)}"
            elif "health" in request.message.lower():
                response_text = "System is running. For detailed analysis, please ensure OpenAI API key is configured."
            else:
                response_text = "I'm running in limited mode. I can provide basic etcd status information, but full analysis requires OpenAI API configuration."
            
            return ChatResponse(
                response=response_text,
                conversation_id=request.conversation_id,
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        # Create conversation config
        config = {"configurable": {"thread_id": request.conversation_id}}
        
        # Get response from agent
        response = await llm_agent.ainvoke(
            {"messages": [HumanMessage(content=request.message)]},
            config=config
        )
        
        # Extract the response content
        response_text = ""
        if response and "messages" in response:
            for msg in response["messages"]:
                if isinstance(msg, AIMessage):
                    response_text += msg.content
        
        return ChatResponse(
            response=response_text or "I'm sorry, I couldn't generate a response.",
            conversation_id=request.conversation_id,
            timestamp=datetime.now(pytz.UTC).isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming chat endpoint"""
    
    async def generate_response():
        try:
            if not llm_agent:
                yield f"data: {json.dumps({'type': 'message', 'content': 'LLM agent not available. Using basic mode.'})}\n\n"
                return
            
            # Create conversation config
            config = {"configurable": {"thread_id": request.conversation_id}}
            
            # Stream response from agent
            response_text = ""
            async for chunk in llm_agent.astream(
                {"messages": [HumanMessage(content=request.message)]},
                config=config
            ):
                if "messages" in chunk:
                    for message in chunk["messages"]:
                        if isinstance(message, AIMessage):
                            # Get new content (delta)
                            new_content = message.content[len(response_text):]
                            if new_content:
                                response_text += new_content
                                yield f"data: {json.dumps({'type': 'message', 'content': new_content})}\n\n"
            
        except Exception as e:
            logger.error(f"Error in streaming chat: {e}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(pytz.UTC).isoformat(),
        "mcp_connected": mcp_client is not None,
        "tools_discovered": len(mcp_client.tools) if mcp_client else 0,
        "agent_initialized": llm_agent is not None,
        "openai_configured": bool(os.getenv("OPENAI_API_KEY"))
    }


@app.get("/tools")
async def list_tools():
    """List available MCP tools"""
    if not mcp_client:
        raise HTTPException(status_code=503, detail="MCP client not initialized")
    
    tools_info = []
    for tool_name, tool_data in mcp_client.tools.items():
        tools_info.append({
            "name": tool_name,
            "description": tool_data.get("description", ""),
            "parameters": mcp_client._parse_parameters_from_description(tool_data.get("description", ""))
        })
    
    return {
        "tools": tools_info,
        "count": len(mcp_client.tools)
    }


@app.get("/test-mcp/{tool_name}")
async def test_mcp_tool(tool_name: str, duration: str = "1h"):
    """Test endpoint for MCP tools"""
    if not mcp_client:
        raise HTTPException(status_code=503, detail="MCP client not initialized")
    
    try:
        # Automatically determine parameters based on tool
        params = {}
        if tool_name in mcp_client.tools:
            tool_info = mcp_client.tools[tool_name]
            parameters = mcp_client._parse_parameters_from_description(tool_info.get("description", ""))
            
            # Set default values for known parameters
            for param in parameters:
                param_name = param.get('name')
                if param_name == 'duration':
                    params[param_name] = duration
        logger.info("-"*60)
        logger.info(f"tool_name is {tool_name}, params is {params}")
        result = await mcp_client.call_tool(tool_name, params)
        # logger.info("-"*60)
        # logger.info("result is %s in /test-mcp/tools", result)
        return {
            "tool": tool_name,
            "params_used": params,
            "result": result
        }
    except Exception as e:
        traceback.print_stack()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Run the server - note the port is now configurable
    port = int(os.getenv("CLIENT_PORT", "8080"))
    host = os.getenv("CLIENT_HOST", "0.0.0.0")
    
    logger.info(f"Starting server on {host}:{port}")
    
    uvicorn.run(
        app,  # Pass the app directly, not as a string
        host=host,
        port=port,
        reload=False,
        log_level=os.getenv("CLIENT_LOG_LEVEL", "info")
    )