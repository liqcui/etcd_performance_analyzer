#!/bin/bash
#
# OpenShift etcd Analyzer MCP Server Launch Script
# Usage: ./ocp_etcd_analyzer_command.sh [start|stop|restart|status|logs|client]
#

set -e

# Configuration
SERVER_SCRIPT="ocp_etcd_analyzer_mcp_server.py"
CLIENT_SCRIPT="ocp_etcd_analyzer_client_chat.py"
PID_FILE="/tmp/ocp_etcd_analyzer_mcp.pid"
LOG_FILE="/tmp/ocp_etcd_analyzer_mcp.log"
VENV_DIR="venv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check if script exists
check_script() {
    local script=$1
    if [[ ! -f "$script" ]]; then
        error "Script not found: $script"
        error "Please ensure you're running this from the correct directory"
        exit 1
    fi
}

# Check Python environment
check_python_env() {
    # Check if virtual environment exists
    if [[ ! -d "$VENV_DIR" ]]; then
        warn "Virtual environment not found. Creating one..."
        python3 -m venv "$VENV_DIR"
        log "Virtual environment created at $VENV_DIR"
    fi
    
    # Activate virtual environment
    source "$VENV_DIR/bin/activate"
    
    # Check if required packages are installed
    if ! python -c "import fastmcp" 2>/dev/null; then
        warn "Required packages not found. Installing..."
        pip install -r requirements.txt 2>/dev/null || {
            info "requirements.txt not found. Installing packages manually..."
            pip install "fastmcp>=1.12.4" "fastapi>=0.115.7" "pydantic>=2.0.0" \
                        "kubernetes>=29.0.0" "prometheus-api-client>=0.5.3" \
                        "requests>=2.31.0" "pyyaml>=6.0.1" "aiohttp>=3.8.0" \
                        "uvicorn>=0.20.0" "python-dateutil>=2.8.0" "pytz>=2023.3"
        }
        log "Required packages installed"
    fi
}

# Check KUBECONFIG
check_kubeconfig() {
    if [[ -z "$KUBECONFIG" ]]; then
        error "KUBECONFIG environment variable is not set"
        error "Please set KUBECONFIG to point to your OpenShift cluster configuration"
        error "Example: export KUBECONFIG=/path/to/your/kubeconfig"
        exit 1
    fi
    
    if [[ ! -f "$KUBECONFIG" ]]; then
        error "KUBECONFIG file not found: $KUBECONFIG"
        exit 1
    fi
    
    info "Using KUBECONFIG: $KUBECONFIG"
}

# Set timezone to UTC
set_timezone() {
    export TZ=UTC
    info "Timezone set to UTC"
}

# Start the MCP server
start_server() {
    log "Starting OpenShift etcd Analyzer MCP Server..."
    
    # Check prerequisites
    check_script "$SERVER_SCRIPT"
    check_python_env
    check_kubeconfig
    set_timezone
    
    # Check if server is already running
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        warn "Server is already running (PID: $(cat "$PID_FILE"))"
        return 0
    fi
    
    # Start server in background
    source "$VENV_DIR/bin/activate"
    nohup python "$SERVER_SCRIPT" > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!
    
    # Save PID
    echo $SERVER_PID > "$PID_FILE"
    
    # Wait a moment and check if server started successfully
    sleep 3
    if kill -0 $SERVER_PID 2>/dev/null; then
        log "Server started successfully (PID: $SERVER_PID)"
        log "Server logs: $LOG_FILE"
        log "Server should be available at: http://localhost:8080"
        
        # Test health endpoint
        info "Testing server health..."
        sleep 2
        if curl -s http://localhost:8080/health > /dev/null; then
            log "✅ Server health check passed"
        else
            warn "⚠️  Health check failed - server may still be starting up"
        fi
    else
        error "Failed to start server"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Stop the MCP server
stop_server() {
    log "Stopping OpenShift etcd Analyzer MCP Server..."
    
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            sleep 2
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                warn "Force killing server process..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            
            rm -f "$PID_FILE"
            log "Server stopped"
        else
            warn "Server process not found (PID: $pid)"
            rm -f "$PID_FILE"
        fi
    else
        warn "PID file not found. Server may not be running."
    fi
}

# Restart the server
restart_server() {
    log "Restarting OpenShift etcd Analyzer MCP Server..."
    stop_server
    sleep 1
    start_server
}

# Check server status
check_status() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "✅ Server is running (PID: $pid)"
            
            # Check health endpoint
            if curl -s http://localhost:8080/health > /dev/null; then
                log "✅ Server health check passed"
            else
                warn "⚠️  Server process running but health check failed"
            fi
        else
            warn "❌ PID file exists but process not running"
            rm -f "$PID_FILE"
        fi
    else
        info "❌ Server is not running"
    fi
}

# Show server logs
show_logs() {
    if [[ -f "$LOG_FILE" ]]; then
        log "Showing server logs (last 50 lines):"
        echo "----------------------------------------"
        tail -n 50 "$LOG_FILE"
        echo "----------------------------------------"
        info "Full logs available at: $LOG_FILE"
    else
        warn "Log file not found: $LOG_FILE"
    fi
}

# Start interactive client
start_client() {
    log "Starting etcd Analyzer Interactive Client..."
    
    check_script "$CLIENT_SCRIPT"
    check_python_env
    
    # Check if server is running
    if ! curl -s http://localhost:8080/health > /dev/null; then
        error "MCP Server is not running or not accessible"
        error "Please start the server first: $0 start"
        exit 1
    fi
    
    source "$VENV_DIR/bin/activate"
    python "$CLIENT_SCRIPT"
}

# Show usage information
show_usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start       Start the MCP server
    stop        Stop the MCP server  
    restart     Restart the MCP server
    status      Check server status
    logs        Show server logs
    client      Start interactive client
    help        Show this help message

Environment Variables:
    KUBECONFIG  Path to OpenShift/Kubernetes config file (required)
    TZ          Timezone (automatically set to UTC)

Examples:
    # Start the server
    export KUBECONFIG=/path/to/kubeconfig
    $0 start
    
    # Check status
    $0 status
    
    # Start interactive client
    $0 client
    
    # View logs
    $0 logs

Prerequisites:
    - Python 3.8+
    - Access to OpenShift cluster with KUBECONFIG
    - etcd monitoring enabled in OpenShift
    
For more information, see README.md
EOF
}

# Main script logic
case "${1:-help}" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        restart_server
        ;;
    status)
        check_status
        ;;
    logs)
        show_logs
        ;;
    client)
        start_client
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        error "Unknown command: $1"
        echo
        show_usage
        exit 1
        ;;
esac