import threading
import logging
from flask import Flask
from .discovery import DeviceDiscovery
from .protocol import MCPProtocol, SIMULATION_MODE

logger = logging.getLogger(__name__)

# Global reference to the MCP protocol handler
mcp_protocol = None
discovery_service = None
discovery_thread = None
simulator = None
simulator_thread = None
device_manager = None

def init_mcp(app: Flask):
    global mcp_protocol, discovery_service, discovery_thread, simulator, simulator_thread, device_manager
    
    logger.info("Initializing MCP Protocol service")
    mcp_protocol = MCPProtocol()
    
    # Initialize simulator if simulation mode is enabled
    if SIMULATION_MODE:
        try:
            from .simulator import init_simulator
            logger.info("Initializing device simulator")
            simulator = init_simulator(
                num_devices=app.config.get('MCP_SIMULATOR_DEVICES', 3),
                run_interval=app.config.get('MCP_SIMULATOR_INTERVAL', 5)
            )
            
            # Start simulator in a separate thread
            simulator_thread = threading.Thread(target=simulator.start_simulator, daemon=True)
            simulator_thread.start()
            logger.info("Device simulator started")
        except ImportError:
            logger.warning("Simulator module not available")
    
    logger.info("Initializing Device Discovery service")
    discovery_service = DeviceDiscovery(
        protocol=mcp_protocol,
        broadcast_address=app.config['MCP_BROADCAST_ADDRESS'],
        broadcast_port=app.config['MCP_BROADCAST_PORT'],
        discovery_interval=app.config['MCP_DISCOVERY_INTERVAL']
    )
    
    # Start the discovery service in a separate thread if auto-discovery is enabled
    if app.config['MCP_AUTO_DISCOVERY']:
        logger.info("Starting automatic device discovery")
        discovery_thread = threading.Thread(target=discovery_service.start_discovery_loop, daemon=True)
        discovery_thread.start()
    
    # Initialize device manager for data refresh
    try:
        from .device import init_device_manager
        logger.info("Initializing Device Manager")
        device_manager = init_device_manager(
            protocol=mcp_protocol,
            refresh_interval=app.config.get('DATA_REFRESH_INTERVAL')
        )
        logger.info("Device data refresh service started")
    except Exception as e:
        logger.error(f"Failed to initialize device manager: {str(e)}")
    
    logger.info("MCP service initialization complete")

def get_protocol():
    """Get the MCP protocol handler instance."""
    return mcp_protocol

def get_discovery_service():
    """Get the discovery service instance."""
    return discovery_service

def get_device_manager():
    """Get the device manager instance."""
    from .device import get_device_manager as get_dm
    return get_dm()

# Export key components
__all__ = ['mcp_protocol', 'discovery_service', 'init_mcp', 'get_protocol', 
           'get_discovery_service', 'get_device_manager']
