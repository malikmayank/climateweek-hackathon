import logging
import socket
import json
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from .sunspec import SunSpecModels

# Configure logger
logger = logging.getLogger(__name__)

# Flag to enable/disable simulation mode
SIMULATION_MODE = True

class MCPProtocol:
    """
    Implementation of the SunSpec Model Context Protocol (MCP).
    
    This class handles the core protocol functionality including message formatting,
    sending/receiving, and managing protocol-specific aspects of device communication.
    """
    
    def __init__(self):
        """Initialize the MCP protocol handler."""
        self.sunspec_models = SunSpecModels()
        logger.info("MCP Protocol handler initialized")
    
    def create_discovery_message(self) -> Dict[str, Any]:
        """
        Create a discovery message in MCP format.
        
        Returns:
            Dict containing the discovery message
        """
        message = {
            "mcp": {
                "version": "1.0",
                "type": "discovery",
                "timestamp": int(time.time())
            }
        }
        return message
    
    def create_read_request(self, context_path: str, point_ids: List[str] = None) -> Dict[str, Any]:
        """
        Create a read request message for a specific context path.
        
        Args:
            context_path: The context path to read from
            point_ids: Optional list of specific point IDs to read (None = read all)
            
        Returns:
            Dict containing the read request message
        """
        message = {
            "mcp": {
                "version": "1.0",
                "type": "read",
                "timestamp": int(time.time()),
                "context": context_path
            }
        }
        
        if point_ids:
            message["mcp"]["points"] = point_ids
            
        return message
    
    def create_write_request(self, context_path: str, points: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a write request message for a specific context path.
        
        Args:
            context_path: The context path to write to
            points: Dictionary of point_id -> value pairs to write
            
        Returns:
            Dict containing the write request message
        """
        message = {
            "mcp": {
                "version": "1.0",
                "type": "write",
                "timestamp": int(time.time()),
                "context": context_path,
                "points": points
            }
        }
        return message
    
    def send_message(self, message: Dict[str, Any], ip_address: str, port: int, 
                     timeout: int = 5) -> Optional[Dict[str, Any]]:
        """
        Send an MCP message to a device and receive the response.
        
        Args:
            message: The message to send
            ip_address: The device IP address
            port: The device port
            timeout: Timeout in seconds
            
        Returns:
            Dict containing the response or None if the operation failed
        """
        # If simulation mode is enabled and it's a simulated device, use simulator
        if SIMULATION_MODE and ip_address == "127.0.0.1":
            try:
                from .simulator import get_simulator
                simulator = get_simulator()
                
                if simulator:
                    # Get the message type and handle accordingly
                    msg_type = message.get('mcp', {}).get('type')
                    
                    if msg_type == 'device_info':
                        # Find the device by IP
                        for device in simulator.get_devices():
                            # Create a device info response
                            response = {
                                "mcp": {
                                    "version": "1.0",
                                    "type": "device_info_response",
                                    "timestamp": int(time.time()),
                                    "device": {
                                        "uuid": device['uuid'],
                                        "name": device['name'],
                                        "model": device['model'],
                                        "manufacturer": device.get('manufacturer', 'MCPHub Simulator'),
                                        "firmwareVersion": device.get('firmware_version', '1.0.0'),
                                        "protocolVersion": device.get('protocol_version', '1.0')
                                    }
                                }
                            }
                            return response
                    
                    elif msg_type == 'contexts':
                        # Find the device by IP
                        for device in simulator.get_devices():
                            # Create a contexts response
                            contexts_list = []
                            for context in device.get('contexts', []):
                                context_info = {
                                    'id': context['id'],
                                    'type': context['type'],
                                    'description': context.get('description', ''),
                                    'modelId': context.get('modelId'),
                                    'modelName': context.get('modelName')
                                }
                                contexts_list.append(context_info)
                            
                            response = {
                                "mcp": {
                                    "version": "1.0",
                                    "type": "contexts_response",
                                    "timestamp": int(time.time()),
                                    "contexts": contexts_list
                                }
                            }
                            return response
                    
                    elif msg_type == 'read':
                        # Extract context path and point IDs from message
                        context_path = message.get('mcp', {}).get('context')
                        point_ids = message.get('mcp', {}).get('points')
                        
                        # Find the device with this context
                        for device in simulator.get_devices():
                            # Find the context
                            for context in device.get('contexts', []):
                                if context['id'] == context_path:
                                    # Handle the read request using simulator
                                    read_result = simulator.handle_read_request(
                                        device['uuid'], context_path, point_ids)
                                    
                                    if read_result['success']:
                                        response = {
                                            "mcp": {
                                                "version": "1.0",
                                                "type": "read_response",
                                                "timestamp": int(time.time()),
                                                "context": context_path,
                                                "success": True,
                                                "points": read_result['data']
                                            }
                                        }
                                        return response
                    
                    elif msg_type == 'write':
                        # Extract context path and points from message
                        context_path = message.get('mcp', {}).get('context')
                        points = message.get('mcp', {}).get('points', {})
                        
                        # Find the device with this context
                        for device in simulator.get_devices():
                            # Find the context
                            for context in device.get('contexts', []):
                                if context['id'] == context_path:
                                    # Handle the write request using simulator
                                    write_result = simulator.handle_write_request(
                                        device['uuid'], context_path, points)
                                    
                                    if write_result['success']:
                                        response = {
                                            "mcp": {
                                                "version": "1.0",
                                                "type": "write_response",
                                                "timestamp": int(time.time()),
                                                "context": context_path,
                                                "success": True,
                                                "points": write_result['updated_points']
                                            }
                                        }
                                        return response
                
                # If we get here, we couldn't handle the request in simulation
                logger.warning(f"Could not handle simulated request: {message}")
                return None
                
            except ImportError:
                logger.warning("Simulator module not available, falling back to real communication")
        
        # Proceed with real device communication
        sock = None  # Initialize sock to avoid the "possibly unbound" error
        try:
            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            
            # Connect and send message
            sock.connect((ip_address, port))
            message_bytes = json.dumps(message).encode('utf-8')
            
            # Add message length header (4 bytes, big-endian)
            message_length = len(message_bytes).to_bytes(4, byteorder='big')
            sock.sendall(message_length + message_bytes)
            
            logger.debug(f"Sent MCP message to {ip_address}:{port}: {message}")
            
            # Receive response length (4 bytes)
            response_length_bytes = sock.recv(4)
            if not response_length_bytes or len(response_length_bytes) < 4:
                logger.error(f"Failed to receive response length from {ip_address}:{port}")
                return None
                
            response_length = int.from_bytes(response_length_bytes, byteorder='big')
            
            # Receive response data
            response_data = b''
            bytes_received = 0
            
            while bytes_received < response_length:
                chunk = sock.recv(min(4096, response_length - bytes_received))
                if not chunk:
                    break
                response_data += chunk
                bytes_received += len(chunk)
            
            # Parse and return response
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                logger.debug(f"Received MCP response from {ip_address}:{port}: {response}")
                return response
            
            return None
            
        except socket.timeout:
            logger.error(f"Connection to {ip_address}:{port} timed out")
            return None
        except ConnectionRefusedError:
            logger.error(f"Connection to {ip_address}:{port} refused")
            return None
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response from {ip_address}:{port}")
            return None
        except Exception as e:
            logger.error(f"Error communicating with device at {ip_address}:{port}: {str(e)}")
            return None
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def broadcast_discovery(self, broadcast_address: str, broadcast_port: int) -> List[Dict[str, Any]]:
        """
        Broadcast a discovery message and collect responses.
        
        Args:
            broadcast_address: The broadcast address to use
            broadcast_port: The broadcast port to use
            
        Returns:
            List of device discovery responses
        """
        discovery_message = self.create_discovery_message()
        responses = []
        
        # If simulation mode is enabled, use simulated devices
        if SIMULATION_MODE:
            try:
                from .simulator import get_simulator
                simulator = get_simulator()
                
                if simulator:
                    # Get simulated device responses
                    simulated_devices = simulator.get_devices()
                    
                    if not simulated_devices:
                        # If no devices exist yet, create them
                        simulator.create_simulated_devices()
                        simulated_devices = simulator.get_devices()
                    
                    # Create proper discovery responses for each simulated device
                    for device in simulated_devices:
                        # Create a discovery response in MCP format
                        response = {
                            "mcp": {
                                "version": "1.0",
                                "type": "discovery_response",
                                "timestamp": int(time.time()),
                                "device": {
                                    "uuid": device['uuid'],
                                    "name": device['name'],
                                    "model": device['model'],
                                    "manufacturer": device.get('manufacturer', 'MCPHub Simulator'),
                                    "firmwareVersion": device.get('firmware_version', '1.0.0'),
                                    "protocolVersion": device.get('protocol_version', '1.0')
                                }
                            },
                            "source_ip": "127.0.0.1",  # Local IP for simulated devices
                            "source_port": 47808  # Default MCP port
                        }
                        
                        responses.append(response)
                    
                    logger.info(f"Using {len(responses)} simulated devices in discovery")
                    return responses
            except ImportError:
                logger.warning("Simulator module not available, falling back to real discovery")
        
        # Perform real discovery if simulation mode is disabled or simulator unavailable
        try:
            # Create UDP socket for broadcasting
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(3)  # Wait for responses for 3 seconds
            
            # Send broadcast
            message_bytes = json.dumps(discovery_message).encode('utf-8')
            sock.sendto(message_bytes, (broadcast_address, broadcast_port))
            logger.debug(f"Sent discovery broadcast to {broadcast_address}:{broadcast_port}")
            
            # Collect responses
            start_time = time.time()
            while time.time() - start_time < 3:  # Continue collecting for 3 seconds
                try:
                    data, addr = sock.recvfrom(4096)
                    try:
                        response = json.loads(data.decode('utf-8'))
                        if self._validate_discovery_response(response):
                            logger.debug(f"Received valid discovery response from {addr[0]}:{addr[1]}")
                            # Add source IP and port to response for reference
                            response['source_ip'] = addr[0]
                            response['source_port'] = addr[1]
                            responses.append(response)
                    except json.JSONDecodeError:
                        logger.warning(f"Received invalid JSON from {addr[0]}:{addr[1]}")
                except socket.timeout:
                    break
                
        except Exception as e:
            logger.error(f"Error during discovery broadcast: {str(e)}")
        finally:
            sock.close()
            
        return responses
    
    def _validate_discovery_response(self, response: Dict[str, Any]) -> bool:
        """
        Validate a discovery response message.
        
        Args:
            response: The response message to validate
            
        Returns:
            True if the response is valid, False otherwise
        """
        # Check for required fields in a discovery response
        if 'mcp' not in response:
            return False
        
        mcp = response['mcp']
        if not all(key in mcp for key in ['version', 'type', 'device']):
            return False
        
        if mcp['type'] != 'discovery_response':
            return False
            
        # Check device information
        device = mcp.get('device', {})
        if not all(key in device for key in ['uuid', 'model']):
            return False
            
        return True
    
    def get_device_info(self, ip_address: str, port: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a device.
        
        Args:
            ip_address: The device IP address
            port: The device port
            
        Returns:
            Dict containing device information or None if retrieval failed
        """
        # Create a device info request message
        message = {
            "mcp": {
                "version": "1.0",
                "type": "device_info",
                "timestamp": int(time.time())
            }
        }
        
        return self.send_message(message, ip_address, port)
    
    def get_available_contexts(self, ip_address: str, port: int) -> Optional[Dict[str, Any]]:
        """
        Get available contexts from a device.
        
        Args:
            ip_address: The device IP address
            port: The device port
            
        Returns:
            Dict containing contexts information or None if retrieval failed
        """
        # Create a contexts request message
        message = {
            "mcp": {
                "version": "1.0",
                "type": "contexts",
                "timestamp": int(time.time())
            }
        }
        
        return self.send_message(message, ip_address, port)
    
    def read_context(self, ip_address: str, port: int, context_path: str, 
                    point_ids: List[str] = None) -> Optional[Dict[str, Any]]:
        """
        Read data from a specific context.
        
        Args:
            ip_address: The device IP address
            port: The device port
            context_path: The context path to read from
            point_ids: Optional list of specific point IDs to read
            
        Returns:
            Dict containing the read results or None if the operation failed
        """
        message = self.create_read_request(context_path, point_ids)
        return self.send_message(message, ip_address, port)
    
    def write_context(self, ip_address: str, port: int, context_path: str, 
                     points: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Write data to a specific context.
        
        Args:
            ip_address: The device IP address
            port: The device port
            context_path: The context path to write to
            points: Dictionary of point_id -> value pairs to write
            
        Returns:
            Dict containing the write results or None if the operation failed
        """
        message = self.create_write_request(context_path, points)
        return self.send_message(message, ip_address, port)
