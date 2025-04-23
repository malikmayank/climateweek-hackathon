import logging
import time
import threading
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from app import db
from models import Device, Context, DataPoint, DeviceEvent
from .protocol import MCPProtocol

logger = logging.getLogger(__name__)

class DeviceDiscovery:
    """
    Handles device discovery and registration using the MCP protocol.
    
    This class manages the discovery process, including sending broadcast messages,
    processing responses, and registering discovered devices in the database.
    """
    
    def __init__(self, protocol: MCPProtocol, broadcast_address: str = '255.255.255.255', 
                 broadcast_port: int = 47808, discovery_interval: int = 60):
        """
        Initialize the discovery service.
        
        Args:
            protocol: The MCP protocol handler
            broadcast_address: Address to use for discovery broadcasts
            broadcast_port: Port to use for discovery broadcasts
            discovery_interval: Interval between discovery attempts in seconds
        """
        self.protocol = protocol
        self.broadcast_address = broadcast_address
        self.broadcast_port = broadcast_port
        self.discovery_interval = discovery_interval
        self.discovery_active = False
        self._stop_event = threading.Event()
        logger.info(f"Device Discovery service initialized with broadcast to {broadcast_address}:{broadcast_port}")
    
    def discover_devices(self) -> List[Dict[str, Any]]:
        """
        Perform a single discovery operation and register found devices.
        
        Returns:
            List of discovered device information
        """
        logger.info("Starting device discovery")
        discovered_devices = []
        
        # Send discovery broadcast and collect responses
        responses = self.protocol.broadcast_discovery(self.broadcast_address, self.broadcast_port)
        logger.info(f"Received {len(responses)} discovery responses")
        
        # Process each discovery response
        for response in responses:
            try:
                if 'mcp' in response and 'device' in response['mcp']:
                    device_info = response['mcp']['device']
                    device_uuid = device_info.get('uuid')
                    
                    if not device_uuid:
                        logger.warning("Received device info without UUID, skipping")
                        continue
                    
                    # Extract source IP and port from the response
                    source_ip = response.get('source_ip')
                    source_port = response.get('source_port')
                    
                    if not source_ip:
                        logger.warning(f"Missing source IP for device {device_uuid}, skipping")
                        continue
                    
                    # Process and register the device
                    device_data = self._process_device_info(device_info, source_ip, source_port)
                    if device_data:
                        discovered_devices.append(device_data)
                        
                        # Get additional device information
                        self._get_device_details(device_data)
            except Exception as e:
                logger.error(f"Error processing discovery response: {str(e)}")
        
        # Update device online/offline status
        self._update_device_status(discovered_devices)
        
        return discovered_devices
    
    def _process_device_info(self, device_info: Dict[str, Any], ip_address: str, 
                            port: int) -> Optional[Dict[str, Any]]:
        """
        Process device information from a discovery response and register in the database.
        
        Args:
            device_info: Device information from the discovery response
            ip_address: The device IP address
            port: The device port (if specified in the response, or use default)
            
        Returns:
            Dict with processed device data or None if processing failed
        """
        try:
            uuid = device_info.get('uuid')
            if not uuid:
                logger.warning("Device missing UUID, cannot process")
                return None
            
            # Use the source port from the response, or default to a common MCP port
            device_port = port or 47808
            
            # Extract device properties
            device_data = {
                'uuid': uuid,
                'name': device_info.get('name', f"Device-{uuid[:8]}"),
                'model': device_info.get('model'),
                'manufacturer': device_info.get('manufacturer'),
                'firmware_version': device_info.get('firmwareVersion'),
                'protocol_version': device_info.get('protocolVersion', device_info.get('version')),
                'ip_address': ip_address,
                'port': device_port,
                'is_online': True,
                'last_seen': datetime.utcnow()
            }
            
            # Check if device already exists in database
            with db.session.begin():
                existing_device = Device.query.filter_by(uuid=uuid).first()
                
                if existing_device:
                    logger.debug(f"Updating existing device {uuid}")
                    
                    # Update device properties
                    for key, value in device_data.items():
                        if key != 'uuid':  # Don't update UUID
                            setattr(existing_device, key, value)
                    
                    # Log device status change if previously offline
                    if not existing_device.is_online:
                        event = DeviceEvent(
                            device_id=existing_device.id,
                            event_type='status_change',
                            message='Device came online',
                            details=f"IP: {ip_address}, Port: {device_port}"
                        )
                        db.session.add(event)
                    
                    device = existing_device
                else:
                    logger.info(f"Registering new device {uuid}")
                    device = Device(**device_data)
                    db.session.add(device)
                    db.session.flush()  # Flush to get device ID
                    
                    # Log device discovery event
                    event = DeviceEvent(
                        device_id=device.id,
                        event_type='discovery',
                        message='Device discovered',
                        details=f"Model: {device.model}, Manufacturer: {device.manufacturer}"
                    )
                    db.session.add(event)
                
                db.session.commit()
            
            device_data['id'] = device.id
            return device_data
            
        except Exception as e:
            logger.error(f"Error processing device info: {str(e)}")
            db.session.rollback()
            return None
    
    def _get_device_details(self, device_data: Dict[str, Any]):
        """
        Get additional details about a device, including its contexts.
        
        Args:
            device_data: Basic device information including IP and port
        """
        device_id = device_data.get('id')
        ip_address = device_data.get('ip_address')
        port = device_data.get('port')
        
        if not all([device_id, ip_address, port]):
            logger.warning("Missing required device information, cannot get details")
            return
        
        try:
            # Get available contexts from the device
            contexts_response = self.protocol.get_available_contexts(ip_address, port)
            
            if contexts_response and 'mcp' in contexts_response and 'contexts' in contexts_response['mcp']:
                contexts = contexts_response['mcp']['contexts']
                self._process_device_contexts(device_id, contexts)
        except Exception as e:
            logger.error(f"Error getting device details for device {device_id}: {str(e)}")
    
    def _process_device_contexts(self, device_id: int, contexts_data: List[Dict[str, Any]]):
        """
        Process and register device contexts and their data points.
        
        Args:
            device_id: The device ID in the database
            contexts_data: Context information from the device
        """
        # We need to use the main db.session but with a nested transaction
        from app import db
        from models import Context, DataPoint
        
        try:
            # Get existing contexts for this device
            existing_contexts = Context.query.filter_by(device_id=device_id).all()
            existing_context_ids = {context.context_id for context in existing_contexts}
            
            # Process each context
            for context_info in contexts_data:
                context_id = context_info.get('id')
                if not context_id:
                    continue
                
                context_type = context_info.get('type', 'unknown')
                model_id = context_info.get('modelId')
                model_name = context_info.get('modelName')
                description = context_info.get('description', f"{context_type.capitalize()} context")
                
                # Check if context already exists
                if context_id in existing_context_ids:
                    logger.debug(f"Updating existing context {context_id} for device {device_id}")
                    context = next(c for c in existing_contexts if c.context_id == context_id)
                    
                    # Update context properties
                    context.context_type = context_type
                    context.model_id = model_id
                    context.model_name = model_name
                    context.description = description
                else:
                    logger.info(f"Registering new context {context_id} for device {device_id}")
                    context = Context(
                        device_id=device_id,
                        context_id=context_id,
                        context_type=context_type,
                        model_id=model_id,
                        model_name=model_name,
                        description=description
                    )
                    db.session.add(context)
                    db.session.flush()  # Flush to get context ID
                
                # Process data points if included
                if 'points' in context_info:
                    self._process_context_points(context, context_info['points'])
            
            db.session.commit()
                
        except Exception as e:
            logger.error(f"Error processing contexts for device {device_id}: {str(e)}")
            db.session.rollback()
    
    def _process_context_points(self, context: Context, points_data: Dict[str, Any]):
        """
        Process and register data points for a context.
        
        Args:
            context: The context object
            points_data: Data points information
        """
        try:
            # Get existing points for this context
            existing_points = DataPoint.query.filter_by(context_id=context.id).all()
            existing_point_ids = {point.point_id for point in existing_points}
            
            # Process each point
            for point_id, point_info in points_data.items():
                if isinstance(point_info, dict):
                    name = point_info.get('name', point_id)
                    data_type = point_info.get('type')
                    unit = point_info.get('unit')
                    access = point_info.get('access')
                    description = point_info.get('description')
                    value = point_info.get('value')
                    
                    # Check if point already exists
                    if point_id in existing_point_ids:
                        logger.debug(f"Updating existing point {point_id} for context {context.id}")
                        point = next(p for p in existing_points if p.point_id == point_id)
                        
                        # Update point properties
                        point.name = name
                        point.data_type = data_type
                        point.unit = unit
                        point.access = access
                        point.description = description
                        
                        # Only update value if it's provided
                        if value is not None:
                            point.value = str(value)
                            point.last_updated = datetime.utcnow()
                    else:
                        logger.debug(f"Registering new point {point_id} for context {context.id}")
                        point = DataPoint(
                            context_id=context.id,
                            name=name,
                            point_id=point_id,
                            data_type=data_type,
                            unit=unit,
                            access=access,
                            description=description,
                            value=str(value) if value is not None else None
                        )
                        db.session.add(point)
        
        except Exception as e:
            logger.error(f"Error processing points for context {context.id}: {str(e)}")
            raise  # Propagate exception to parent transaction
    
    def _update_device_status(self, discovered_devices: List[Dict[str, Any]]):
        """
        Update online/offline status for all devices based on discovery results.
        
        Args:
            discovered_devices: List of devices found in the current discovery
        """
        try:
            # Extract UUIDs of discovered devices
            discovered_uuids = {device['uuid'] for device in discovered_devices}
            
            with db.session.begin():
                # Get all devices from database
                all_devices = Device.query.all()
                
                for device in all_devices:
                    # If device was previously online but not in current discovery, mark offline
                    if device.is_online and device.uuid not in discovered_uuids:
                        logger.info(f"Device {device.uuid} is now offline")
                        device.is_online = False
                        
                        # Log status change event
                        event = DeviceEvent(
                            device_id=device.id,
                            event_type='status_change',
                            message='Device went offline'
                        )
                        db.session.add(event)
                
                db.session.commit()
                
        except Exception as e:
            logger.error(f"Error updating device status: {str(e)}")
            db.session.rollback()
    
    def start_discovery_loop(self):
        """Start the continuous discovery loop in the current thread."""
        from app import app
        
        logger.info("Starting continuous device discovery")
        self.discovery_active = True
        self._stop_event.clear()
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Run discovery with application context
                    with app.app_context():
                        self.discover_devices()
                except Exception as e:
                    logger.error(f"Error in discovery cycle: {str(e)}")
                
                # Wait for the next discovery cycle or until stopped
                self._stop_event.wait(self.discovery_interval)
        
        finally:
            self.discovery_active = False
            logger.info("Device discovery loop stopped")
    
    def stop_discovery(self):
        """Stop the continuous discovery loop."""
        if self.discovery_active:
            logger.info("Stopping device discovery")
            self._stop_event.set()
