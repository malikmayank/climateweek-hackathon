"""
Device Data Management Module
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

from flask import current_app
from app import db
from models import Device, Context, DataPoint, DeviceEvent

logger = logging.getLogger(__name__)

class DeviceManager:
    """
    Manages device data operations, including data refreshing and updates.
    """
    
    def __init__(self, protocol):
        """
        Initialize the device manager.
        
        Args:
            protocol: The MCP protocol handler
        """
        self.protocol = protocol
        self.refresh_active = False
        self._stop_event = threading.Event()
        
        # Default refresh interval (seconds)
        self.refresh_interval = 30
    
    def start_refresh_loop(self, interval: Optional[int] = None):
        """
        Start the device data refresh loop.
        
        Args:
            interval: Refresh interval in seconds (optional, uses config value if not provided)
        """
        from app import app
        
        if interval is not None:
            self.refresh_interval = interval
        else:
            self.refresh_interval = current_app.config.get('DATA_REFRESH_INTERVAL', 30)
        
        logger.info(f"Starting device data refresh loop (interval: {self.refresh_interval}s)")
        self.refresh_active = True
        self._stop_event.clear()
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Run refresh with application context
                    with app.app_context():
                        self.refresh_all_devices()
                except Exception as e:
                    logger.error(f"Error in refresh cycle: {str(e)}")
                
                # Wait for the next refresh cycle or until stopped
                self._stop_event.wait(self.refresh_interval)
        
        finally:
            self.refresh_active = False
            logger.info("Device data refresh loop stopped")
    
    def stop_refresh(self):
        """Stop the device data refresh loop."""
        if self.refresh_active:
            logger.info("Stopping device data refresh")
            self._stop_event.set()
    
    def refresh_all_devices(self):
        """Refresh data for all online devices."""
        logger.debug("Refreshing data for all online devices")
        
        try:
            # Get all online devices
            online_devices = Device.query.filter_by(is_online=True).all()
            
            if not online_devices:
                logger.debug("No online devices to refresh")
                return
            
            # Refresh each device
            for device in online_devices:
                try:
                    self.refresh_device(device)
                except Exception as e:
                    logger.error(f"Error refreshing device {device.uuid}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error in device refresh: {str(e)}")
    
    def refresh_device(self, device: Device):
        """
        Refresh data for a specific device.
        
        Args:
            device: The device to refresh
        """
        logger.debug(f"Refreshing data for device {device.uuid}")
        
        try:
            # Update the last_seen timestamp
            device.last_seen = datetime.utcnow()
            db.session.commit()
            
            # Refresh data for each context
            for context in device.contexts:
                self.refresh_context(device, context)
            
            # Log successful refresh
            logger.debug(f"Successfully refreshed device {device.uuid}")
        
        except Exception as e:
            logger.error(f"Error refreshing device {device.uuid}: {str(e)}")
            
            # Log error event
            event = DeviceEvent(
                device_id=device.id,
                event_type='error',
                message='Error refreshing device data',
                details=str(e)
            )
            db.session.add(event)
            db.session.commit()
    
    def refresh_context(self, device: Device, context: Context):
        """
        Refresh data for a specific context in a device.
        
        Args:
            device: The device containing the context
            context: The context to refresh
        """
        try:
            # Read data from the context
            response = self.protocol.read_context(
                device.ip_address, device.port, context.context_id)
            
            if not response or 'mcp' not in response:
                logger.warning(f"Failed to read context {context.context_id} from device {device.uuid}")
                return
            
            # Process the data
            points_data = response.get('mcp', {}).get('points', {})
            if points_data:
                self._update_context_points(context, points_data)
        
        except Exception as e:
            logger.error(f"Error refreshing context {context.context_id} for device {device.uuid}: {str(e)}")
    
    def _update_context_points(self, context: Context, points_data: Dict[str, Any]):
        """
        Update data points for a context with new values.
        
        Args:
            context: The context to update
            points_data: Dictionary of point_id -> value pairs
        """
        try:
            # Get existing points for this context
            existing_points = {point.point_id: point for point in context.points}
            
            # Update each point value
            for point_id, value in points_data.items():
                if point_id in existing_points:
                    point = existing_points[point_id]
                    
                    # Convert value to string (if needed)
                    if value is not None:
                        str_value = str(value)
                        
                        # Only update if value changed
                        if point.value != str_value:
                            point.value = str_value
                            point.last_updated = datetime.utcnow()
            
            # Commit the changes
            db.session.commit()
            
        except Exception as e:
            logger.error(f"Error updating points for context {context.id}: {str(e)}")
            db.session.rollback()
    
    def read_device_context(self, device_id: int, context_id: int) -> Dict[str, Any]:
        """
        Read all data from a specific context.
        
        Args:
            device_id: The device ID
            context_id: The context database ID
            
        Returns:
            Dict with operation result
        """
        try:
            # Get the device
            device = Device.query.get(device_id)
            if not device or not device.is_online:
                logger.error(f"Device {device_id} not found or offline")
                return {'success': False, 'error': 'Device not found or offline'}
            
            # Get the context
            context = Context.query.get(context_id)
            if not context:
                logger.error(f"Context {context_id} not found")
                return {'success': False, 'error': 'Context not found'}
            
            # Read data from the context
            self.refresh_context(device, context)
            
            # Return success
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error reading context {context_id}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def write_device_context(self, device_id: int, context_id: int, points_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write values to points in a context.
        
        Args:
            device_id: The device ID
            context_id: The context database ID
            points_data: Dictionary of point_id -> value pairs
            
        Returns:
            Dict with operation result
        """
        try:
            # Get the device
            device = Device.query.get(device_id)
            if not device or not device.is_online:
                logger.error(f"Device {device_id} not found or offline")
                return {'success': False, 'error': 'Device not found or offline'}
            
            # Get the context
            context = Context.query.get(context_id)
            if not context:
                logger.error(f"Context {context_id} not found")
                return {'success': False, 'error': 'Context not found'}
            
            # Validate and prepare points for writing
            validated_points = {}
            
            for point_id, value in points_data.items():
                # Get the point
                point = DataPoint.query.filter_by(context_id=context.id, point_id=point_id).first()
                if not point:
                    logger.warning(f"Point {point_id} not found in context {context.context_id}")
                    continue
                
                # Check if point is writable
                if point.access not in ['W', 'RW']:
                    logger.warning(f"Point {point_id} is not writable (access: {point.access})")
                    continue
                
                # Add to validated points
                validated_points[point_id] = value
            
            if not validated_points:
                logger.error(f"No valid writable points in request")
                return {'success': False, 'error': 'No valid writable points'}
            
            # Send write request
            response = self.protocol.write_context(
                device.ip_address, device.port, context.context_id, validated_points)
            
            if not response or 'mcp' not in response:
                logger.error(f"Failed to write to context {context.context_id} on device {device.uuid}")
                return {'success': False, 'error': 'Failed to write to device'}
            
            # Check if write was successful
            success = response.get('mcp', {}).get('success', False)
            
            if success:
                # Update the point values in the database
                for point_id, value in validated_points.items():
                    point = DataPoint.query.filter_by(context_id=context.id, point_id=point_id).first()
                    if point:
                        point.value = str(value)
                        point.last_updated = datetime.utcnow()
                
                db.session.commit()
                
                # Log successful write
                logger.info(f"Successfully wrote values to context {context.context_id}")
                
                # Create event
                event = DeviceEvent(
                    device_id=device_id,
                    event_type='write',
                    message=f"Wrote values to {context.context_id}",
                    details=f"Points: {', '.join(validated_points.keys())}"
                )
                db.session.add(event)
                db.session.commit()
                
                return {'success': True}
            else:
                error = response.get('mcp', {}).get('error', 'Unknown error')
                logger.error(f"Write failed: {error}")
                return {'success': False, 'error': error}
                
        except Exception as e:
            logger.error(f"Error writing to context {context_id}: {str(e)}")
            db.session.rollback()
            return {'success': False, 'error': str(e)}
    
    def write_point_value(self, device_id: int, context_id: str, point_id: str, value: Any) -> bool:
        """
        Write a value to a specific data point.
        
        Args:
            device_id: The device ID
            context_id: The context ID within the device
            point_id: The point ID within the context
            value: The value to write
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the device
            device = Device.query.get(device_id)
            if not device or not device.is_online:
                logger.error(f"Device {device_id} not found or offline")
                return False
            
            # Get the context
            context = Context.query.filter_by(device_id=device_id, context_id=context_id).first()
            if not context:
                logger.error(f"Context {context_id} not found for device {device_id}")
                return False
            
            # Get the point
            point = DataPoint.query.filter_by(context_id=context.id, point_id=point_id).first()
            if not point:
                logger.error(f"Point {point_id} not found in context {context_id}")
                return False
            
            # Check if point is writable
            if point.access not in ['W', 'RW']:
                logger.error(f"Point {point_id} is not writable (access: {point.access})")
                return False
            
            # Prepare write request (single point)
            points_data = {point_id: value}
            
            # Send write request
            response = self.protocol.write_context(
                device.ip_address, device.port, context_id, points_data)
            
            if not response or 'mcp' not in response:
                logger.error(f"Failed to write to context {context_id} on device {device.uuid}")
                return False
            
            # Check if write was successful
            success = response.get('mcp', {}).get('success', False)
            
            if success:
                # Update the point value in the database
                point.value = str(value)
                point.last_updated = datetime.utcnow()
                db.session.commit()
                
                # Log successful write
                logger.info(f"Successfully wrote {value} to {point_id} in context {context_id}")
                
                # Create event
                event = DeviceEvent(
                    device_id=device_id,
                    event_type='write',
                    message=f"Wrote value to {context_id}.{point_id}",
                    details=f"Value: {value}"
                )
                db.session.add(event)
                db.session.commit()
                
                return True
            else:
                error = response.get('mcp', {}).get('error', 'Unknown error')
                logger.error(f"Write failed: {error}")
                return False
        
        except Exception as e:
            logger.error(f"Error writing to point {point_id}: {str(e)}")
            db.session.rollback()
            return False

# Singleton instance
device_manager = None

def init_device_manager(protocol, refresh_interval: Optional[int] = None):
    """
    Initialize the device manager with the specified parameters.
    
    Args:
        protocol: The MCP protocol handler
        refresh_interval: Refresh interval in seconds (uses config value if not provided)
    """
    global device_manager
    
    if device_manager is None:
        device_manager = DeviceManager(protocol)
    
    # Start the refresh loop in a separate thread
    refresh_thread = threading.Thread(target=device_manager.start_refresh_loop, 
                                     args=(refresh_interval,), daemon=True)
    refresh_thread.start()
    
    return device_manager

def get_device_manager() -> Optional[DeviceManager]:
    """Get the singleton device manager instance."""
    return device_manager
