import logging
import json
import os
from typing import Dict, List, Any, Optional

# Configure logger
logger = logging.getLogger(__name__)

class SunSpecModels:
    """
    Manages SunSpec model information.
    
    This class provides access to SunSpec model definitions and handles
    model-specific operations like data type conversions and validation.
    """
    
    def __init__(self):
        """Initialize the SunSpec models handler."""
        self.models = {}
        self._load_models()
        logger.info(f"SunSpec Models initialized with {len(self.models)} models")
    
    def _load_models(self):
        """Load SunSpec model definitions from a built-in dictionary."""
        # This is a simplified version with just a few core models
        # In a production system, you would load the complete model definitions from files
        self.models = {
            # Common Models
            1: {
                "id": 1,
                "name": "Common",
                "description": "Common Model",
                "points": {
                    "Mn": {"name": "Manufacturer", "type": "string", "description": "Device manufacturer"},
                    "Md": {"name": "Model", "type": "string", "description": "Device model"},
                    "SN": {"name": "SerialNumber", "type": "string", "description": "Device serial number"},
                    "Ver": {"name": "Version", "type": "string", "description": "Device version"}
                }
            },
            # Inverter Model
            101: {
                "id": 101,
                "name": "Inverter",
                "description": "Single Phase Inverter",
                "points": {
                    "A": {"name": "AC Current", "type": "float", "unit": "A", "description": "AC Total Current", "access": "R"},
                    "AphA": {"name": "Phase A Current", "type": "float", "unit": "A", "description": "Phase A Current", "access": "R"},
                    "PhVphA": {"name": "Phase A Voltage", "type": "float", "unit": "V", "description": "Phase A Voltage", "access": "R"},
                    "W": {"name": "AC Power", "type": "float", "unit": "W", "description": "AC Power", "access": "R"},
                    "Hz": {"name": "Frequency", "type": "float", "unit": "Hz", "description": "AC Frequency", "access": "R"},
                    "WH": {"name": "Energy", "type": "float", "unit": "Wh", "description": "AC Energy", "access": "R"},
                    "St": {"name": "Operating State", "type": "enum16", "description": "Inverter operating state", "access": "R"}
                }
            },
            # MPPT Model
            160: {
                "id": 160,
                "name": "Multiple MPPT Inverter Extension",
                "description": "DER Multiple MPPT Inverter Extension Model",
                "points": {
                    "ID": {"name": "ID", "type": "uint16", "description": "MPPT ID", "access": "R"},
                    "DCA": {"name": "DC Current", "type": "float", "unit": "A", "description": "DC Current", "access": "R"},
                    "DCV": {"name": "DC Voltage", "type": "float", "unit": "V", "description": "DC Voltage", "access": "R"},
                    "DCW": {"name": "DC Power", "type": "float", "unit": "W", "description": "DC Power", "access": "R"},
                    "TmpCab": {"name": "Cabinet Temperature", "type": "float", "unit": "C", "description": "Cabinet Temperature", "access": "R"},
                    "St": {"name": "Operating State", "type": "enum16", "description": "MPPT operating state", "access": "R"}
                }
            },
            # Storage Model
            124: {
                "id": 124,
                "name": "Storage",
                "description": "Storage Model",
                "points": {
                    "ChaState": {"name": "State of Charge", "type": "float", "unit": "%", "description": "Battery state of charge", "access": "R"},
                    "ChaSt": {"name": "Battery Status", "type": "enum16", "description": "Battery status", "access": "R"},
                    "W": {"name": "Power", "type": "float", "unit": "W", "description": "Battery power", "access": "R"},
                    "WChaMax": {"name": "Max Charge Rate", "type": "float", "unit": "W", "description": "Max charge rate", "access": "RW"},
                    "WDisChaMax": {"name": "Max Discharge Rate", "type": "float", "unit": "W", "description": "Max discharge rate", "access": "RW"}
                }
            }
        }
    
    def get_model(self, model_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a SunSpec model definition by its ID.
        
        Args:
            model_id: The SunSpec model ID
            
        Returns:
            Dict with model definition or None if not found
        """
        return self.models.get(model_id)
    
    def get_point_info(self, model_id: int, point_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific point in a model.
        
        Args:
            model_id: The SunSpec model ID
            point_id: The point ID
            
        Returns:
            Dict with point information or None if not found
        """
        model = self.get_model(model_id)
        
        if model and 'points' in model:
            return model['points'].get(point_id)
            
        return None
    
    def get_all_models(self) -> Dict[int, Dict[str, Any]]:
        """
        Get all loaded model definitions.
        
        Returns:
            Dict of model_id -> model_definition
        """
        return self.models
    
    def parse_value(self, model_id: int, point_id: str, value: Any) -> Any:
        """
        Parse a value according to its data type in the model.
        
        Args:
            model_id: The SunSpec model ID
            point_id: The point ID
            value: The raw value
            
        Returns:
            The parsed value according to its data type
        """
        point_info = self.get_point_info(model_id, point_id)
        
        if not point_info:
            return value
            
        data_type = point_info.get('type')
        
        try:
            if data_type == 'float':
                return float(value)
            elif data_type in ('uint16', 'uint32', 'uint64'):
                return int(value)
            elif data_type in ('int16', 'int32', 'int64'):
                return int(value)
            elif data_type == 'enum16':
                return int(value)
            elif data_type == 'string':
                return str(value)
            elif data_type == 'boolean':
                return bool(value)
            else:
                return value
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse value '{value}' as {data_type}")
            return value
    
    def format_value(self, model_id: int, point_id: str, value: Any) -> str:
        """
        Format a value for display according to its data type and unit.
        
        Args:
            model_id: The SunSpec model ID
            point_id: The point ID
            value: The raw value
            
        Returns:
            Formatted value string
        """
        if value is None:
            return "N/A"
            
        point_info = self.get_point_info(model_id, point_id)
        
        if not point_info:
            return str(value)
            
        data_type = point_info.get('type')
        unit = point_info.get('unit', '')
        
        try:
            if data_type == 'float':
                # Format floats with 2 decimal places
                formatted = f"{float(value):.2f}"
            else:
                formatted = str(value)
                
            # Add unit if available
            if unit:
                formatted += f" {unit}"
                
            return formatted
            
        except (ValueError, TypeError):
            return str(value)
    
    def validate_value(self, model_id: int, point_id: str, value: Any) -> bool:
        """
        Validate a value against its data type in the model.
        
        Args:
            model_id: The SunSpec model ID
            point_id: The point ID
            value: The value to validate
            
        Returns:
            True if the value is valid, False otherwise
        """
        point_info = self.get_point_info(model_id, point_id)
        
        if not point_info:
            return True  # Can't validate without point info
            
        data_type = point_info.get('type')
        
        try:
            if data_type == 'float':
                float(value)
            elif data_type in ('uint16', 'uint32', 'uint64'):
                int_val = int(value)
                if int_val < 0:
                    return False
            elif data_type in ('int16', 'int32', 'int64'):
                int(value)
            elif data_type == 'enum16':
                int(value)
            elif data_type == 'string':
                str(value)
            elif data_type == 'boolean':
                bool(value)
            return True
        except (ValueError, TypeError):
            return False
