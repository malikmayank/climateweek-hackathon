# SunSpec Context Management moduule

import logging
from typing import Dict, List, Any, Optional, Union, Tuple

logger = logging.getLogger(__name__)

class ContextPathParser:
    @staticmethod
    def parse_context_path(context_path: str) -> Tuple[str, Optional[List[str]]]:
        if not context_path or not isinstance(context_path, str):
            return "", None
        
        parts = context_path.split('.')
        
        # Simple case - no subcontexts
        if len(parts) <= 1:
            return context_path, None
        
        # Complex case - has subcontext parts
        context_id = parts[0]
        subcontext_parts = parts[1:] if len(parts) > 1 else None
        
        return context_id, subcontext_parts
    
    @staticmethod
    def build_context_path(context_id: str, subcontext_parts: Optional[List[str]] = None) -> str:
        if not subcontext_parts:
            return context_id
        
        return f"{context_id}.{'.'.join(subcontext_parts)}"
    
    @staticmethod
    def is_valid_context_path(context_path: str) -> bool:
        if not context_path or not isinstance(context_path, str):
            return False
        
        # Basic validation rules
        parts = context_path.split('.')
        
        # Must have at least one part
        if not parts or not parts[0]:
            return False
        
        # All parts must be non-empty
        for part in parts:
            if not part:
                return False
        
        return True
    
    @staticmethod
    def get_context_type(context_path: str) -> str:
        if not context_path or not isinstance(context_path, str):
            return "unknown"
        
        parts = context_path.split('.')
        if not parts:
            return "unknown"
        
        # Extract the alphabetic part from the first component
        context_type = ''.join(c for c in parts[0] if c.isalpha())
        
        return context_type.lower() if context_type else "unknown"
    
    @staticmethod
    def get_context_index(context_path: str) -> Optional[int]:
        if not context_path or not isinstance(context_path, str):
            return None
        
        parts = context_path.split('.')
        if not parts:
            return None
        
        # Try to extract numeric part from the first component
        try:
            # Find all digits
            digits = ''.join(c for c in parts[0] if c.isdigit())
            if digits:
                return int(digits)
            
            # If no digits in first part, check second part if it exists
            if len(parts) > 1 and parts[1].isdigit():
                return int(parts[1])
        except ValueError:
            pass
        
        return None

class ContextDataHandler:
    @staticmethod
    def filter_points_by_ids(points_data: Dict[str, Any], point_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        if point_ids is None:
            return points_data
        
        return {
            point_id: value 
            for point_id, value in points_data.items() 
            if point_id in point_ids
        }
    
    @staticmethod
    def merge_points_data(base_data: Dict[str, Any], new_data: Dict[str, Any], 
                           overwrite: bool = True) -> Dict[str, Any]:
        result = base_data.copy()
        
        for point_id, value in new_data.items():
            if overwrite or point_id not in result:
                result[point_id] = value
        
        return result
    
    @staticmethod
    def transform_to_mcp_response(context_id: str, points_data: Dict[str, Any], 
                                 success: bool = True, error: Optional[str] = None) -> Dict[str, Any]:
        response = {
            "mcp": {
                "version": "1.0",
                "context": context_id,
                "success": success,
                "points": points_data
            }
        }
        
        if error:
            response["mcp"]["error"] = error
        
        return response
    
    @staticmethod
    def extract_writable_points(points_data: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        writable_points = {}
        
        for point_id, details in points_data.items():
            access = details.get('access', 'R')
            if access in ['W', 'RW']:
                writable_points[point_id] = access
        
        return writable_points

# Initialize a shared context path parser instance for the application
context_parser = ContextPathParser()

def get_context_parser() -> ContextPathParser:
    """Get the application's context path parser instance."""
    return context_parser
