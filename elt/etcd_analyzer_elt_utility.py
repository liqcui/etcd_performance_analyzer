#!/usr/bin/env python3
"""
Utility ELT Module
Common utility functions for ELT operations
"""

import json
import logging
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timezone
from dataclasses import asdict, is_dataclass

logger = logging.getLogger(__name__)


class UtilityELT:
    """Common utility functions for ELT operations"""
    
    def __init__(self):
        self.memory_units = {
            'B': 1,
            'KB': 1024,
            'MB': 1024**2,
            'GB': 1024**3,
            'TB': 1024**4,
            'Ki': 1024,
            'Mi': 1024**2,
            'Gi': 1024**3,
            'Ti': 1024**4
        }
    
    def safe_get_nested(self, data: Dict, keys: List[str], default: Any = None) -> Any:
        """Safely get nested dictionary values"""
        try:
            current = data
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return default
            return current
        except (KeyError, TypeError, AttributeError):
            return default
    
    def convert_memory_to_bytes(self, memory_str: str) -> float:
        """Convert memory string to bytes"""
        try:
            if not isinstance(memory_str, str):
                return 0.0
            
            # Remove whitespace and make uppercase for standard units
            memory_str = memory_str.strip()
            
            # Handle Kubernetes units (Ki, Mi, Gi)
            if memory_str.endswith('Ki'):
                return float(memory_str[:-2]) * self.memory_units['Ki']
            elif memory_str.endswith('Mi'):
                return float(memory_str[:-2]) * self.memory_units['Mi']
            elif memory_str.endswith('Gi'):
                return float(memory_str[:-2]) * self.memory_units['Gi']
            elif memory_str.endswith('Ti'):
                return float(memory_str[:-2]) * self.memory_units['Ti']
            
            # Handle standard units
            memory_upper = memory_str.upper()
            for unit, multiplier in self.memory_units.items():
                if memory_upper.endswith(unit.upper()):
                    return float(memory_upper[:-len(unit)]) * multiplier
            
            # If no unit, assume bytes
            return float(memory_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert memory string '{memory_str}': {e}")
            return 0.0
    
    def convert_bytes_to_readable(self, bytes_value: float, precision: int = 2) -> str:
        """Convert bytes to human readable format"""
        try:
            if bytes_value == 0:
                return "0 B"
            
            units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
            unit_index = 0
            
            value = float(bytes_value)
            while value >= 1024 and unit_index < len(units) - 1:
                value /= 1024
                unit_index += 1
            
            if unit_index == 0:  # Bytes
                return f"{int(value)} {units[unit_index]}"
            else:
                return f"{value:.{precision}f} {units[unit_index]}"
                
        except (ValueError, TypeError):
            return str(bytes_value)
    
    def parse_cpu_value(self, cpu_str: str) -> float:
        """Parse CPU value string to float (cores)"""
        try:
            if not isinstance(cpu_str, str):
                return float(cpu_str)
            
            cpu_str = cpu_str.strip()
            
            # Handle millicores (e.g., "100m" = 0.1 cores)
            if cpu_str.endswith('m'):
                return float(cpu_str[:-1]) / 1000
            
            # Handle regular cores
            return float(cpu_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse CPU value '{cpu_str}': {e}")
            return 0.0
    
    def format_cpu_readable(self, cpu_cores: float, precision: int = 2) -> str:
        """Format CPU cores to readable string"""
        try:
            if cpu_cores == 0:
                return "0 cores"
            elif cpu_cores < 1:
                return f"{cpu_cores*1000:.0f}m"
            else:
                return f"{cpu_cores:.{precision}f} cores"
        except (ValueError, TypeError):
            return str(cpu_cores)
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse various timestamp formats"""
        if not timestamp_str:
            return None
        
        try:
            # ISO format with timezone
            if timestamp_str.endswith('+00:00'):
                return datetime.fromisoformat(timestamp_str.replace('+00:00', '+00:00'))
            elif 'T' in timestamp_str and ('Z' in timestamp_str or '+' in timestamp_str):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                # Try basic ISO format
                return datetime.fromisoformat(timestamp_str)
                
        except ValueError as e:
            logger.warning(f"Could not parse timestamp '{timestamp_str}': {e}")
            return None
    
    def format_timestamp(self, timestamp: Union[str, datetime], format_str: str = "%Y-%m-%d %H:%M:%S UTC") -> str:
        """Format timestamp for display"""
        try:
            if isinstance(timestamp, str):
                dt = self.parse_timestamp(timestamp)
                if dt is None:
                    return timestamp
            else:
                dt = timestamp
            
            # Convert to UTC if timezone aware
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            
            return dt.strftime(format_str)
            
        except (ValueError, AttributeError) as e:
            logger.warning(f"Could not format timestamp: {e}")
            return str(timestamp)
    
    def calculate_percentage(self, value: Union[int, float], total: Union[int, float], 
                           precision: int = 1) -> float:
        """Calculate percentage safely"""
        try:
            if total == 0:
                return 0.0
            return round((float(value) / float(total)) * 100, precision)
        except (ValueError, TypeError, ZeroDivisionError):
            return 0.0
    
    def get_top_values(self, data: Dict[str, Union[int, float]], 
                      count: int = 5, reverse: bool = True) -> List[Tuple[str, Union[int, float]]]:
        """Get top N values from dictionary"""
        try:
            sorted_items = sorted(data.items(), key=lambda x: float(x[1]), reverse=reverse)
            return sorted_items[:count]
        except (ValueError, TypeError):
            return list(data.items())[:count]
    
    def calculate_statistics(self, values: List[Union[int, float]]) -> Dict[str, float]:
        """Calculate basic statistics for a list of values"""
        if not values:
            return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0}
        
        try:
            numeric_values = [float(v) for v in values if v is not None]
            if not numeric_values:
                return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0}
            
            return {
                'count': len(numeric_values),
                'sum': sum(numeric_values),
                'avg': sum(numeric_values) / len(numeric_values),
                'min': min(numeric_values),
                'max': max(numeric_values)
            }
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not calculate statistics: {e}")
            return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0}
    
    def flatten_json(self, data: Dict, parent_key: str = '', separator: str = '.', 
                    max_depth: int = 5) -> Dict[str, Any]:
        """Flatten nested JSON structure"""
        items = []
        
        if max_depth <= 0:
            return {parent_key: str(data)} if parent_key else {'value': str(data)}
        
        try:
            for key, value in data.items():
                new_key = f"{parent_key}{separator}{key}" if parent_key else key
                
                if isinstance(value, dict):
                    items.extend(
                        self.flatten_json(value, new_key, separator, max_depth - 1).items()
                    )
                elif isinstance(value, list):
                    if len(value) > 0 and isinstance(value[0], dict):
                        # Handle list of dictionaries
                        for i, item in enumerate(value):
                            if isinstance(item, dict):
                                items.extend(
                                    self.flatten_json(item, f"{new_key}[{i}]", separator, max_depth - 1).items()
                                )
                            else:
                                items.append((f"{new_key}[{i}]", item))
                    else:
                        # Handle simple list
                        items.append((new_key, value))
                else:
                    items.append((new_key, value))
                    
        except (AttributeError, TypeError):
            items.append((parent_key or 'value', str(data)))
        
        return dict(items)
    
    def sanitize_string(self, text: str, max_length: int = 100) -> str:
        """Sanitize string for display"""
        if not isinstance(text, str):
            text = str(text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # Truncate if too long
        if len(text) > max_length:
            text = text[:max_length-3] + '...'
        
        return text
    
    def convert_dataclass_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert dataclass to dictionary"""
        if is_dataclass(obj):
            return asdict(obj)
        elif isinstance(obj, dict):
            return obj
        else:
            return {'value': obj}
    
    def deep_merge_dicts(self, dict1: Dict, dict2: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = dict1.copy()
        
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self.deep_merge_dicts(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def validate_json_structure(self, data: Any, required_keys: List[str] = None) -> Tuple[bool, List[str]]:
        """Validate JSON structure"""
        errors = []
        
        if not isinstance(data, dict):
            errors.append("Data is not a dictionary")
            return False, errors
        
        if required_keys:
            missing_keys = [key for key in required_keys if key not in data]
            if missing_keys:
                errors.append(f"Missing required keys: {missing_keys}")
        
        return len(errors) == 0, errors
    
    def extract_numeric_metrics(self, data: Dict, 
                               numeric_keys: List[str] = None) -> Dict[str, Union[int, float]]:
        """Extract numeric metrics from data"""
        if numeric_keys is None:
            numeric_keys = [
                'cpu', 'memory', 'disk', 'count', 'total', 'available', 'used',
                'capacity', 'nodes', 'pods', 'services', 'namespaces'
            ]
        
        metrics = {}
        flattened = self.flatten_json(data)
        
        for key, value in flattened.items():
            key_lower = key.lower()
            
            # Check if key contains numeric indicators
            if any(indicator in key_lower for indicator in numeric_keys):
                try:
                    # Try to extract numeric value
                    if isinstance(value, (int, float)):
                        metrics[key] = value
                    elif isinstance(value, str):
                        # Try to parse numeric from string
                        if value.replace('.', '').replace('-', '').isdigit():
                            metrics[key] = float(value)
                        elif value.endswith(('Ki', 'Mi', 'Gi', 'Ti')):
                            # Memory values
                            metrics[f"{key}_bytes"] = self.convert_memory_to_bytes(value)
                        elif value.endswith('m') and value[:-1].isdigit():
                            # CPU millicores
                            metrics[f"{key}_cores"] = self.parse_cpu_value(value)
                except (ValueError, TypeError):
                    continue
        
        return metrics
    
    def generate_summary_stats(self, data: Dict) -> Dict[str, Any]:
        """Generate summary statistics from data"""
        try:
            numeric_metrics = self.extract_numeric_metrics(data)
            
            if not numeric_metrics:
                return {'total_metrics': 0, 'numeric_metrics': 0}
            
            stats = self.calculate_statistics(list(numeric_metrics.values()))
            top_metrics = self.get_top_values(numeric_metrics, count=5)
            
            return {
                'total_keys': len(self.flatten_json(data)),
                'numeric_metrics': len(numeric_metrics),
                'stats': stats,
                'top_5_metrics': top_metrics,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to generate summary stats: {e}")
            return {'error': str(e)}