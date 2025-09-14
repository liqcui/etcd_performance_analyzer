#!/usr/bin/env python3
"""
JSON to HTML Table ELT Module
Generic functionality for converting JSON data to HTML tables
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

try:
    from .etcd_analyzer_elt_utility import UtilityELT
except ImportError:
    from etcd_analyzer_elt_utility import UtilityELT

logger = logging.getLogger(__name__)


class JsonToTableELT:
    """Generic JSON to HTML table converter"""
    
    def __init__(self):
        self.utility = UtilityELT()
        self.critical_thresholds = {
            'cpu_usage': 80.0,
            'memory_usage': 85.0,
            'disk_usage': 90.0,
            'not_ready_nodes': 1,
            'unavailable_operators': 1,
            'degraded_mcp': 1
        }
    
    def json_to_html_table(self, data: Union[Dict, List], 
                          title: str = "Data Table",
                          max_depth: int = 3,
                          two_column_mode: bool = False,
                          highlight_metrics: bool = True) -> str:
        """
        Convert JSON data to HTML table
        
        Args:
            data: JSON data (dict or list)
            title: Table title
            max_depth: Maximum depth to traverse in nested structures
            two_column_mode: If True, create simple key-value table
            highlight_metrics: If True, highlight critical values
        
        Returns:
            HTML table string
        """
        try:
            if isinstance(data, dict):
                return self._dict_to_html_table(data, title, max_depth, two_column_mode, highlight_metrics)
            elif isinstance(data, list):
                return self._list_to_html_table(data, title, max_depth, highlight_metrics)
            else:
                return f"<p>Unsupported data type: {type(data)}</p>"
        
        except Exception as e:
            logger.error(f"Failed to convert JSON to HTML table: {e}")
            return f"<p>Error converting data to table: {e}</p>"
    
    def _dict_to_html_table(self, data: Dict, title: str, max_depth: int, 
                           two_column_mode: bool, highlight_metrics: bool) -> str:
        """Convert dictionary to HTML table"""
        
        if two_column_mode:
            return self._create_two_column_table(data, title, highlight_metrics, max_depth)
        else:
            return self._create_multi_level_table(data, title, highlight_metrics, max_depth)
    
    def _create_two_column_table(self, data: Dict, title: str, 
                                highlight_metrics: bool, max_depth: int) -> str:
        """Create simple two-column key-value table"""
        
        html = f"""
        <div class="table-container">
            <h3>{title}</h3>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Metric</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Flatten the data
        flattened = self._flatten_dict(data, max_depth=max_depth)
        
        # Sort by key for consistent display
        for key in sorted(flattened.keys()):
            value = flattened[key]
            formatted_key = self._format_key(key)
            formatted_value = self._format_value(value)
            
            # Apply highlighting if enabled
            row_class = ""
            if highlight_metrics:
                row_class = self._get_highlight_class(key, value)
            
            html += f"""
                    <tr class="{row_class}">
                        <td class="metric-name">{formatted_key}</td>
                        <td class="metric-value">{formatted_value}</td>
                    </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        """
        
        return html + self._get_table_styles()
    
    def _create_multi_level_table(self, data: Dict, title: str, 
                                 highlight_metrics: bool, max_depth: int) -> str:
        """Create multi-level grouped table"""
        
        html = f"""
        <div class="table-container">
            <h3>{title}</h3>
        """
        
        # Process each top-level section
        for section_key, section_data in data.items():
            if isinstance(section_data, dict) and section_data:
                html += self._create_section_table(section_key, section_data, highlight_metrics, max_depth - 1)
            elif isinstance(section_data, list) and section_data:
                html += self._create_list_section(section_key, section_data, highlight_metrics)
            else:
                # Simple value
                html += f"""
                <div class="simple-section">
                    <h4>{self._format_key(section_key)}</h4>
                    <p>{self._format_value(section_data)}</p>
                </div>
                """
        
        html += "</div>"
        return html + self._get_table_styles()
    
    def _create_section_table(self, section_title: str, section_data: Dict,
                             highlight_metrics: bool, max_depth: int) -> str:
        """Create table for a specific section"""
        
        html = f"""
        <div class="section-table">
            <h4>{self._format_key(section_title)}</h4>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Metric</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Handle nested data
        if max_depth > 0:
            flattened = self._flatten_dict(section_data, max_depth=max_depth)
        else:
            flattened = {k: str(v) for k, v in section_data.items()}
        
        for key in sorted(flattened.keys()):
            value = flattened[key]
            formatted_key = self._format_key(key)
            formatted_value = self._format_value(value)
            
            row_class = ""
            if highlight_metrics:
                row_class = self._get_highlight_class(key, value)
            
            html += f"""
                    <tr class="{row_class}">
                        <td class="metric-name">{formatted_key}</td>
                        <td class="metric-value">{formatted_value}</td>
                    </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        """
        
        return html
    
    def _create_list_section(self, section_title: str, section_data: List,
                            highlight_metrics: bool) -> str:
        """Create table for list data"""
        
        html = f"""
        <div class="section-table">
            <h4>{self._format_key(section_title)}</h4>
        """
        
        if not section_data:
            html += "<p>No items</p></div>"
            return html
        
        # Check if list contains dictionaries (structured data)
        if isinstance(section_data[0], dict):
            html += self._create_structured_list_table(section_data, highlight_metrics)
        else:
            # Simple list
            html += "<ul>"
            for item in section_data:
                html += f"<li>{self._format_value(item)}</li>"
            html += "</ul>"
        
        html += "</div>"
        return html
    
    def _create_structured_list_table(self, data: List[Dict], highlight_metrics: bool) -> str:
        """Create table from list of dictionaries"""
        
        if not data:
            return "<p>No data</p>"
        
        # Get all unique keys
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        sorted_keys = sorted(all_keys)
        
        html = """
            <table class="data-table">
                <thead>
                    <tr>
        """
        
        for key in sorted_keys:
            html += f"<th>{self._format_key(key)}</th>"
        
        html += """
                    </tr>
                </thead>
                <tbody>
        """
        
        for item in data:
            html += "<tr>"
            for key in sorted_keys:
                value = item.get(key, "")
                formatted_value = self._format_value(value)
                
                cell_class = ""
                if highlight_metrics:
                    cell_class = self._get_highlight_class(key, value)
                
                html += f'<td class="{cell_class}">{formatted_value}</td>'
            
            html += "</tr>"
        
        html += """
                </tbody>
            </table>
        """
        
        return html
    
    def _list_to_html_table(self, data: List, title: str, max_depth: int, 
                           highlight_metrics: bool) -> str:
        """Convert list to HTML table"""
        
        if not data:
            return f"<div class='table-container'><h3>{title}</h3><p>No data available</p></div>"
        
        html = f"""
        <div class="table-container">
            <h3>{title}</h3>
        """
        
        if isinstance(data[0], dict):
            html += self._create_structured_list_table(data, highlight_metrics)
        else:
            # Simple list
            html += "<ul>"
            for item in data:
                html += f"<li>{self._format_value(item)}</li>"
            html += "</ul>"
        
        html += "</div>"
        return html + self._get_table_styles()
    
    def _flatten_dict(self, data: Dict, parent_key: str = '', sep: str = '.', max_depth: int = 3) -> Dict:
        """Flatten nested dictionary"""
        items = []
        
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict) and max_depth > 0:
                items.extend(self._flatten_dict(value, new_key, sep, max_depth - 1).items())
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                # For list of dicts, create summary
                items.append((f"{new_key}_count", len(value)))
                if max_depth > 0:
                    for i, item in enumerate(value[:3]):  # Limit to first 3 items
                        if isinstance(item, dict):
                            for sub_key, sub_value in item.items():
                                items.append((f"{new_key}[{i}].{sub_key}", sub_value))
            else:
                items.append((new_key, value))
        
        return dict(items)
    
    def _format_key(self, key: str) -> str:
        """Format key for display"""
        # Replace underscores with spaces and title case
        formatted = key.replace('_', ' ').replace('.', ' → ')
        return formatted.title()
    
    def _format_value(self, value: Any) -> str:
        """Format value for display with appropriate units"""
        if value is None:
            return "N/A"
        
        if isinstance(value, bool):
            return "✓" if value else "✗"
        
        if isinstance(value, (int, float)):
            return self._format_numeric_value(value)
        
        if isinstance(value, str):
            # Handle timestamp formatting
            if 'timestamp' in str(value).lower() or value.endswith('+00:00'):
                return self.utility.format_timestamp(value)
            
            # Handle memory units
            if value.endswith('Ki') or value.endswith('Mi') or value.endswith('Gi'):
                bytes_val = self.utility.convert_memory_to_bytes(value)
                return self.utility.convert_bytes_to_readable(bytes_val)
        
        if isinstance(value, list):
            return f"[{len(value)} items]" if len(value) > 5 else str(value)
        
        return str(value)
    
    def _format_numeric_value(self, value: Union[int, float]) -> str:
        """Format numeric values with appropriate units"""
        if isinstance(value, float):
            # Memory values (assuming GB)
            if value > 1000:
                return f"{value/1000:.1f} TB"
            elif value > 1:
                return f"{value:.1f} GB"
            elif value > 0.001:
                return f"{value*1000:.0f} MB"
            else:
                return f"{value:.3f}"
        else:
            # Integer values
            if value > 1000000:
                return f"{value/1000000:.1f}M"
            elif value > 1000:
                return f"{value/1000:.1f}K"
            else:
                return str(value)
    
    def _get_highlight_class(self, key: str, value: Any) -> str:
        """Get CSS class for highlighting critical values"""
        key_lower = key.lower()
        
        # Critical issues
        if 'not_ready' in key_lower and isinstance(value, (int, float)) and value > 0:
            return "critical"
        
        if 'unavailable' in key_lower and isinstance(value, (int, float)) and value > 0:
            return "critical"
        
        if 'degraded' in key_lower and isinstance(value, (int, float)) and value > 0:
            return "critical"
        
        if 'error' in key_lower or 'failed' in key_lower:
            return "critical"
        
        # Warning conditions
        if 'updating' in key_lower and isinstance(value, (int, float)) and value > 0:
            return "warning"
        
        # Top metrics (highest values)
        if isinstance(value, (int, float)) and value > 0:
            if any(metric in key_lower for metric in ['cpu', 'memory', 'nodes', 'pods']):
                return "top-metric"
        
        return ""
    
    def _get_table_styles(self) -> str:
        """Get CSS styles for the HTML table"""
        return """
        <style>
        .table-container {
            margin: 20px 0;
            font-family: Arial, sans-serif;
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .data-table th {
            background-color: #f8f9fa;
            color: #333;
            font-weight: bold;
            padding: 12px;
            text-align: left;
            border: 1px solid #dee2e6;
        }
        
        .data-table td {
            padding: 10px 12px;
            border: 1px solid #dee2e6;
            vertical-align: top;
        }
        
        .data-table tbody tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        
        .data-table tbody tr:hover {
            background-color: #e9ecef;
        }
        
        .metric-name {
            font-weight: 500;
            color: #495057;
            width: 40%;
        }
        
        .metric-value {
            font-family: monospace;
            color: #212529;
        }
        
        .critical {
            background-color: #f8d7da !important;
            color: #721c24;
            font-weight: bold;
        }
        
        .warning {
            background-color: #fff3cd !important;
            color: #856404;
        }
        
        .top-metric {
            background-color: #d1ecf1 !important;
            color: #0c5460;
            font-weight: bold;
        }
        
        .section-table {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            background-color: #ffffff;
        }
        
        .section-table h4 {
            margin: 0 0 15px 0;
            color: #495057;
            font-size: 1.1em;
            border-bottom: 2px solid #007bff;
            padding-bottom: 5px;
        }
        
        .simple-section {
            margin: 15px 0;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 3px;
        }
        
        .simple-section h4 {
            margin: 0 0 8px 0;
            color: #495057;
        }
        
        .simple-section p {
            margin: 0;
            font-family: monospace;
            color: #212529;
        }
        
        ul {
            margin: 10px 0;
            padding-left: 20px;
        }
        
        li {
            margin: 5px 0;
            color: #495057;
        }
        </style>
        """