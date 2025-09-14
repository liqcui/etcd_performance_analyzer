"""
OpenShift Prometheus Base Query Module
Provides base functionality for querying Prometheus metrics
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import json
from urllib.parse import urlencode
import pytz


class PrometheusBaseQuery:
    """Base class for Prometheus queries"""
    
    def __init__(self, prometheus_config: Dict[str, Any]):
        self.base_url = prometheus_config['url'].rstrip('/')
        self.headers = prometheus_config.get('headers', {})
        self.verify_ssl = prometheus_config.get('verify', False)
        self.logger = logging.getLogger(__name__)
        self.session = None
        
        # Set timezone to UTC
        self.timezone = pytz.UTC
        
        # Log configuration for debugging
        self.logger.info(f"Prometheus config - URL: {self.base_url}, SSL verify: {self.verify_ssl}")
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=60)  # Increased timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def _get_time_range(self, duration: str) -> tuple[datetime, datetime]:
        """Get start and end times for query range"""
        end_time = datetime.now(self.timezone)
        
        # Parse duration string (e.g., "1h", "30m", "1d")
        duration_map = {
            's': 'seconds',
            'm': 'minutes', 
            'h': 'hours',
            'd': 'days',
            'w': 'weeks'
        }
        
        duration = duration.lower().strip()
        if duration[-1] in duration_map:
            unit = duration_map[duration[-1]]
            try:
                value = int(duration[:-1])
                delta = timedelta(**{unit: value})
            except ValueError:
                self.logger.warning(f"Invalid duration format: {duration}, defaulting to 1 hour")
                delta = timedelta(hours=1)
        else:
            # Default to 1 hour if parsing fails
            self.logger.warning(f"Unable to parse duration: {duration}, defaulting to 1 hour")
            delta = timedelta(hours=1)
        
        start_time = end_time - delta
        self.logger.debug(f"Time range: {start_time} to {end_time} (duration: {duration})")
        return start_time, end_time
    
    def _format_timestamp(self, dt: datetime) -> str:
        """Format datetime for Prometheus API"""
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    async def query_instant(self, query: str, time: Optional[datetime] = None) -> Dict[str, Any]:
        """Execute instant query against Prometheus"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        params = {'query': query}
        if time:
            params['time'] = self._format_timestamp(time)
        
        url = f"{self.base_url}/api/v1/query"
        
        self.logger.debug(f"Executing instant query: {query}")
        
        try:
            async with self.session.get(url, params=params) as response:
                response_text = await response.text()
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        result_count = len(data.get('data', {}).get('result', []))
                        self.logger.debug(f"Query successful: {result_count} series returned")
                        
                        return {
                            'status': 'success',
                            'data': data.get('data', {}),
                            'query': query,
                            'timestamp': time or datetime.now(self.timezone)
                        }
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON response: {e}")
                        return {
                            'status': 'error',
                            'error': f"Invalid JSON response: {e}",
                            'query': query
                        }
                else:
                    self.logger.error(f"Prometheus query failed: {response.status} - {response_text}")
                    return {
                        'status': 'error',
                        'error': f"HTTP {response.status}: {response_text}",
                        'query': query,
                        'response_status': response.status
                    }
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP client error executing instant query: {e}")
            return {
                'status': 'error',
                'error': f"HTTP client error: {str(e)}",
                'query': query
            }
        except Exception as e:
            self.logger.error(f"Unexpected error executing instant query: {e}")
            return {
                'status': 'error',
                'error': f"Unexpected error: {str(e)}",
                'query': query
            }
    
    async def query_range(self, query: str, start: datetime, end: datetime, 
                         step: str = '15s') -> Dict[str, Any]:
        """Execute range query against Prometheus"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        params = {
            'query': query,
            'start': self._format_timestamp(start),
            'end': self._format_timestamp(end),
            'step': step
        }
        
        url = f"{self.base_url}/api/v1/query_range"
        
        self.logger.debug(f"Executing range query: {query} from {start} to {end}")
        
        try:
            async with self.session.get(url, params=params) as response:
                response_text = await response.text()
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        result_count = len(data.get('data', {}).get('result', []))
                        self.logger.debug(f"Range query successful: {result_count} series returned")
                        
                        return {
                            'status': 'success',
                            'data': data.get('data', {}),
                            'query': query,
                            'start': start,
                            'end': end,
                            'step': step
                        }
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON response: {e}")
                        return {
                            'status': 'error',
                            'error': f"Invalid JSON response: {e}",
                            'query': query
                        }
                else:
                    self.logger.error(f"Prometheus range query failed: {response.status} - {response_text}")
                    return {
                        'status': 'error',
                        'error': f"HTTP {response.status}: {response_text}",
                        'query': query,
                        'response_status': response.status
                    }
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP client error executing range query: {e}")
            return {
                'status': 'error',
                'error': f"HTTP client error: {str(e)}",
                'query': query
            }
        except Exception as e:
            self.logger.error(f"Unexpected error executing range query: {e}")
            return {
                'status': 'error',
                'error': f"Unexpected error: {str(e)}",
                'query': query
            }
    
    async def query_with_duration(self, query: str, duration: str = "1h") -> Dict[str, Any]:
        """Execute range query with duration string"""
        start_time, end_time = self._get_time_range(duration)
        return await self.query_range(query, start_time, end_time)
    
    def _extract_metric_values(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract metric values from Prometheus result"""
        if result['status'] != 'success':
            self.logger.debug("Cannot extract values from failed result")
            return []
        
        data = result.get('data', {})
        result_type = data.get('resultType', '')
        result_data = data.get('result', [])
        
        self.logger.debug(f"Extracting values from {len(result_data)} series of type {result_type}")
        
        extracted_values = []
        
        for item in result_data:
            metric_labels = item.get('metric', {})
            
            if result_type == 'vector':
                # Instant query result
                value_data = item.get('value', [])
                if len(value_data) >= 2:
                    timestamp, value = value_data[0], value_data[1]
                    if timestamp is not None and value is not None:
                        # Handle special float values
                        numeric_value = None
                        if value not in ['+Inf', '-Inf', 'NaN']:
                            try:
                                numeric_value = float(value)
                            except (ValueError, TypeError):
                                self.logger.debug(f"Could not convert value to float: {value}")
                        
                        extracted_values.append({
                            'labels': metric_labels,
                            'timestamp': float(timestamp),
                            'value': numeric_value
                        })
            
            elif result_type == 'matrix':
                # Range query result
                values = item.get('values', [])
                for value_data in values:
                    if len(value_data) >= 2:
                        timestamp, value = value_data[0], value_data[1]
                        if timestamp is not None and value is not None:
                            # Handle special float values
                            numeric_value = None
                            if value not in ['+Inf', '-Inf', 'NaN']:
                                try:
                                    numeric_value = float(value)
                                except (ValueError, TypeError):
                                    self.logger.debug(f"Could not convert value to float: {value}")
                            
                            extracted_values.append({
                                'labels': metric_labels,
                                'timestamp': float(timestamp),
                                'value': numeric_value
                            })
        
        self.logger.debug(f"Extracted {len(extracted_values)} data points")
        return extracted_values
    
    def _calculate_statistics(self, values: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate min, max, avg statistics from metric values"""
        if not values:
            return {'min': None, 'max': None, 'avg': None, 'count': 0, 'latest': None}
        
        # Filter out None values and ensure we have valid numbers
        numeric_values = []
        for v in values:
            val = v.get('value')
            if val is not None and isinstance(val, (int, float)) and not (
                val != val  # NaN check
            ):
                numeric_values.append(val)
        
        if not numeric_values:
            return {'min': None, 'max': None, 'avg': None, 'count': 0, 'latest': None}
        
        # Sort values by timestamp to get latest
        sorted_values = sorted(values, key=lambda x: x.get('timestamp', 0))
        latest_value = None
        for v in reversed(sorted_values):
            if v.get('value') is not None:
                latest_value = v['value']
                break
        
        return {
            'min': min(numeric_values),
            'max': max(numeric_values),
            'avg': sum(numeric_values) / len(numeric_values),
            'count': len(numeric_values),
            'latest': latest_value
        }
    
    async def query_with_stats(self, query: str, duration: str = "1h") -> Dict[str, Any]:
        """Execute query and return results with statistics"""
        result = await self.query_with_duration(query, duration)
        
        if result['status'] != 'success':
            return result
        
        # Extract values
        values = self._extract_metric_values(result)
        
        # Calculate overall statistics
        overall_stats = self._calculate_statistics(values)
        
        # Group by metric labels for multiple series
        series_stats = {}
        for value in values:
            labels_key = json.dumps(value['labels'], sort_keys=True)
            if labels_key not in series_stats:
                series_stats[labels_key] = {
                    'labels': value['labels'],
                    'values': []
                }
            series_stats[labels_key]['values'].append({
                'timestamp': value['timestamp'],
                'value': value['value']
            })
        
        # Calculate stats for each series
        for key in series_stats:
            series_values = series_stats[key]['values']
            series_stats[key]['statistics'] = self._calculate_statistics(series_values)
        
        return {
            'status': 'success',
            'query': query,
            'duration': duration,
            'overall_statistics': overall_stats,
            'series_count': len(series_stats),
            'series_data': list(series_stats.values()),
            'total_data_points': len(values)
        }
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to Prometheus"""
        try:
            self.logger.debug("Testing Prometheus connection")
            result = await self.query_instant('up')
            if result['status'] == 'success':
                targets_up = len(result['data'].get('result', []))
                self.logger.info(f"Prometheus connection successful, {targets_up} targets up")
                return {
                    'status': 'connected',
                    'prometheus_url': self.base_url,
                    'targets_up': targets_up
                }
            else:
                self.logger.error(f"Prometheus connection test failed: {result.get('error')}")
                return {
                    'status': 'connection_failed',
                    'error': result.get('error', 'Unknown error')
                }
        except Exception as e:
            self.logger.error(f"Error testing Prometheus connection: {e}")
            return {
                'status': 'connection_error',
                'error': str(e)
            }
    
    async def get_metric_metadata(self, metric_name: str) -> Dict[str, Any]:
        """Get metadata for a specific metric"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        url = f"{self.base_url}/api/v1/metadata"
        params = {'metric': metric_name}
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'status': 'success',
                        'data': data.get('data', {}),
                        'metric': metric_name
                    }
                else:
                    error_text = await response.text()
                    return {
                        'status': 'error',
                        'error': f"HTTP {response.status}: {error_text}",
                        'metric': metric_name
                    }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'metric': metric_name
            }
    
    async def get_label_values(self, label_name: str) -> Dict[str, Any]:
        """Get all values for a specific label"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        url = f"{self.base_url}/api/v1/label/{label_name}/values"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'status': 'success',
                        'data': data.get('data', []),
                        'label': label_name
                    }
                else:
                    error_text = await response.text()
                    return {
                        'status': 'error',
                        'error': f"HTTP {response.status}: {error_text}",
                        'label': label_name
                    }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'label': label_name
            }
    
    async def query_debug(self, query: str, duration: str = "1h") -> Dict[str, Any]:
        """Execute query with detailed debugging information"""
        debug_info = {
            'query': query,
            'duration': duration,
            'timestamp': datetime.now(self.timezone).isoformat(),
            'steps': {}
        }
        
        try:
            # Step 1: Test basic connection
            debug_info['steps']['connection_test'] = await self.test_connection()
            
            if debug_info['steps']['connection_test']['status'] != 'connected':
                debug_info['status'] = 'error'
                debug_info['error'] = 'Connection test failed'
                return debug_info
            
            # Step 2: Try instant query first
            self.logger.debug(f"Debug: Testing instant query: {query}")
            instant_result = await self.query_instant(query)
            debug_info['steps']['instant_query'] = {
                'status': instant_result['status'],
                'series_count': len(instant_result.get('data', {}).get('result', [])),
                'error': instant_result.get('error')
            }
            
            # Step 3: Try range query
            self.logger.debug(f"Debug: Testing range query: {query}")
            range_result = await self.query_with_duration(query, duration)
            debug_info['steps']['range_query'] = {
                'status': range_result['status'],
                'series_count': len(range_result.get('data', {}).get('result', [])),
                'error': range_result.get('error')
            }
            
            # Step 4: Try with stats
            if range_result['status'] == 'success':
                stats_result = await self.query_with_stats(query, duration)
                debug_info['steps']['stats_processing'] = {
                    'status': stats_result['status'],
                    'series_count': stats_result.get('series_count', 0),
                    'data_points': stats_result.get('total_data_points', 0),
                    'overall_stats': stats_result.get('overall_statistics', {}),
                    'error': stats_result.get('error')
                }
                
                if stats_result['status'] == 'success':
                    debug_info['status'] = 'success'
                    debug_info['final_result'] = stats_result
                else:
                    debug_info['status'] = 'partial_success'
                    debug_info['error'] = 'Stats processing failed'
            else:
                debug_info['status'] = 'error'
                debug_info['error'] = 'Range query failed'
            
            return debug_info
            
        except Exception as e:
            debug_info['status'] = 'error'
            debug_info['error'] = str(e)
            debug_info['exception'] = type(e).__name__
            return debug_info