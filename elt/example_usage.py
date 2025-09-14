#!/usr/bin/env python3
"""
Usage example showing how to use the ELT modules together
to process cluster information and generate HTML tables
"""

import json
from elt.etcd_analyzer_elt_cluster_info import ClusterInfoELT
from elt.etcd_analyzer_elt_json2table import (
    convert_cluster_metrics_to_html_table, 
    json_to_html_table, 
    create_summary_table
)
from elt.etcd_analyzer_elt_utility import (
    load_json_file, 
    format_bytes, 
    format_number,
    clean_metric_name
)

def process_cluster_info_file(file_path: str) -> str:
    """Process cluster info file and generate HTML tables"""
    try:
        # Load JSON data
        json_data = load_json_file(file_path)
        
        # Extract cluster information
        extractor = ClusterInfoELT()
        cluster_data = extractor.extract_cluster_info(json_data)
        
        # Get formatted metrics for table display
        metrics = extractor.get_metrics_for_table()
        
        # Generate HTML tables
        html_output = ""
        
        # Summary table
        html_output += create_summary_table(cluster_data, "Cluster Overview Summary")
        html_output += "<br>"
        
        # Detailed metrics table
        html_output += convert_cluster_metrics_to_html_table(
            metrics, 
            "Detailed Cluster Metrics",
            show_categories=True
        )
        html_output += "<br>"
        
        # Node details table (if available)
        if 'node_details' in cluster_data:
            node_details = cluster_data['node_details']
            
            # Master nodes
            if node_details['master_nodes']:
                html_output += json_to_html_table(
                    node_details['master_nodes'],
                    title="Master Nodes Details"
                )
                html_output += "<br>"
            
            # Worker nodes
            if node_details['worker_nodes']:
                html_output += json_to_html_table(
                    node_details['worker_nodes'],
                    title="Worker Nodes Details"
                )
                html_output += "<br>"
        
        return html_output
        
    except Exception as e:
        return f"<p>Error processing cluster info: {e}</p>"

# Example usage with the uploaded cluster_info.json
def main():
    # Sample data from the uploaded file
    sample_data = {
        "tool": "get_ocp_cluster_info",
        "params_used": {},
        "result": {
            "status": "success",
            "data": {
                "cluster_name": "perfscale-qe-ocp-sqz6n",
                "cluster_version": "4.19.10",
                "platform": "BareMetal",
                "total_nodes": 5,
                "master_nodes": [
                    {
                        "name": "openshift-qe-019.lab.eng.rdu2.redhat.com",
                        "cpu_capacity": "32",
                        "memory_capacity": "131451488Ki",
                        "ready_status": "Ready",
                        "schedulable": True
                    }
                ],
                "worker_nodes": [
                    {
                        "name": "openshift-qe-017.lab.eng.rdu2.redhat.com",
                        "cpu_capacity": "32", 
                        "memory_capacity": "131451484Ki",
                        "ready_status": "Ready",
                        "schedulable": True
                    }
                ],
                "pods_count": 307,
                "namespaces_count": 78,
                "services_count": 101,
                "unavailable_cluster_operators": []
            }
        }
    }
    
    # Process the data
    extractor = ClusterInfoELT()
    cluster_data = extractor.extract_cluster_info(sample_data)
    metrics = extractor.get_metrics_for_table()
    
    # Generate HTML
    html = convert_cluster_metrics_to_html_table(metrics, "Sample Cluster Metrics")
    
    print("Generated HTML table:")
    print(html)

if __name__ == "__main__":
    main()