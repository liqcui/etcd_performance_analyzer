2025-09-25 06:22:01,601 - __main__ - INFO - Generating performance report...
2025-09-25 06:22:01,602 - __main__ - INFO - Performance report generation completed
2025-09-25 06:22:01,602 - __main__ - INFO - Displaying performance analysis results...
====================================================================================================
ETCD PERFORMANCE ANALYSIS REPORT
====================================================================================================
Test ID: 4d0538ee-a257-4e27-bccf-101f73be0b67
Analysis Duration: 1h
Report Generated: 2025-09-25 06:22:01 UTC
Analysis Status: SUCCESS

EXECUTIVE SUMMARY
==================================================

Overall Disk Performance Health: CRITICAL
Performance Grade: GOOD

CRITICAL ALERTS
==================================================

[CRITICAL] WAL fsync latency exceeds critical threshold (>10ms)
Impact: High write latency affecting cluster stability
Action Required: Immediate storage performance investigation required

[CRITICAL] Backend commit latency exceeds critical threshold (>25ms)
Impact: Database write performance significantly degraded
Action Required: Immediate storage optimization required

CRITICAL METRICS ANALYSIS
==================================================

WAL Fsync Performance:
  Health Status: CRITICAL
  Average Latency: 10.107 ms
  Maximum Latency: 18.168 ms
  Threshold: 10.0 ms
  Pods with Issues: 2/3

Backend Commit Performance:
  Health Status: CRITICAL
  Average Latency: 15.873 ms
  Maximum Latency: 46.81 ms
  Threshold: 25.0 ms
  Pods with Issues: 3/3

CPU Performance:
  Health Status: GOOD
  Average Usage: 5.88%
  Maximum Usage: 27.29%
  Critical Pods: 0/3

Memory Performance:
  Health Status: GOOD
  Average Usage: 2.37 MB
  Maximum Usage: 4.22 MB
  Critical Pods: 0/3

Network Performance:
  Overall Health: GOOD
  Peer Latency: 12.672 ms avg, 12.672 ms max
  Pods with Latency Issues: 0/3

DETAILED METRICS
==================================================

WAL Fsync P99 Latency
=====================
Pod Name                                           Avg (ms)     Max (ms)     Status
------------------------------------------------------------------------------------
etcd-ci-op-xxxxx-master-0           7.956        9.525        GOOD
etcd-ci-op-xxxxx-master-1           9.949        18.168       WARNING
etcd-ci-op-xxxxx-master-2           12.417       15.611       CRITICAL

Threshold: 10.0 ms


Backend Commit P99 Latency
==========================
Pod Name                                           Avg (ms)     Max (ms)     Status
------------------------------------------------------------------------------------
etcd-ci-op-xxxxx-master-0           14.459       25.142       WARNING
etcd-ci-op-xxxxx-master-1           16.376       46.810       WARNING
etcd-ci-op-xxxxx-master-2           16.785       25.822       WARNING

Threshold: 25.0 ms


CPU Usage
=========
Pod Name                                           Avg (%)      Max (%)      Status
------------------------------------------------------------------------------------
etcd-ci-op-xxxxx-master-0           6.80         24.99        GOOD
etcd-ci-op-xxxxx-master-1           4.86         13.08        GOOD
etcd-ci-op-xxxxx-master-2           5.98         27.29        GOOD


Memory Usage
============
Pod Name                                           Avg (MB)     Max (MB)     Status
------------------------------------------------------------------------------------
etcd-ci-op-xxxxx-master-0           395.41       630.54       GOOD
etcd-ci-op-xxxxx-master-1           388.07       691.67       GOOD
etcd-ci-op-xxxxx-master-2           379.25       579.67       GOOD

BASELINE COMPARISON
==================================================

Metric                              Current         Target          Status
---------------------------------------------------------------------------
Wal Fsync P99 Ms                    10.107          10.0            FAIL
Backend Commit P99 Ms               15.873          25.0            PASS
Cpu Usage Percent                   5.88            70.0            PASS
Memory Usage Percent                2.37            70.0            PASS

Overall Performance Grade: GOOD

RECOMMENDATIONS
==================================================

HIGH PRIORITY
-------------

1. High WAL fsync latency detected
   Category: Disk Performance
   Recommendation: Upgrade to high-performance NVMe SSDs with low latency
   Rationale: Ensure etcd gets adequate CPU resources over other processes

ANALYSIS METHODOLOGY
==================================================

This report analyzes etcd performance based on industry best practices:

Critical Thresholds:
• WAL fsync P99 latency should be < 10.0 ms
• Backend commit P99 latency should be < 25.0 ms
• CPU usage should remain below 70.0% (warning) / 85.0% (critical)
• Memory usage monitoring for resource planning
• Network peer latency should be < 50.0 ms

Performance degradation can be caused by:
1. Slow disk I/O (most common) - upgrade to NVMe SSDs, dedicated storage
2. CPU starvation - increase CPU resources, process isolation
3. Network issues - optimize topology, increase bandwidth
4. Memory pressure - monitor and increase as needed

Data Sources:
• Metrics collection duration: 1h
• Test ID: 4d0538ee-a257-4e27-bccf-101f73be0b67
• Collection timestamp: 2025-09-25T06:21:55.335598+00:00

====================================================================================================
END OF REPORT
====================================================================================================

====================================================================================================
DETAILED ROOT CAUSE ANALYSIS
====================================================================================================

FAILED THRESHOLDS SUMMARY:
--------------------------------------------------
[CRITICAL] WAL_FSYNC_P99
  Current Value: 10.107
  Threshold: 10.0
  Affected Pods: 2/3

[CRITICAL] BACKEND_COMMIT_P99
  Current Value: 15.873
  Threshold: 25.0
  Affected Pods: 3/3


SCRIPT-BASED ROOT CAUSE ANALYSIS:
==================================================

🔍 DISK I/O PERFORMANCE ANALYSIS:
----------------------------------------
  Cluster Write Performance: 0.98 MB/s across 3 nodes
  Performance Grade: POOR
  ⚠️  LOW THROUGHPUT DETECTED - This is likely the PRIMARY root cause!

  📊 DETECTED I/O BOTTLENECKS:
    🔴 [HIGH] ci-op-xxxxx-master-0: 0.43 MB/s
        Node ci-op-xxxxx-master-0 showing low write throughput (0.43 MB/s)
    🔴 [HIGH] ci-op-xxxxx-master-1: 1.87 MB/s
        Node ci-op-xxxxx-master-1 showing low write throughput (1.87 MB/s)
    🔴 [HIGH] ci-op-xxxxx-master-2: 0.65 MB/s
        Node ci-op-xxxxx-master-2 showing low write throughput (0.65 MB/s)
    🔴 [HIGH] ci-op-xxxxx-master-0: 0 IOPS
        Node ci-op-xxxxx-master-0 showing low IOPS (0), etcd requires 1000+ write IOPS
    🔴 [HIGH] ci-op-xxxxx-master-1: 0 IOPS
        Node ci-op-xxxxx-master-1 showing low IOPS (0), etcd requires 1000+ write IOPS
    🔴 [HIGH] ci-op-xxxxx-master-2: 0 IOPS
        Node ci-op-xxxxx-master-2 showing low IOPS (0), etcd requires 1000+ write IOPS
    🔴 [HIGH] ci-op-xxxxx-master-0: 72 IOPS
        Node ci-op-xxxxx-master-0 showing low IOPS (72), etcd requires 1000+ write IOPS
    🔴 [HIGH] ci-op-xxxxx-master-1: 78 IOPS
        Node ci-op-xxxxx-master-1 showing low IOPS (78), etcd requires 1000+ write IOPS
    🔴 [HIGH] ci-op-xxxxx-master-2: 72 IOPS
        Node ci-op-xxxxx-master-2 showing low IOPS (72), etcd requires 1000+ write IOPS

  💡 STORAGE OPTIMIZATION RECOMMENDATIONS:
    1. Upgrade to high-performance NVMe SSDs with sustained write performance > 100 MB/s
    2. Ensure dedicated storage for etcd data directory with minimum 3000 IOPS
    3. Consider enterprise-grade storage with consistent low latency characteristics
    4. Verify storage is not shared with other I/O intensive workloads
    5. Check for storage controller bottlenecks or outdated drivers

🌐 NETWORK PERFORMANCE ANALYSIS:
----------------------------------------
  Average Peer Latency: 12.672 ms
  Network Grade: GOOD
  High Latency Pods: 0/3

  💡 NETWORK OPTIMIZATION RECOMMENDATIONS:
    1. Network performance appears healthy for etcd cluster operations
    2. Continue monitoring network latency and utilization trends

⚖️  CONSENSUS HEALTH ANALYSIS:
----------------------------------------
  🟡 CONSENSUS RISK LEVEL: MEDIUM
  Affected Pods: 1
  ⚠️  LEADER ELECTION INSTABILITY RISK:
    Reason: 1 pods showing high I/O latency that may affect consensus
    Affected Pods: etcd-ci-op-xxxxx-master-2

  💡 CONSENSUS STABILITY RECOMMENDATIONS:
    1. High I/O latency detected on multiple nodes may cause consensus instability
    2. Consider isolating slow nodes or improving their storage performance
    3. Monitor for frequent leader elections which indicate consensus issues
    4. Verify etcd heartbeat and election timeout configurations

🤖 AI-POWERED ROOT CAUSE ANALYSIS:
==================================================

📋 ROOT CAUSE ANALYSIS SUMMARY:
----------------------------------------
  • Storage I/O Performance (High Confidence)
  • Network Connectivity Issues (Medium Confidence)
  • Resource Contention (Medium Confidence)

🎯 RECOMMENDED IMMEDIATE ACTIONS:
  1. Focus on storage performance optimization (highest impact)
  2. Verify network connectivity and latency between etcd nodes
  3. Ensure adequate CPU/memory resources for etcd pods
  4. Monitor for improvements after each optimization step

================================================================================
ANALYSIS SUMMARY
================================================================================
Success: True
Test ID: 4d0538ee-a257-4e27-bccf-101f73be0b67
Metrics Collected: True
Analysis Completed: True
Report Generated: True
================================================================================