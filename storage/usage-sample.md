# Example 1: Collect and store current cluster and all metrics including general info
logger.info("=== Collect Current Metrics (All Types Including General Info) ===")
result1 = await agent.query_by_duration(duration="1h", print_table_info=True)

# Example 2: Query by specific time range (UTC) - collect new data including general info
logger.info("=== Query by Time Range (UTC) - New Data Collection ===")
start_time = "2025-09-20T06:00:00Z"
end_time = "2025-09-20T07:00:00Z" 
result2 = await agent.query_by_time_range(start_time, end_time)

# Example 3: Query stored data by duration (all metrics including general info)
logger.info("=== Query Stored Data (Last 2 Hours) - All Metrics ===")
stored_result = await agent.query_by_duration(duration="2h", query_stored_only=True)

# Example 4: Query stored data by time range including general info
logger.info("=== Query Stored Data by Time Range - Including General Info ===")
stored_time_result = await agent.query_by_time_range(
    start_time="2025-09-20T06:00:00Z", 
    end_time="2025-09-20T08:00:00Z",
    query_stored_only=True
)

# Example 5: Collect new data with different duration including general info metrics
logger.info("=== Collect 30-minute metrics (All Types) ===")
result5 = await agent.query_by_duration(duration="30m")

# Example 6: Test general info specific functionality
logger.info("=== General Info Specific Testing ===")
if result5.get("status") == "success" and result5.get("general_info_summary"):
    general_info_summary = result5["general_info_summary"]