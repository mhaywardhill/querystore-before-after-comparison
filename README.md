# Query Store Before/After Comparison

Compare SQL Server Query Store performance by `query_hash` across two time windows (pre vs post), including plan-change detection and CPU/read deltas.

## Files

- `setup_demo.sql`
	- Creates a demo workload (`dbo.OrderHistory` + `dbo.GetCustomerOrders`) and intentionally produces plan variation in Query Store.
- `QS_PrePost_QueryHash_Diff.sql`
	- Produces a top-20 before/after comparison grouped by `query_hash`.

## Prerequisites

- SQL Server with Query Store available.
- Permissions to read Query Store DMVs/views:
	- `sys.query_store_runtime_stats`
	- `sys.query_store_runtime_stats_interval`
	- `sys.query_store_plan`
	- `sys.query_store_query`
- A database with enough workload in both the pre and post windows.

## Quick Start

1. (Optional demo) Run `setup_demo.sql` in a test database to generate sample workload and plan changes.
2. Open `QS_PrePost_QueryHash_Diff.sql`.
3. Update the window variables at the top of the script:
	 - `@preStart`, `@preEnd`
	 - `@postStart`, `@postEnd`
4. Execute the script.
5. Review the returned top 20 rows ordered by `post_cpu_us`.

## Output Highlights

- `query_hash`
	- Hex value for the logical query shape.
- `presence_status`
	- `new_in_post`: no pre executions, appears post.
	- `missing_in_post`: appears pre, not post.
	- `in_both`: appears in both windows.
- `pre_dominant_plan_id`, `post_dominant_plan_id`
	- Highest-CPU plan per window for that query hash.
- `plan_change`
	- `1` when dominant plan differs between windows.
- `delta_cpu_us`, `delta_exec_count`, `delta_total_reads`
	- Absolute pre/post change metrics.
- `contribution_pct_of_net_delta`
	- Share of overall net CPU delta.
- `contribution_pct_of_positive_delta`
	- Share of total CPU increases only.

## Notes

- CPU and reads are weighted by execution count (aggregated from average metrics).
- Windows are half-open intervals: start inclusive, end exclusive.
- This script is intended for troubleshooting and trend comparison, not exact accounting.