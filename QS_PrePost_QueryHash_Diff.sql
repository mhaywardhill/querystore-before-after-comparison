DECLARE @preStart  datetime2(0) = '2026-03-25T07:00:00';
DECLARE @preEnd    datetime2(0) = '2026-03-25T11:00:00';
DECLARE @postStart datetime2(0) = '2026-03-25T12:00:00';
DECLARE @postEnd   datetime2(0) = '2026-03-25T23:00:00';

;WITH base AS
(
    SELECT
        CASE
            WHEN rsi.start_time >= @preStart  AND rsi.start_time < @preEnd  THEN 'pre'
            WHEN rsi.start_time >= @postStart AND rsi.start_time < @postEnd THEN 'post'
            ELSE 'other'
        END AS [window],
        qsq.query_hash,
        qsp.plan_id,
        CAST(rs.avg_cpu_time AS float) * CAST(rs.count_executions AS float) AS cpu_us,
        CAST(rs.count_executions AS float) AS exec_count,
        CAST(rs.avg_logical_io_reads AS float)  * CAST(rs.count_executions AS float) AS logical_reads,
        CAST(rs.avg_physical_io_reads AS float) * CAST(rs.count_executions AS float) AS physical_reads
    FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_runtime_stats_interval rsi
      ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    JOIN sys.query_store_plan qsp
      ON rs.plan_id = qsp.plan_id
    JOIN sys.query_store_query qsq
      ON qsp.query_id = qsq.query_id
    WHERE
        (rsi.start_time >= @preStart  AND rsi.start_time < @preEnd)
        OR
        (rsi.start_time >= @postStart AND rsi.start_time < @postEnd)
),
perHash AS
(
    SELECT
        [window],
        query_hash,
        SUM(cpu_us)         AS cpu_us,
        SUM(exec_count)     AS exec_count,
        SUM(logical_reads)  AS logical_reads,
        SUM(physical_reads) AS physical_reads
    FROM base
    WHERE [window] IN ('pre','post')
    GROUP BY [window], query_hash
),
perPlan AS
(
    SELECT
        [window],
        query_hash,
        plan_id,
        SUM(cpu_us)     AS plan_cpu_us,
        SUM(exec_count) AS plan_exec_count
    FROM base
    WHERE [window] IN ('pre','post')
    GROUP BY [window], query_hash, plan_id
),
dominantPlan AS
(
    SELECT
        [window],
        query_hash,
        plan_id,
        ROW_NUMBER() OVER
        (
            PARTITION BY [window], query_hash
            ORDER BY plan_cpu_us DESC, plan_exec_count DESC, plan_id DESC
        ) AS rn
    FROM perPlan
),
prePlan AS
(
    SELECT query_hash, CAST(plan_id AS varchar(50)) AS pre_dominant_plan_id
    FROM dominantPlan
    WHERE [window] = 'pre' AND rn = 1
),
postPlan AS
(
    SELECT query_hash, CAST(plan_id AS varchar(50)) AS post_dominant_plan_id
    FROM dominantPlan
    WHERE [window] = 'post' AND rn = 1
),
preAgg AS
(
    SELECT
        query_hash,
        cpu_us AS pre_cpu_us,
        exec_count AS pre_exec_count,
        (logical_reads + physical_reads) AS pre_total_reads
    FROM perHash
    WHERE [window] = 'pre'
),
postAgg AS
(
    SELECT
        query_hash,
        cpu_us AS post_cpu_us,
        exec_count AS post_exec_count,
        (logical_reads + physical_reads) AS post_total_reads
    FROM perHash
    WHERE [window] = 'post'
),
merged AS
(
    SELECT
        COALESCE(p.query_hash, s.query_hash) AS query_hash,
        COALESCE(p.pre_cpu_us, 0.0)        AS pre_cpu_us,
        COALESCE(s.post_cpu_us, 0.0)       AS post_cpu_us,
        COALESCE(p.pre_exec_count, 0.0)    AS pre_exec_count,
        COALESCE(s.post_exec_count, 0.0)   AS post_exec_count,
        COALESCE(p.pre_total_reads, 0.0)   AS pre_total_reads,
        COALESCE(s.post_total_reads, 0.0)  AS post_total_reads,
        pp.pre_dominant_plan_id,
        sp.post_dominant_plan_id
    FROM preAgg p
    FULL OUTER JOIN postAgg s
      ON p.query_hash = s.query_hash
    LEFT JOIN prePlan pp
      ON pp.query_hash = COALESCE(p.query_hash, s.query_hash)
    LEFT JOIN postPlan sp
      ON sp.query_hash = COALESCE(p.query_hash, s.query_hash)
),
calc AS
(
    SELECT
        query_hash,
        pre_dominant_plan_id,
        post_dominant_plan_id,
        CASE
            WHEN pre_dominant_plan_id IS NOT NULL
             AND post_dominant_plan_id IS NOT NULL
             AND pre_dominant_plan_id <> post_dominant_plan_id THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
        END AS plan_change,
        pre_cpu_us,
        post_cpu_us,
        post_cpu_us - pre_cpu_us AS delta_cpu_us,
        pre_exec_count,
        post_exec_count,
        post_exec_count - pre_exec_count AS delta_exec_count,
        CASE WHEN pre_exec_count  > 0 THEN pre_cpu_us  / pre_exec_count  END AS pre_cpu_per_exec_us,
        CASE WHEN post_exec_count > 0 THEN post_cpu_us / post_exec_count END AS post_cpu_per_exec_us,
        pre_total_reads,
        post_total_reads,
        post_total_reads - pre_total_reads AS delta_total_reads,
        CASE
            WHEN pre_exec_count = 0 AND post_exec_count > 0 THEN 'new_in_post'
            WHEN pre_exec_count > 0 AND post_exec_count = 0 THEN 'missing_in_post'
            WHEN pre_exec_count > 0 AND post_exec_count > 0 THEN 'in_both'
            ELSE 'no_data'
        END AS presence_status
    FROM merged
),
totals AS
(
    SELECT
        SUM(delta_cpu_us) AS total_delta_cpu_us,
        SUM(CASE WHEN delta_cpu_us > 0 THEN delta_cpu_us ELSE 0 END) AS total_positive_delta_cpu_us
    FROM calc
)
SELECT TOP (20)
    query_hash = sys.fn_varbintohexstr(c.query_hash),
    c.presence_status,
    c.pre_dominant_plan_id,
    c.post_dominant_plan_id,
    c.plan_change,
    c.pre_cpu_us,
    c.post_cpu_us,
    c.delta_cpu_us,
    CASE
        WHEN t.total_delta_cpu_us <> 0
        THEN (c.delta_cpu_us / t.total_delta_cpu_us) * 100.0
    END AS contribution_pct_of_net_delta,
    CASE
        WHEN t.total_positive_delta_cpu_us > 0
        THEN (c.delta_cpu_us / t.total_positive_delta_cpu_us) * 100.0
    END AS contribution_pct_of_positive_delta,
    c.pre_exec_count,
    c.post_exec_count,
    c.delta_exec_count,
    c.pre_cpu_per_exec_us,
    c.post_cpu_per_exec_us,
    c.pre_total_reads,
    c.post_total_reads,
    c.delta_total_reads
FROM calc c
CROSS JOIN totals t
ORDER BY c.post_cpu_us DESC;