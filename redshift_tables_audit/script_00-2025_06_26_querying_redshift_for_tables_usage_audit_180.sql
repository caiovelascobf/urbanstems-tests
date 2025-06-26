-- Redshift Tables Audit (180-day version)

-- 1) List all user-defined tables and views in Redshift (BASE TABLE, VIEW, LATE BINDING VIEW)
-- A late-binding view defers dependency checks until query time.

-- 1.1) List BASE TABLE and VIEW from svv_tables
-- VIEW: 482, BASE TABLE: 3522 (as of original stats)
SELECT table_schema, table_name, table_type
FROM svv_tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');

-- 1.2) List LATE BINDING VIEW from pg_views
SELECT 
  schemaname AS view_schema,
  viewname AS view_name,
  definition,
  'late-binding' AS binding_type
FROM pg_views
WHERE definition ILIKE '%with no schema binding%'
  AND schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, viewname;

-- 1.3) UNION ALL: BASE TABLE + VIEW + LATE BINDING VIEW
SELECT 
  table_schema,
  table_name,
  table_type
FROM svv_tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
UNION ALL
SELECT 
  schemaname AS table_schema,
  viewname AS table_name,
  'LATE BINDING VIEW' AS table_type
FROM pg_views
WHERE definition ILIKE '%with no schema binding%'
  AND schemaname NOT IN ('pg_catalog', 'information_schema');

-- 1.4) Final object count summary
-- VIEW: 479, BASE TABLE: 3528, LATE BINDING VIEW: 402 (original counts)
-- (optional) count query here if needed

-- 2) Add Usage Statistics (180-day version)
-- LATE BINDING VIEWS EXCLUDED from usage since they are not tracked in stl_scan

-- CTE 1: List all base tables and views (excluding system schemas)
WITH all_objects AS (
    SELECT 
        c.oid AS object_oid,
        n.nspname AS schema_name,
        c.relname AS object_name,
        CASE
            WHEN c.relkind = 'r' THEN 'BASE TABLE'
            WHEN c.relkind = 'v' THEN 'VIEW'
            ELSE 'OTHER'
        END AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relkind IN ('r', 'v')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
),

-- CTE 2: Scan log activity in the last 180 days
scanned_objects AS (
    SELECT
        s.tbl,
        MAX(q.starttime) AS last_query_time,
        COUNT(*) AS scan_count,
        MAX(u.usename) AS queried_by
    FROM stl_scan s
    JOIN stl_query q ON s.query = q.query
    JOIN pg_user u ON q.userid = u.usesysid
    WHERE q.starttime > current_date - interval '180 days'
    GROUP BY s.tbl
)

-- Final output: Join table/view metadata with scan usage (180-day window)
SELECT 
    a.schema_name,
    a.object_name,
    a.object_type,
    CASE 
        WHEN s.tbl IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_used,
    s.queried_by,
    s.last_query_time,
    s.scan_count
FROM all_objects a
LEFT JOIN scanned_objects s ON a.object_oid = s.tbl
ORDER BY is_used DESC, a.schema_name, a.object_name;

-- Optional: Count objects by type
WITH all_objects AS (
    SELECT 
        c.oid AS object_oid,
        n.nspname AS schema_name,
        c.relname AS object_name,
        CASE
            WHEN c.relkind = 'r' THEN 'BASE TABLE'
            WHEN c.relkind = 'v' THEN 'VIEW'
            ELSE 'OTHER'
        END AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relkind IN ('r', 'v')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
)

SELECT
    object_type,
    COUNT(*) AS object_count
FROM all_objects
GROUP BY object_type
ORDER BY object_type;