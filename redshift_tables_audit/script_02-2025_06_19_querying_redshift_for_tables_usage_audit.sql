-- Redshift Tables Audit

-- 1) List all user-defined tables and views in Redshift (BASE TABLE, VIEW, LATE BINDING VIEW)
-- ps.: A late-binding view in Redshift is a view that defers dependency checks on referenced objects until query time,
--      allowing it to be created even if the underlying tables or columns don't yet exist.

-- 1.1) List BASE TABLE and VIEW - here you query "svv_tables"
-- There are 482 views and 3522 BASE TABLES
SELECT table_schema, table_name, table_type
FROM svv_tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');

-- Couting the above
--SELECT table_type, COUNT(*) AS object_count
--FROM svv_tables
--WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
--GROUP BY table_type;

-- 1.2) List LATE BINDING VIEW - here you query from pg_class or pg_views
-- There are 402 LATE BINDING VIEW
SELECT 
  schemaname AS view_schema,
  viewname AS view_name,
  definition,
  'late-binding' AS binding_type
FROM pg_views
WHERE definition ILIKE '%with no schema binding%'
  AND schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, viewname;


-- Counting the above
--SELECT 
--  COUNT(*) AS late_binding_view_count
--FROM pg_views
--WHERE definition ILIKE '%with no schema binding%'
--  AND schemaname NOT IN ('pg_catalog', 'information_schema');

-- 1.3) List all objects (BASE TABLE, VIEW) and (LATE BINDING VIEW) with UNION ALL
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

-- 1.4) Final Count of all objects (BASE TABLE, VIEW) and (LATE BINDING VIEW)
-- VIEW: 482
-- BASE TABLE: 3522
-- LATE BINDING VIEW: 402

-- Counting the above
--SELECT table_type, COUNT(*) AS object_count
--FROM svv_tables
--WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
--GROUP BY table_type
--UNION ALL
--SELECT 'LATE BINDING VIEW' AS table_type, COUNT(*) AS object_count
--FROM pg_views
--WHERE definition ILIKE '%with no schema binding%'
--  AND schemaname NOT IN ('pg_catalog', 'information_schema');


-- 2) Add Usage Statistics for all objects
-- WE ARE NOT ADDING THE LATE BINDING VIEWS BECAUSE IT DOES NOT WORK IN THE CTE FORMAT

-- CTE 1: Get all user-defined base tables and views (excluding system schemas)
WITH all_objects AS (
    SELECT 
        c.oid AS object_oid,                 -- Unique object ID (used to join with scan data)
        n.nspname AS schema_name,            -- Schema name (e.g. analytics, staging)
        c.relname AS object_name,            -- Table or view name
        CASE                                 -- Determine object type based on Redshift internal tag:
            WHEN c.relkind = 'r' THEN 'BASE TABLE'  -- 'r' = ordinary table
            WHEN c.relkind = 'v' THEN 'VIEW'        -- 'v' = view
            ELSE 'OTHER'                            -- Just in case other relkinds exist
        END AS object_type
    FROM pg_class c                         -- System catalog: all relations (tables, views, etc.)
    JOIN pg_namespace n ON c.relnamespace = n.oid  -- Join to get schema name
    WHERE c.relkind IN ('r', 'v')           -- Only include base tables and views
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system/internal schemas
),

-- CTE 2: Track object usage (via scan logs) over the past 30 days
scanned_objects AS (
    SELECT
        s.tbl,                                   -- Object ID (used to join with pg_class)
        MAX(q.starttime) AS last_query_time,     -- Most recent time the object was scanned
        COUNT(*) AS scan_count,                  -- Number of times the object was scanned
        MAX(u.usename) AS queried_by             -- Last user who scanned the object
    FROM stl_scan s                              -- Redshift log: every table scan operation
    JOIN stl_query q ON s.query = q.query        -- Get query metadata (start time)
    JOIN pg_user u ON q.userid = u.usesysid      -- Get user info
    WHERE q.starttime > current_date - interval '30 days'  -- Filter to the past 30 days
    GROUP BY s.tbl                               -- One row per object ID
)

-- Final result: Join object metadata with recent usage (if any)
SELECT 
    a.schema_name,           -- Object's schema
    a.object_name,           -- Object's name
    a.object_type,           -- Either 'BASE TABLE' or 'VIEW'
    CASE 
        WHEN s.tbl IS NOT NULL THEN TRUE         -- If there was a scan, object is used
        ELSE FALSE                               -- Otherwise, object is unused
    END AS is_used,
    s.queried_by,            -- Who last queried the object (NULL if unused)
    s.last_query_time,       -- When it was last queried (NULL if unused)
    s.scan_count             -- Number of scans (NULL if unused)
FROM all_objects a
LEFT JOIN scanned_objects s ON a.object_oid = s.tbl  -- Join usage data (if any)
ORDER BY is_used DESC, a.schema_name, a.object_name; -- Prioritize used objects


-- Counting the above
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

-- Final count by object type
SELECT
    object_type,
    COUNT(*) AS object_count
FROM all_objects
GROUP BY object_type
ORDER BY object_type;

-- VIEW: 479
-- BASE TABLE: 3528
