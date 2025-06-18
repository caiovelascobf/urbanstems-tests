-- General Redshift exploration
SELECT * FROM information_schema.schemata; -- no results
SELECT * FROM information_schema.tables;

-- ALL DATABASES
-- Listing all databases (also outside the analytics database)
SELECT datname FROM pg_database;

-- USER SCHEMAS
-- List all non-system schemas (under the analytics database)
SELECT nspname AS schema_name 
FROM pg_namespace 
WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
ORDER BY schema_name;

-- Count all user schemas (under the analytics database)
-- There are 187 schemas
SELECT COUNT(*) AS schema_count
FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';

-- USER TABLES
-- List all user-defined schemas
SELECT table_schema, table_name 
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;

-- Count tables in user-defined schemas
-- There are 3985 tables
SELECT COUNT(*) AS table_count
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');


-- REDSHIFT SCAN
-- (The queries below ONLY includes tables that had AT LEAST ONE SCAN in the last 30 days)
-- A table scan in Redshift means the engine read data from the table.
-- This happens during any read-related operation: SELECT, JOIN, COPY, etc.
-- The system table stl_scan logs each scan event (what table, when, how many rows, etc.)
-- Question: “Was this table queried recently?”
-- The most accurate signal is: A record in stl_scan (a scan happened), Joined with stl_query (to see who and when)

-- List of tables that were scanned (i.e., used) in the last 30 days.
SELECT DISTINCT c.relname AS table_name -- relname is the table name
FROM stl_scan s							-- system log of table scans (each time Redshift reads from a table)
JOIN pg_class c ON s.tbl = c.oid		-- metadata about tables (name, ID, etc.), match scan record to table by object ID
WHERE s.starttime > current_date - interval '30 days'  -- only include scans in the last 30 days
LIMIT 100;

-- Count how many times each table (with schema) was scanned in the past 30 days
SELECT 
	c.relname AS table_name, 	  -- relname is the table name
	COUNT(*) AS scan_count   	  -- Count how many times this table appeared in stl_scan
FROM stl_scan s				 	  -- scan log: one row per table scan (i.e., usage/access event)
JOIN pg_class c ON s.tbl = c.oid  -- table metadata, contains table names
WHERE s.starttime > current_date - interval '30 days'  -- Only include scans from the last 30 days
GROUP BY c.relname
ORDER BY scan_count DESC;         -- Show most frequently scanned tables first

-- Does the same counting, but adds the schema name as well
SELECT
    n.nspname AS schema_name,      -- n = pg_namespace: nspname is the schema name
    c.relname AS table_name,       -- c = pg_class: relname is the table name
    COUNT(*) AS scan_count         -- Number of times the table was scanned
FROM stl_scan s                    -- s = scan log: records table scan activity
JOIN pg_class c                    -- c = table metadata (name, ID, schema link)
    ON s.tbl = c.oid               -- Match scan record to table by object ID
JOIN pg_namespace n                -- n = schema metadata
    ON c.relnamespace = n.oid      -- Match table to its schema via namespace ID
WHERE s.starttime > current_date - interval '30 days'          -- Limit to recent scans (last 30 days)
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system/internal schemas
GROUP BY n.nspname, c.relname      -- Group by both schema and table
ORDER BY scan_count DESC;          -- Show the most frequently accessed tables first

-- For each table, show how many times it was scanned and when it was last scanned (past 30 days)
SELECT
	n.nspname AS schema_name,          -- n = pg_namespace: schema name
    c.relname AS table_name,           -- c = pg_class: table name
    COUNT(*) AS scan_count,            -- Number of times the table was scanned
    MAX(s.starttime) AS last_scan_time -- The most recent scan timestamp (last time table was queried)
FROM stl_scan s                        -- s = scan log: logs every table scan
JOIN pg_class c                        -- c = table metadata (name, OID, etc.)
    ON s.tbl = c.oid                   -- Join on object ID to get the table name
JOIN pg_namespace n                    -- n = schema metadata
    ON c.relnamespace = n.oid          -- Join to get the schema name
WHERE s.starttime > current_date - interval '30 days'          -- Filter: scans from the last 30 days only
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system schemas
GROUP BY n.nspname, c.relname		   -- Group by schema-- and table name                          
ORDER BY last_scan_time DESC;          -- Sort: most recently scanned tables first


-- Now, show who scanned which tables, how often, and when was the last time
SELECT
    n.nspname AS schema_name,           -- Schema name
    c.relname AS table_name,            -- Table name
    u.usename AS queried_by,            -- Username (who ran the query)
    COUNT(*) AS scan_count,             -- Number of times the table was scanned by that user
    MAX(q.starttime) AS last_query_time -- Most recent scan time for that table by that user
FROM stl_scan s                         -- Table scan logs
JOIN stl_query q    ON s.query = q.query   	   -- SQL query metadata
JOIN pg_class c     ON s.tbl = c.oid		   -- Table metadata
JOIN pg_namespace n ON c.relnamespace = n.oid  -- Schema metadata
JOIN pg_user u      ON q.userid = u.usesysid   -- User metadata
WHERE q.starttime > current_date - interval '30 days'           -- Limit to recent queries
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')   -- Exclude system schemas
GROUP BY n.nspname, c.relname, u.usename
ORDER BY last_query_time DESC;


-- (The query below will show tables that have NO SCAN record in the last 30 days)
-- Find all user tables that had NO scan activity in the last 30 days
-- Find all user tables that had no scans in the last 30 days
WITH scanned_tables AS (
    SELECT DISTINCT s.tbl
    FROM stl_scan s
    WHERE s.starttime > current_date - interval '30 days'
),

all_user_tables AS (
    SELECT 
        c.oid AS table_oid,
        n.nspname AS schema_name,
        c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.relkind = 'r'  -- 'r' = ordinary table
)

SELECT 
    t.schema_name,
    t.table_name
FROM all_user_tables t
LEFT JOIN scanned_tables s ON t.table_oid = s.tbl
WHERE s.tbl IS NULL  -- Filter to only tables that were not scanned
ORDER BY t.schema_name, t.table_name;

-- Final Combined Query for Google Sheets Output (Tracking Used/Scanned and Unused/Not-Scanned Tables)
-- CTE 1: Get scanned tables with usage metadata
WITH scanned_tables AS (
    SELECT
        s.tbl,
        MAX(q.starttime) AS last_query_time,
        COUNT(*) AS scan_count,
        MAX(u.usename) AS queried_by  -- Optional: most recent user (simplified)
    FROM stl_scan s
    JOIN stl_query q ON s.query = q.query
    JOIN pg_user u ON q.userid = u.usesysid
    WHERE q.starttime > current_date - interval '30 days'
    GROUP BY s.tbl
),

-- CTE 2: Get all user tables (base tables only)
all_user_tables AS (
    SELECT 
        c.oid AS table_oid,
        n.nspname AS schema_name,
        c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.relkind = 'r'  -- 'r' = base table
)

-- Final select: left join all tables with scanned tables
SELECT 
    t.schema_name,
    t.table_name,
    CASE 
        WHEN s.tbl IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_used,
    s.queried_by,
    s.last_query_time,
    s.scan_count
FROM all_user_tables t
LEFT JOIN scanned_tables s ON t.table_oid = s.tbl
ORDER BY is_used DESC, t.schema_name, t.table_name;
