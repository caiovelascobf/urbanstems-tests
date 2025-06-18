-- Redshift Tables Audit

-- Step 1: Check if any VIEW in the entire Redshift database references some Test tables:
-- Test tables:
-- accounts        (Is Used in Redshift = Y, Is Used by DBT = N)
-- ad_groups       (Is Used in Redshift = Y, Is Used by DBT = Y)
-- ad_performance  (Is Used in Redshift = N, Is Used by DBT = N)

SELECT 
	   c.table_name AS deprecation_candidate,
	   v.schemaname AS view_schema,
       v.viewname AS view_name,
       v.definition AS view_sql
FROM pg_views v
JOIN (
    SELECT 'accounts' AS table_name
) c ON POSITION(LOWER(c.table_name) IN LOWER(v.definition)) > 0;
