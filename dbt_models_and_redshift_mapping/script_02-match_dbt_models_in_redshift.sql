-- Create table: test_dbt_models_match_in_redshift
-- This script preserves all columns from test_dbt_models_full_path as the anchor,
-- and appends a match_flag indicating existence in svv_tables.

DROP TABLE IF EXISTS dbt_brainforge.test_dbt_models_match_in_redshift;

CREATE TABLE dbt_brainforge.test_dbt_models_match_in_redshift AS
WITH existing_objects AS (
    SELECT 
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name AS dbt_model_name
    FROM svv_tables
)
SELECT
    f.*,
    CASE
        WHEN e.dbt_model_name IS NOT NULL THEN 1
        ELSE 0
    END AS match_flag
FROM dbt_brainforge.test_dbt_models_full_path f
LEFT JOIN existing_objects e
  ON f.database_name = e.database_name
 AND f.schema_name = e.schema_name
 AND f.dbt_model_name = e.dbt_model_name;
