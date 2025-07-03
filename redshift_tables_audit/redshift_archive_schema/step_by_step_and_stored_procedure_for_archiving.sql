-- Create the 'archive' schema
CREATE SCHEMA IF NOT EXISTS analytics.archive;

-- Check if the schema was created
SELECT *
FROM information_schema.schemata
WHERE schema_name = 'archive';

-- Create a temp table in the archive schema and populate with the redshift objects (based tables and views) that are candidates for deprecation
CREATE TABLE archive.archive_candidates (
  schema_name VARCHAR,
  table_name VARCHAR,
  object_type VARCHAR,      -- 'table' or 'view'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Check if 'archive_candidates' was created
SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive' AND table_name = 'archive_candidates';

-- Load CSV from S3 bucket
COPY archive.archive_candidates(schema_name, table_name, object_type)
FROM 's3://caio-archive-redshift-objects/archive_candidates.csv'
IAM_ROLE 'arn:aws:iam::045322402851:role/redshift_s3_access'
FORMAT AS CSV
IGNOREHEADER 1;

-- If you need to erase the rows in the archive.archive_candidates 
-- TRUNCATE TABLE archive.archive_candidates;

-- Verify if the archive_candidates was created
SELECT * 
FROM archive.archive_candidates 
LIMIT 10;

-- Test to see if I can write into 'archive' schema
CREATE TABLE archive.test_table_copy AS
SELECT * FROM adwords.click_performance_report;

SELECT * from archive.test_table_copy limit 5;


-- Store Procedure: Redshift Dynamic SQL to Copy Candidate Tables into archive
-- DROP PROCEDURE IF EXISTS archive.copy_base_tables_from_candidates();

CREATE OR REPLACE PROCEDURE archive.copy_base_tables_from_candidates()
AS $$
DECLARE
    r RECORD;
    stmt TEXT;
    archive_table TEXT;
    exists_count INT;
BEGIN
    FOR r IN
        SELECT schema_name, table_name
        FROM archive.archive_candidates
        WHERE object_type = 'BASE TABLE'
    LOOP
        -- Construct the archive table name with schema prefix
        archive_table := r.schema_name || '__' || r.table_name;

        -- Check if archive table already exists
        SELECT COUNT(*) INTO exists_count
        FROM information_schema.tables
        WHERE table_schema = 'archive'
          AND table_name = archive_table;

        IF exists_count = 0 THEN
            -- Build and execute the CREATE TABLE statement
            stmt := 'CREATE TABLE archive."' || archive_table || '" AS SELECT * FROM "' ||
                    r.schema_name || '"."' || r.table_name || '";';

            BEGIN
                EXECUTE stmt;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'Failed to copy: %', stmt;
            END;
        ELSE
            RAISE NOTICE 'Skipping table already archived: %', archive_table;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Call Store Procedure
CALL archive.copy_base_tables_from_candidates();

-- Checking
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'archive';


-- iF you need to drop tables
-- DROP TABLE "archive"."1p fulfillment cost";
-- DROP TABLE "archive"."2022ye_netsuite_report";
-- DROP TABLE "archive"."3p cost";
-- DROP TABLE "archive"."_sdc_rejected";


-- Check Archived Table Data
-- Original
SELECT COUNT(*) AS original_count FROM fulfillment_costs."3p cost";
-- Archived
SELECT COUNT(*) AS archive_count FROM "archive"."fulfillment_costs__3p cost";

SELECT * from "archive"."fulfillment_costs__3p cost";