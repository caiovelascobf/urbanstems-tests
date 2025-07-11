-- Create the 'archive_schema' schema
CREATE SCHEMA IF NOT EXISTS analytics.archive_schemas;

-- Check if the schema was created
SELECT *
FROM information_schema.schemata
WHERE schema_name = 'archive_schemas';

-- Manually add the archive_candidates.csv to the S3 bucket (s3://caio-archive-redshift-objects/archive_candidates.csv)

-- Create a archive_candidates table in the archive schema and populate with the redshift objects (based tables and views) that are candidates for deprecation
CREATE TABLE archive_schemas.archive_candidates (
  schema_name VARCHAR,
  table_name VARCHAR,
  object_type VARCHAR,      -- 'BASE TABLE' or 'VIEW'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Check if 'archive_candidates' was created
SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive_schemas' AND table_name = 'archive_candidates';

COPY archive_schemas.archive_candidates(schema_name, table_name, object_type)
FROM 's3://caio-archive-redshift-objects/archive_candidates.csv'
IAM_ROLE 'arn:aws:iam::045322402851:role/redshift_s3_access'
FORMAT AS CSV
IGNOREHEADER 1;

-- Verify if the archive_candidates was created
SELECT COUNT(*) 
FROM archive_schemas.archive_candidates;

-- Create the 'archive_tables' schema
CREATE SCHEMA IF NOT EXISTS analytics.archive_tables;

-- Check if the schema was created
SELECT *
FROM information_schema.schemata
WHERE schema_name = 'archive_tables';

-- Check the tables within the archive_tables schema
SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive_tables';

-- Create table to map original and truncated/suffix tables during the moving stored procedure
CREATE TABLE IF NOT EXISTS archive_tables.archive_table_mapping (
    original_schema_name TEXT,
    original_table_name TEXT,
    archive_table_name TEXT,
    truncated BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Check the archive.archive_table_mapping
SELECT * FROM archive_tables.archive_table_mapping;

-- Store Procedure: Redshift Dynamic SQL to Copy Candidate Tables into archive
CALL archive_tables.copy_from_archive_candidates()

-- Check all tables mapping (after the procedure was called)
SELECT * FROM archive_tables.archive_table_mapping;

-- The Stored Procedure
CREATE OR REPLACE PROCEDURE archive_tables.copy_from_archive_candidates()
AS $$
DECLARE
    r RECORD;
    archive_table_name TEXT;
    base_table_name TEXT;
    source_table TEXT;
    stmt VARCHAR(10000);
    exists_count INT;
    suffix INT;
    was_truncated BOOLEAN;
    row_counter INT := 0;
    max_limit INT := 251;  -- Adjust this value for batch size
BEGIN
    FOR r IN
        SELECT schema_name, table_name
        FROM archive_schemas.archive_candidates
        WHERE object_type = 'BASE TABLE'
    LOOP
        base_table_name := r.table_name;

        -- 🔒 Skip known broken tables manually
        IF (r.schema_name = 'klaviyo' AND r.table_name IN (
                'bounce__person__which of the below bouquet styles appeals most to you?',
                'bounce__person__which of the following influences your purchasing decision?',
                'bounce__person__which of these do you like?',
                'bounce__person__which type of flower do you like?',
                'dropped_email__person__which of the below bouquet styles appeals most to you?',
                'dropped_email__person__which of the following influences your purchasing decision?',
                'dropped_email__person__which of these do you like?',
                'dropped_email__person__which type of flower do you like?',
                'mark_as_spam__person__which of the below bouquet styles appeals most to you?',
                'mark_as_spam__person__which of the following influences your purchasing decision?',
                'mark_as_spam__person__which of these do you like?',
                'mark_as_spam__person__which type of flower do you like?',
                'unsub_list__person__which of the below bouquet styles appeals most to you?',
                'unsub_list__person__which of the following influences your purchasing decision?',
                'unsub_list__person__which type of flower do you like?',
                'update_email_preferences__person__which of the below bouquet styles appeals most to you?',
                'update_email_preferences__person__which of the following influences your purchasing decision?',
                'update_email_preferences__person__which of these do you like?',
                'update_email_preferences__person__which type of flower do you like?'
            )) THEN
            RAISE NOTICE '⚠️ Skipping hardcoded broken table %.%', r.schema_name, r.table_name;
            CONTINUE;
        END IF;

        -- Skip if already archived
        IF EXISTS (
            SELECT 1
            FROM archive_tables.archive_table_mapping
            WHERE original_table_name = base_table_name
              AND original_schema_name = r.schema_name
        ) THEN
            CONTINUE;
        END IF;

        row_counter := row_counter + 1;
        IF row_counter > max_limit THEN
            RAISE NOTICE '⏸️ Limit of % reached, stopping batch.', max_limit;
            RETURN;
        END IF;

        archive_table_name := base_table_name;
        was_truncated := FALSE;

        -- Truncate if name exceeds max length
        IF LENGTH(archive_table_name) > 127 THEN
            archive_table_name := SUBSTRING(archive_table_name FROM 1 FOR 127);
            was_truncated := TRUE;
        END IF;

        -- Ensure archive_table_name is unique
        suffix := 1;
        LOOP
            SELECT COUNT(*) INTO exists_count
            FROM information_schema.tables
            WHERE table_schema = 'archive_tables'
              AND table_name = archive_table_name;
            EXIT WHEN exists_count = 0;

            archive_table_name := LEFT(base_table_name, 120) || '_' || suffix;

            IF LENGTH(archive_table_name) > 127 THEN
                archive_table_name := SUBSTRING(archive_table_name FROM 1 FOR 127);
            END IF;

            was_truncated := TRUE;
            suffix := suffix + 1;
        END LOOP;

        -- Safely quote schema and table
        source_table := quote_ident(r.schema_name) || '.' || quote_ident(base_table_name);

        -- Attempt to copy the table
        stmt := 'CREATE TABLE archive_tables.' || quote_ident(archive_table_name) ||
                ' AS SELECT * FROM ' || source_table || ';';

        BEGIN
            EXECUTE stmt;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '❌ Failed to copy %.%: %', r.schema_name, base_table_name, SQLERRM;
                CONTINUE;
        END;

        -- Insert into archive_table_mapping
        stmt := 'INSERT INTO archive_tables.archive_table_mapping (' ||
                'original_schema_name, original_table_name, archive_table_name, truncated' ||
                ') VALUES (' ||
                quote_literal(r.schema_name) || ', ' ||
                quote_literal(base_table_name) || ', ' ||
                quote_literal(archive_table_name) || ', ' ||
                CASE WHEN was_truncated THEN 'true' ELSE 'false' END || ');';
        EXECUTE stmt;
    END LOOP;

    RAISE NOTICE '🏁 Done. % tables processed (new archives only).', row_counter;
END;
$$ LANGUAGE plpgsql;

-- Create table to  match counts after archiving
-- DROP TABLE archive_schemas.archive_table_count_validation
CREATE TABLE IF NOT EXISTS archive_schemas.archive_table_count_validation (
    original_schema_name TEXT,
    original_table_name TEXT,
    archive_table_name TEXT,
    truncated BOOLEAN,
    original_count BIGINT,
    archive_count BIGINT,
    counts_match BOOLEAN,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Stored Procedure to Match Counts after Archiving
CREATE OR REPLACE PROCEDURE archive_schemas.validate_archive_counts()
AS $$
DECLARE
    r RECORD;
    original_count BIGINT;
    archive_count BIGINT;
    counts_match INT;
    stmt VARCHAR(10000);  -- 👈 fixed to avoid Redshift compile error
BEGIN
    FOR r IN
        SELECT original_schema_name, original_table_name, archive_table_name, truncated
        FROM archive_tables.archive_table_mapping
    LOOP
        original_count := NULL;
        archive_count := NULL;

        -- Count original table
        BEGIN
            EXECUTE 'SELECT COUNT(*) FROM ' || quote_ident(r.original_schema_name) || '.' || quote_ident(r.original_table_name)
            INTO original_count;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '⚠️ Failed to count %.% — %', r.original_schema_name, r.original_table_name, SQLERRM;
        END;

        -- Count archive table
        BEGIN
            EXECUTE 'SELECT COUNT(*) FROM archive_tables.' || quote_ident(r.archive_table_name)
            INTO archive_count;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '⚠️ Failed to count archive copy % — %', r.archive_table_name, SQLERRM;
        END;

        -- Evaluate match
        IF original_count IS NOT NULL AND archive_count IS NOT NULL AND original_count = archive_count THEN
            counts_match := 1;
        ELSE
            counts_match := 0;
        END IF;

        -- Insert results
        stmt := 'INSERT INTO archive_schemas.archive_table_count_validation (' ||
                'original_schema_name, original_table_name, archive_table_name, truncated, ' ||
                'original_count, archive_count, counts_match' ||
                ') VALUES (' ||
                quote_literal(r.original_schema_name) || ', ' ||
                quote_literal(r.original_table_name) || ', ' ||
                quote_literal(r.archive_table_name) || ', ' ||
                CASE WHEN r.truncated THEN 'true' ELSE 'false' END || ', ' ||
                COALESCE(original_count::TEXT, 'NULL') || ', ' ||
                COALESCE(archive_count::TEXT, 'NULL') || ', ' ||
                counts_match || ');';

        EXECUTE stmt;
    END LOOP;

    RAISE NOTICE '✅ Archive validation complete.';
END;
$$ LANGUAGE plpgsql;

-- Old code, from when I tried copy all schemas with their tables in the archive_schema
-- Create first table with unique schemas
-- CREATE TABLE IF NOT EXISTS archive_schemas.unique_schemas (
--     schema_name VARCHAR
-- );

-- Copy script_01_unique_schemas.csv to a table in archive
-- COPY archive_schemas.unique_schemas(schema_name)
-- FROM 's3://caio-archive-redshift-objects/script_01_unique_schemas.csv'
-- IAM_ROLE 'arn:aws:iam::045322402851:role/redshift_s3_access'
-- FORMAT AS CSV
-- IGNOREHEADER 1;

-- Check the archive.unique_schemas
-- SELECT * FROM archive_schemas.unique_schemas;

-- Create table to map original to truncated tables after moving tables
-- CREATE TABLE IF NOT EXISTS archive_schemas.archive_schema_mapping (
--     original_schema_name TEXT,
--     archive_schema_name TEXT,
--     truncated BOOLEAN DEFAULT FALSE,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Check the archive.archive_name_mapping
-- SELECT * FROM archive_schemas.archive_schema_mapping;

-- Run Stored Procedure to Load CSVs from S3 bucket
-- CALL archive_schemas.create_table_and_load_csv_to_archive_schema(); 

-- Check one of the tables
-- SELECT * FROM archive_schemas.adwords;