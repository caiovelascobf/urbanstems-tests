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

SELECT COUNT(*) FROM (SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive_tables')

-- The Stored Procedure to copy tables from the archive_candidates table
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
    max_limit INT := 1;  -- Adjust this value for batch size
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

        -- 🔒 Skip internal system schema
        IF r.schema_name = 'pg_automv' THEN
            RAISE NOTICE '⚠️ Skipping internal auto MV table %.%', r.schema_name, r.table_name;
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

-- Call Archive Count Prodedure
CALL archive_schemas.validate_archive_counts();

-- Check if they matched
SELECT * FROM archive_schemas.archive_table_count_validation WHERE counts_match IS FALSE;

-- Stored Procedure to Validate Archive Counts
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

-- Create a tables to log the dropping procedure
CREATE TABLE IF NOT EXISTS archive_schemas.original_table_drop_log (
    original_schema_name TEXT,
    original_table_name TEXT,
    drop_succeeded BOOLEAN,
    error_message TEXT,
    dropped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Call Log Procedure that Drops archived tables from their origin
CALL archive_schemas.drop_original_tables();

-- Check the log
SELECT COUNT(*) FROM archive_schemas.original_table_drop_log;
SELECT * FROM archive_schemas.original_table_drop_log WHERE original_schema_name LIKE '%zendesk%';

-- STORE PROCEDURE TO DROP ALL TABLES
CREATE OR REPLACE PROCEDURE archive_schemas.drop_original_tables()
AS $$
DECLARE
    r RECORD;
    stmt VARCHAR(10000);
    row_counter INT := 0;
    max_limit INT := 50;  -- Adjust for batch size
    drop_success BOOLEAN;
    err_msg TEXT;
BEGIN
    FOR r IN
        SELECT m.original_schema_name,
               m.original_table_name
        FROM archive_tables.archive_table_mapping m
        LEFT JOIN archive_schemas.original_table_drop_log l
               ON m.original_schema_name = l.original_schema_name
              AND m.original_table_name = l.original_table_name
              AND l.drop_succeeded = TRUE
        WHERE l.original_table_name IS NULL
    LOOP
        row_counter := row_counter + 1;

        IF row_counter > max_limit THEN
            RAISE NOTICE '⏸️ Limit of % reached, stopping batch.', max_limit;
            RETURN;
        END IF;

        drop_success := FALSE;
        err_msg := NULL;

        -- Attempt to drop the original table with CASCADE
        BEGIN
            stmt := 'DROP TABLE ' || quote_ident(r.original_schema_name) || '.' || quote_ident(r.original_table_name) || ' CASCADE;';
            EXECUTE stmt;
            drop_success := TRUE;
            -- Notice suppressed for successful drop
        EXCEPTION
            WHEN OTHERS THEN
                drop_success := FALSE;
                err_msg := SQLERRM;
                RAISE NOTICE '❌ Failed to drop %.%: %', r.original_schema_name, r.original_table_name, err_msg;
        END;

        -- Log result
        stmt := 'INSERT INTO archive_schemas.original_table_drop_log (' ||
                'original_schema_name, original_table_name, drop_succeeded, error_message' ||
                ') VALUES (' ||
                quote_literal(r.original_schema_name) || ', ' ||
                quote_literal(r.original_table_name) || ', ' ||
                CASE WHEN drop_success THEN 'true' ELSE 'false' END || ', ' ||
                CASE WHEN err_msg IS NOT NULL THEN quote_literal(err_msg) ELSE 'NULL' END ||
                ');';
        EXECUTE stmt;
    END LOOP;

    RAISE NOTICE '🏁 Drop procedure complete. % attempted.', row_counter;
END;
$$ LANGUAGE plpgsql;


---------- NOW, LET'S DO ARCHIVE VIEWS --------------

-- Create the 'archive_tables' schema
CREATE SCHEMA IF NOT EXISTS analytics.archive_views;

-- DROP TABLE archive_views.archive_view_mapping;

CREATE TABLE analytics.archive_views.archive_view_mapping (
    original_schema_name VARCHAR(65535),  -- Source schema of the original view (e.g., dbt_egiant)
    original_view_name   VARCHAR(65535),  -- Original view name to be archived
    archive_view_name    VARCHAR(65535),  -- Final archived view name (may be truncated or suffixed)
    truncated            BOOLEAN DEFAULT FALSE,  -- Whether the archived name was truncated to meet length limits
    is_view_broken       BOOLEAN DEFAULT FALSE,  -- True if SELECT * FROM original view failed (view is broken at source)
    create_error         BOOLEAN DEFAULT FALSE,  -- True if CREATE VIEW failed during recreation (e.g., invalid DDL)
    broken_reason        VARCHAR(65535),  -- Text of the exception or reason for failure
    view_definition      VARCHAR(65535),  -- Raw DDL as captured by SHOW VIEW
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp of processing
);

SELECT * FROM archive_views.archive_view_mapping;

-- THE PYTHON SCRIPT THAT ARCHIVE VIEWS GOES BELOW, PLEASE UNCOMMENT IT AND RUN LOCALLY. 
-- IT WILL CONNECT TO REDSHIFT IF YOU CREATE A .ENV FILE WITH YOUR CONNECTION DETAILS
--import os
--import re
--import psycopg2
--from psycopg2 import sql
--from dotenv import load_dotenv
--
--# Load environment variables
--load_dotenv()
--
--# Config
--MAX_LIMIT = 50
--ARCHIVE_SCHEMA = "archive_views"
--MAPPING_TABLE = "analytics.archive_views.archive_view_mapping"
--MAX_VIEW_NAME_LENGTH = 127
--
--# Connect to Redshift
--conn = psycopg2.connect(
--    host=os.getenv("REDSHIFT_HOST"),
--    port=os.getenv("REDSHIFT_PORT", "5439"),
--    dbname=os.getenv("REDSHIFT_DBNAME"),
--    user=os.getenv("REDSHIFT_USER_NAME"),
--    password=os.getenv("REDSHIFT_PASSWORD")
--)
--conn.autocommit = True
--cur = conn.cursor()
--
--archived_count = 0
--skipped_broken = 0
--
--# Fetch batch
--cur.execute(sql.SQL(f"""
--    SELECT schema_name, table_name
--    FROM archive_schemas.archive_candidates c
--    WHERE object_type = 'VIEW'
--    AND NOT EXISTS (
--        SELECT 1
--        FROM {MAPPING_TABLE} m
--        WHERE m.original_schema_name = c.schema_name
--          AND m.original_view_name = c.table_name
--    )
--    LIMIT %s
--"""), (MAX_LIMIT,))
--views = cur.fetchall()
--
--for schema, view in views:
--    print(f"\n🔍 Archiving view: {schema}.{view}")
--    archive_view = view
--    was_truncated = False
--    ddl = None
--    broken_reason = None
--    is_broken = False
--
--    # Truncate if needed
--    if len(archive_view) > MAX_VIEW_NAME_LENGTH:
--        archive_view = archive_view[:MAX_VIEW_NAME_LENGTH]
--        was_truncated = True
--
--    # Ensure uniqueness
--    suffix = 1
--    base_name = archive_view
--    while True:
--        cur.execute(sql.SQL("""
--            SELECT 1
--            FROM information_schema.views
--            WHERE table_schema = %s AND table_name = %s
--        """), (ARCHIVE_SCHEMA, archive_view))
--        if not cur.fetchone():
--            break
--        suffix_str = f"_{suffix}"
--        archive_view = base_name[:MAX_VIEW_NAME_LENGTH - len(suffix_str)] + suffix_str
--        was_truncated = True
--        suffix += 1
--
--    # Try to get the DDL (SHOW VIEW)
--    try:
--        cur.execute(sql.SQL("SHOW VIEW {}.{}").format(
--            sql.Identifier(schema),
--            sql.Identifier(view)
--        ))
--        ddl = cur.fetchone()[0]
--    except Exception as e:
--        broken_reason = f"SHOW VIEW error: {str(e).strip()}"
--        print(f"❌ Failed to get DDL for {schema}.{view}: {broken_reason}")
--        cur.execute(sql.SQL(f"""
--            INSERT INTO {MAPPING_TABLE} (
--                original_schema_name,
--                original_view_name,
--                archive_view_name,
--                truncated,
--                is_view_broken,
--                broken_reason,
--                view_definition
--            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
--        """), (schema, view, archive_view, was_truncated, True, broken_reason, None))
--        skipped_broken += 1
--        continue
--
--    # Validate SELECT * FROM view
--    try:
--        cur.execute(sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
--            sql.Identifier(schema),
--            sql.Identifier(view)
--        ))
--    except Exception as e:
--        is_broken = True
--        broken_reason = str(e).strip()
--        print(f"⚠️ View {schema}.{view} is broken and will be skipped: {broken_reason}")
--
--    # If broken, log and skip CREATE VIEW
--    if is_broken:
--        cur.execute(sql.SQL(f"""
--            INSERT INTO {MAPPING_TABLE} (
--                original_schema_name,
--                original_view_name,
--                archive_view_name,
--                truncated,
--                is_view_broken,
--                broken_reason,
--                view_definition
--            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
--        """), (schema, view, archive_view, was_truncated, True, broken_reason, ddl))
--        skipped_broken += 1
--        continue
--
--    # Clean DDL for CREATE VIEW
--    cleaned = ddl.strip()
--    cleaned = re.sub(r'(?is)^.*?AS\s*\(', '', cleaned)
--    cleaned = re.sub(r'\)\s+with no schema binding\s*;?\s*$', '', cleaned, flags=re.IGNORECASE)
--    cleaned = re.sub(r'\)\s*$', '', cleaned.strip())
--
--    # CREATE the archived view
--    create_sql = sql.SQL("CREATE VIEW {}.{} AS ({})").format(
--        sql.Identifier(ARCHIVE_SCHEMA),
--        sql.Identifier(archive_view),
--        sql.SQL(cleaned)
--    )
--
--    try:
--        cur.execute(create_sql)
--    except Exception as e:
--        broken_reason = f"CREATE VIEW error: {str(e).strip()}"
--        print(f"❌ Failed to create archived view {ARCHIVE_SCHEMA}.{archive_view}: {broken_reason}")
--        cur.execute(sql.SQL(f"""
--            INSERT INTO {MAPPING_TABLE} (
--                original_schema_name,
--                original_view_name,
--                archive_view_name,
--                truncated,
--                is_view_broken,
--                broken_reason,
--                view_definition
--            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
--        """), (schema, view, archive_view, was_truncated, True, broken_reason, ddl))
--        skipped_broken += 1
--        continue
--
--    # Log success
--    cur.execute(sql.SQL(f"""
--        INSERT INTO {MAPPING_TABLE} (
--            original_schema_name,
--            original_view_name,
--            archive_view_name,
--            truncated,
--            is_view_broken,
--            broken_reason,
--            view_definition
--        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
--    """), (schema, view, archive_view, was_truncated, False, None, ddl))
--
--    print(f"✅ Archived {schema}.{view} → {ARCHIVE_SCHEMA}.{archive_view}")
--    archived_count += 1
--
--# Done
--cur.close()
--conn.close()
--
--print(f"\n🏁 Done. {archived_count} views archived, {skipped_broken} broken views logged.")