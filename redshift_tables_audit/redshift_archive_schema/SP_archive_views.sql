-- CALL analytics.archive_views.copy_one_view();
-- SHOW VIEW analytics.adwords_accounts;

CREATE OR REPLACE PROCEDURE analytics.archive_views.copy_one_view()
AS $$
DECLARE
    r RECORD;
    raw_create TEXT;
    cleaned_def TEXT;
    stmt TEXT;
    preview_stmt TEXT;
BEGIN
    -- Get one unarchived view
    FOR r IN
        SELECT c.schema_name, c.table_name
        FROM archive_schemas.archive_candidates c
        WHERE object_type = 'VIEW'
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.archive_views.archive_view_mapping m
              WHERE m.original_schema_name = c.schema_name
                AND m.original_view_name = c.table_name
          )
        LIMIT 1
    LOOP
        RAISE NOTICE '🔍 Archiving %.%', r.schema_name, r.table_name;

        -- ✅ Redshift-compatible dynamic SHOW VIEW
        EXECUTE 'SHOW VIEW ' || quote_ident(r.schema_name) || '.' || quote_ident(r.table_name) INTO raw_create;

        IF raw_create IS NULL THEN
            RAISE NOTICE '⚠️ No definition found for %.%. Skipping.', r.schema_name, r.table_name;
            RETURN;
        END IF;

        -- Clean CREATE VIEW ... AS (...) syntax
        cleaned_def := REGEXP_REPLACE(raw_create, '(?is)^.*?AS\s*\(', '', 'g');
        cleaned_def := REGEXP_REPLACE(cleaned_def, '\)\s*;?\s*$', '', 'g');

        stmt := 'CREATE VIEW analytics.archive_views.' || quote_ident(r.table_name) ||
                ' AS (' || cleaned_def || ')';

        preview_stmt := LEFT(stmt, 200) || '...';
        RAISE NOTICE '🛠️ Executing preview: %', preview_stmt;

        BEGIN
            EXECUTE stmt;
            RAISE NOTICE '✅ Archived view %.%', r.table_name;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '❌ Failed to create view %: %', r.table_name, SQLERRM;
                RETURN;
        END;

        INSERT INTO analytics.archive_views.archive_view_mapping (
            original_schema_name,
            original_view_name,
            archive_view_name,
            truncated
        ) VALUES (
            r.schema_name,
            r.table_name,
            r.table_name,
            FALSE
        );

        RAISE NOTICE '📝 Mapping logged for %.%', r.table_name;
        RETURN;
    END LOOP;

    RAISE NOTICE '📭 No unarchived views left.';
END;
$$ LANGUAGE plpgsql;
