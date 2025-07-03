# Redshift Table Archiving: Workflow Summary

## 🗂️ Goal

Deprecate and archive selected Redshift base tables by copying them from their original schemas into a single `archive` schema in the same database (`analytics`). Preserve data (not just metadata) and enable safe, resumable execution.

---

## ✅ Step 1: Candidate List Preparation

- A CSV named `archive_candidates.csv` was created with this format:

```csv
schema_name,table_name,object_type
sales,orders,BASE TABLE
support,orders,BASE TABLE
hr,employees,BASE TABLE
...
```

- The CSV was uploaded to an S3 bucket:
  ```
  s3://caio-archive-redshift-objects/archive_candidates.csv
  ```

---

## ✅ Step 2: Load Candidates into Redshift

- A table was created to store the candidate list:

```sql
CREATE TABLE archive.archive_candidates (
  schema_name VARCHAR,
  table_name VARCHAR,
  object_type VARCHAR,
  created_at TIMESTAMP DEFAULT current_timestamp
);
```

- Data was loaded from S3:

```sql
COPY archive.archive_candidates(schema_name, table_name, object_type)
FROM 's3://caio-archive-redshift-objects/archive_candidates.csv'
IAM_ROLE 'arn:aws:iam::045322402851:role/redshift_s3_access'
FORMAT AS CSV
IGNOREHEADER 1;
```

---

## ✅ Step 3: Create Archive Schema

```sql
CREATE SCHEMA IF NOT EXISTS archive;
```

---

## ✅ Step 4: Initial Test Procedure

- A stored procedure was created to copy candidate tables from source schemas into the `archive` schema:

```sql
CREATE OR REPLACE PROCEDURE archive.copy_base_tables_from_candidates()
AS $$
DECLARE
    r RECORD;
    stmt TEXT;
    exists_count INT;
BEGIN
    FOR r IN
        SELECT schema_name, table_name
        FROM archive.archive_candidates
        WHERE object_type = 'BASE TABLE'
    LOOP
        SELECT COUNT(*) INTO exists_count
        FROM information_schema.tables
        WHERE table_schema = 'archive'
          AND table_name = r.table_name;

        IF exists_count = 0 THEN
            stmt := 'CREATE TABLE archive.' || r.table_name ||
                    ' AS SELECT * FROM ' || r.schema_name || '.' || r.table_name || ';';

            BEGIN
                EXECUTE stmt;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'Failed to copy: %', stmt;
            END;
        ELSE
            RAISE NOTICE 'Skipping table already archived: %', r.table_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

- This was tested successfully with small batches using `LIMIT`, but was later modified for full-run processing.

---

## ⚠️ Issue Identified

- Multiple source schemas had tables with the same name (e.g., `sales.orders`, `support.orders`).
- Without name mangling, only one version could exist in `archive`, causing errors.

---

## ✅ Step 5: Updated Procedure with Schema Prefixing

To avoid table name collisions, the procedure was updated to include the source schema in the archive table name:

- Example:  
  `sales.orders` → `archive.sales__orders`  
  `support.orders` → `archive.support__orders`

```sql
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
        archive_table := r.schema_name || '__' || r.table_name;

        SELECT COUNT(*) INTO exists_count
        FROM information_schema.tables
        WHERE table_schema = 'archive'
          AND table_name = archive_table;

        IF exists_count = 0 THEN
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
```

- The archive table names are now globally unique, traceable to their source schema, and safely handled.

---

## 🔍 Monitoring Progress

While the procedure runs, progress is checked by:

```sql
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'archive';
```

Or:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'archive'
ORDER BY table_name;
```

---

## 🔄 What’s Next

- Continue running the procedure to archive all `BASE TABLE` entries
- Optionally:
  - Add a `processed` flag to `archive_candidates`
  - Add logging table (`archive.archive_log`) to record success/failure
  - Add logic to handle `VIEW` objects similarly

---

## ✅ Current Status

- Archive schema and candidates pipeline set up
- Tables are safely archived with schema-prefixed names
- Procedure is working and ready to process all candidates in a single run
