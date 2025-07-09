# 📦 Redshift Archiving System — Phase 1: BASE TABLES Only

This document sets up and explains a robust, idempotent Redshift archival system. It is designed to copy **only `BASE TABLES`** (not views or other object types) from source schemas to an `archive_tables` schema.

> ⚠️ **IMPORTANT:**  
> - This procedure currently **only handles `BASE TABLES`**.  
> - **Views are excluded** by design (`object_type = 'BASE TABLE'` filter).  
> - **Phase 2** will extend this logic to support archiving `VIEWS` separately.

---

## 🧠 Overview of the Workflow

1. A CSV file lists candidate tables for deprecation  
2. Redshift loads the list into `archive_schemas.archive_candidates`  
3. A stored procedure loops through the list:
   - ✅ Copies `BASE TABLES` into `archive_tables`
   - ✅ Skips any already archived
   - ✅ Truncates and suffixes names to meet Redshift constraints
   - ✅ Logs everything in a mapping table

---

## 📁 Step 1: Create the Input Schema

```sql
CREATE SCHEMA IF NOT EXISTS analytics.archive_schemas;
```

✅ Confirm creation:

```sql
SELECT *
FROM information_schema.schemata
WHERE schema_name = 'archive_schemas';
```

---

## 📄 Step 2: Upload Archive Candidates File

Manually upload this CSV to S3:

```
s3://caio-archive-redshift-objects/archive_candidates.csv
```

Each row must define:
- `schema_name`
- `table_name`
- `object_type` — Only `BASE TABLE` will be processed

---

## 📋 Step 3: Create and Load `archive_candidates` Table

```sql
CREATE TABLE archive_schemas.archive_candidates (
  schema_name VARCHAR,
  table_name VARCHAR,
  object_type VARCHAR,      -- 'BASE TABLE' or 'VIEW'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

✅ Verify the table exists:

```sql
SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive_schemas' AND table_name = 'archive_candidates';
```

🚛 Load CSV contents:

```sql
COPY archive_schemas.archive_candidates(schema_name, table_name, object_type)
FROM 's3://caio-archive-redshift-objects/archive_candidates.csv'
IAM_ROLE 'arn:aws:iam::045322402851:role/redshift_s3_access'
FORMAT AS CSV
IGNOREHEADER 1;
```

✅ Confirm load:

```sql
SELECT COUNT(*) 
FROM archive_schemas.archive_candidates;
```

---

## 🗃️ Step 4: Create Archive Output Schema

```sql
CREATE SCHEMA IF NOT EXISTS analytics.archive_tables;
```

✅ Confirm schema exists:

```sql
SELECT *
FROM information_schema.schemata
WHERE schema_name = 'archive_tables';
```

🔎 View current contents:

```sql
SELECT *
FROM information_schema.tables
WHERE table_schema = 'archive_tables';
```

---

## 🔗 Step 5: Create Archive Mapping Table

This table logs original → archived mappings:

```sql
CREATE TABLE IF NOT EXISTS archive_tables.archive_table_mapping (
    original_schema_name TEXT,
    original_table_name TEXT,
    archive_table_name TEXT,
    truncated BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

✅ Check mappings:

```sql
SELECT * FROM archive_tables.archive_table_mapping;
```

---

## ⚙️ Step 6: Create the Archive Procedure

The stored procedure below:
- Iterates over candidate `BASE TABLES`
- Ensures uniqueness in archive schema
- Safely truncates long names (max 127 chars)
- Skips duplicates
- Records each success in a mapping table

```sql
CREATE OR REPLACE PROCEDURE archive_tables.copy_from_archive_candidates()
AS $$
DECLARE
    r RECORD;
    archive_table_name TEXT;
    base_table_name TEXT;
    source_table TEXT;
    stmt VARCHAR(10000);  -- Allow long SQL statements
    exists_count INT;
    suffix INT;
    was_truncated BOOLEAN;
    row_counter INT := 0;
    max_limit INT := 2400;  -- Prevent overloading in one run
BEGIN
    FOR r IN
        SELECT schema_name, table_name
        FROM archive_schemas.archive_candidates
        WHERE object_type = 'BASE TABLE'  -- ✅ Only base tables are processed
    LOOP
        base_table_name := r.table_name;

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

        -- Truncate if too long
        IF LENGTH(archive_table_name) > 127 THEN
            archive_table_name := SUBSTRING(archive_table_name FROM 1 FOR 127);
            was_truncated := TRUE;
        END IF;

        -- Ensure uniqueness
        suffix := 1;
        LOOP
            SELECT COUNT(*) INTO exists_count
            FROM information_schema.tables
            WHERE table_schema = 'archive_tables'
              AND table_name = archive_table_name;
            EXIT WHEN exists_count = 0;

            archive_table_name := LEFT(base_table_name, 120);
            archive_table_name := archive_table_name || '_' || suffix;

            IF LENGTH(archive_table_name) > 127 THEN
                archive_table_name := SUBSTRING(archive_table_name FROM 1 FOR 127);
            END IF;

            was_truncated := TRUE;
            suffix := suffix + 1;
        END LOOP;

        -- Build and execute copy
        source_table := '"' || r.schema_name || '"."' || base_table_name || '"';
        stmt := 'CREATE TABLE archive_tables."' || archive_table_name || '" AS SELECT * FROM ' || source_table || ';';

        BEGIN
            EXECUTE stmt;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '❌ Failed to copy %.%: %', r.schema_name, base_table_name, SQLERRM;
                CONTINUE;
        END;

        -- Log mapping
        stmt := 'INSERT INTO archive_tables.archive_table_mapping (' ||
                'original_schema_name, original_table_name, archive_table_name, truncated' ||
                ') VALUES (''' ||
                r.schema_name || ''', ''' ||
                base_table_name || ''', ''' ||
                archive_table_name || ''', ' ||
                CASE WHEN was_truncated THEN 'true' ELSE 'false' END || ');';
        EXECUTE stmt;
    END LOOP;

    RAISE NOTICE '🏁 Done. % tables processed (new archives only).', row_counter;
END;
$$ LANGUAGE plpgsql;
```

---

## ▶️ Step 7: Run the Procedure

```sql
CALL archive_tables.copy_from_archive_candidates();
```

You can run this repeatedly — it will only archive new `BASE TABLES` not yet mapped.

---

## 📊 Step 8: View Archived Mappings

```sql
SELECT * FROM archive_tables.archive_table_mapping;
```

---

## ✅ Summary

| Step | Purpose |
|------|---------|
| archive_candidates | Stores candidate `BASE TABLES` for deprecation |
| archive_table_mapping | Tracks copies and final archive names |
| `copy_from_archive_candidates()` | Copies base tables safely and idempotently |
| ✅ Skips duplicates | Already archived tables won't be processed again |
| ✅ Truncates + suffixes | Handles Redshift's 127-char table name limit |
| ❗ Views not included | Future versions may handle views separately |

---

📌 This is **Phase 1 — Base Table Archiving**.  
**Views** and more complex dependencies will come next!
