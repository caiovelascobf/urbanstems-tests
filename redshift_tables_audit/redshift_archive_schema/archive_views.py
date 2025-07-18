import os
import re
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Config
MAX_LIMIT = 100
ARCHIVE_SCHEMA = "archive_views"
MAPPING_TABLE = "analytics.archive_views.archive_view_mapping"
MAX_VIEW_NAME_LENGTH = 127

# Connect to Redshift
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT", "5439"),
    dbname=os.getenv("REDSHIFT_DBNAME"),
    user=os.getenv("REDSHIFT_USER_NAME"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
conn.autocommit = True
cur = conn.cursor()

archived_count = 0
skipped_broken = 0
create_failures = 0

# Fetch batch
cur.execute(sql.SQL(f"""
    SELECT schema_name, table_name
    FROM archive_schemas.archive_candidates c
    WHERE object_type = 'VIEW'
    AND NOT EXISTS (
        SELECT 1
        FROM {MAPPING_TABLE} m
        WHERE m.original_schema_name = c.schema_name
          AND m.original_view_name = c.table_name
    )
    LIMIT %s
"""), (MAX_LIMIT,))
views = cur.fetchall()

for schema, view in views:
    print(f"\n🔍 Archiving view: {schema}.{view}")
    archive_view = view
    was_truncated = False
    ddl = None
    broken_reason = None
    is_broken = False
    create_error = False

    # Truncate if needed
    if len(archive_view) > MAX_VIEW_NAME_LENGTH:
        archive_view = archive_view[:MAX_VIEW_NAME_LENGTH]
        was_truncated = True

    # Ensure uniqueness
    suffix = 1
    base_name = archive_view
    while True:
        cur.execute(sql.SQL("""
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = %s AND table_name = %s
        """), (ARCHIVE_SCHEMA, archive_view))
        if not cur.fetchone():
            break
        suffix_str = f"_{suffix}"
        archive_view = base_name[:MAX_VIEW_NAME_LENGTH - len(suffix_str)] + suffix_str
        was_truncated = True
        suffix += 1

    # Try to get the DDL (SHOW VIEW)
    try:
        cur.execute(sql.SQL("SHOW VIEW {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(view)
        ))
        ddl = cur.fetchone()[0]
    except Exception as e:
        broken_reason = f"SHOW VIEW error: {str(e).strip()}"
        print(f"❌ Failed to get DDL for {schema}.{view}: {broken_reason}")
        cur.execute(sql.SQL(f"""
            INSERT INTO {MAPPING_TABLE} (
                original_schema_name,
                original_view_name,
                archive_view_name,
                truncated,
                is_view_broken,
                create_error,
                broken_reason,
                view_definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """), (schema, view, archive_view, was_truncated, True, False, broken_reason, None))
        skipped_broken += 1
        continue

    # Validate SELECT * FROM view
    try:
        cur.execute(sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
            sql.Identifier(schema),
            sql.Identifier(view)
        ))
    except Exception as e:
        is_broken = True
        broken_reason = str(e).strip()
        print(f"⚠️ View {schema}.{view} is broken and will be skipped: {broken_reason}")
        cur.execute(sql.SQL(f"""
            INSERT INTO {MAPPING_TABLE} (
                original_schema_name,
                original_view_name,
                archive_view_name,
                truncated,
                is_view_broken,
                create_error,
                broken_reason,
                view_definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """), (schema, view, archive_view, was_truncated, True, False, broken_reason, ddl))
        skipped_broken += 1
        continue

    # Clean DDL from SHOW VIEW
    cleaned = ddl.strip()

    # Extract contents from inside the outermost CREATE VIEW AS ( ... )
    match = re.search(r'(?is)create\s+view\s+.*?\s+as\s*\((.*)\)\s+with\s+no\s+schema\s+binding\s*;?\s*$', cleaned, re.DOTALL)

    if match:
        cleaned = match.group(1).strip()
    else:
        # Fallback: strip leading AS ( and trailing )
        cleaned = re.sub(r'(?is)^.*?AS\s*\(', '', cleaned)
        cleaned = re.sub(r'\)\s+with\s+no\s+schema\s+binding\s*;?\s*$', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\)\s*$', '', cleaned.strip())

    # ✅ Do NOT add or wrap in extra parentheses anymore
    # Redshift expects: CREATE VIEW ... AS <sql>
    create_sql = sql.SQL("CREATE VIEW {}.{} AS {}").format(
        sql.Identifier(ARCHIVE_SCHEMA),
        sql.Identifier(archive_view),
        sql.SQL(cleaned)
    )


    try:
        cur.execute(create_sql)
    except Exception as e:
        create_error = True
        broken_reason = f"CREATE VIEW error: {str(e).strip()}"
        print(f"❌ Failed to create archived view {ARCHIVE_SCHEMA}.{archive_view}: {broken_reason}")
        cur.execute(sql.SQL(f"""
            INSERT INTO {MAPPING_TABLE} (
                original_schema_name,
                original_view_name,
                archive_view_name,
                truncated,
                is_view_broken,
                create_error,
                broken_reason,
                view_definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """), (schema, view, archive_view, was_truncated, False, True, broken_reason, ddl))
        create_failures += 1
        continue

    # Log success
    cur.execute(sql.SQL(f"""
        INSERT INTO {MAPPING_TABLE} (
            original_schema_name,
            original_view_name,
            archive_view_name,
            truncated,
            is_view_broken,
            create_error,
            broken_reason,
            view_definition
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """), (schema, view, archive_view, was_truncated, False, False, None, ddl))

    print(f"✅ Archived {schema}.{view} → {ARCHIVE_SCHEMA}.{archive_view}")
    archived_count += 1

# Done
cur.close()
conn.close()

print(f"\n🏁 Done. {archived_count} views archived, {skipped_broken} broken views, {create_failures} create failures logged.")
