import os
import re
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load Redshift credentials from .env
load_dotenv()

# Establish Redshift connection
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT", "5439"),
    dbname=os.getenv("REDSHIFT_DBNAME"),  # should be 'analytics'
    user=os.getenv("REDSHIFT_USER_NAME"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
conn.autocommit = True
cur = conn.cursor()

try:
    # Step 1: Get one unarchived view
    cur.execute("""
        SELECT schema_name, table_name
        FROM archive_schemas.archive_candidates c
        WHERE object_type = 'VIEW'
        AND NOT EXISTS (
            SELECT 1
            FROM analytics.archive_views.archive_view_mapping m
            WHERE m.original_schema_name = c.schema_name
              AND m.original_view_name = c.table_name
        )
        LIMIT 1
    """)
    row = cur.fetchone()

    if not row:
        print("📭 No unarchived views found.")
        conn.close()
        exit()

    schema, view = row
    archive_schema = "archive_views"
    archive_view = view

    print(f"🔍 Archiving view: {schema}.{view}")

    # Step 2: Get CREATE VIEW definition
    cur.execute(sql.SQL("SHOW VIEW {}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(view)
    ))
    ddl = cur.fetchone()[0]

    # Step 3: Clean the DDL
    cleaned = ddl.strip()
    cleaned = re.sub(r'(?is)^.*?AS\s*\(', '', cleaned)  # remove header
    cleaned = re.sub(r'\)\s+with no schema binding\s*;?\s*$', '', cleaned, flags=re.IGNORECASE)  # remove footer
    cleaned = re.sub(r'\)\s*$', '', cleaned.strip())  # safety net

    # Step 4: Generate CREATE VIEW for archive
    create_sql = sql.SQL("CREATE VIEW {}.{} AS ({})").format(
        sql.Identifier(archive_schema),
        sql.Identifier(archive_view),
        sql.SQL(cleaned)
    )

    print("🛠️ Final CREATE VIEW SQL:")
    print(create_sql.as_string(cur)[:500] + "...\n")  # preview first 500 chars

    # Step 5: Execute CREATE VIEW
    try:
        cur.execute(create_sql)
        print(f"✅ Created archived view: {archive_schema}.{archive_view}")
    except Exception as e:
        print(f"❌ Failed to create archived view: {e}")
        conn.close()
        exit()

    # Step 6: Log to archive_view_mapping
    cur.execute("""
        INSERT INTO analytics.archive_views.archive_view_mapping (
            original_schema_name,
            original_view_name,
            archive_view_name,
            truncated
        ) VALUES (%s, %s, %s, %s)
    """, (schema, view, archive_view, False))

    print(f"📝 Mapping logged: {schema}.{view} → {archive_view}")

finally:
    cur.close()
    conn.close()
