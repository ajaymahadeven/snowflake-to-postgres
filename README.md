````markdown
# Snowflake to PostgreSQL Migration

A Django-based utility to migrate schemas and data from **Snowflake** to **PostgreSQL**.

This tool is particularly useful for **cost reduction**, **faster local testing**, and **simplifying analytics pipelines** by moving data from Snowflake (cloud-based) to a more affordable PostgreSQL setup.

---

## Advantages

- **Cost Cutting:** Reduce reliance on Snowflake for heavy queries and storage.
- **Faster Development & Testing:** Work locally with PostgreSQL without hitting Snowflake.
- **Data Portability:** Easily transfer schemas and data between environments.
- **Selective Migration:** Migrate entire schemas or specific tables.

---

## Pre-requisites

1. **Python Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate     # Windows
   ```
````

2. **Install Required Packages**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**
   Copy `.env.example` to `.env` and fill in your credentials. Example:

   ```env
   # Django
   SECRET_KEY=''
   DEBUG=True
   ALLOWED_HOSTS=localhost,127.0.0.1

   # Snowflake Credentials
   SNOWFLAKE_USER=''
   SNOWFLAKE_PASSWORD=''
   SNOWFLAKE_ACCOUNT=''
   SNOWFLAKE_URL=''
   SNOWFLAKE_DATABASE=''
   SNOWFLAKE_WAREHOUSE=''

   # PostgreSQL Credentials
   POSTGRES_USER=''
   POSTGRES_PASSWORD=''
   POSTGRES_HOST=''
   POSTGRES_PORT=''
   POSTGRES_DB='data_factory_ops'
   ```

   ⚠️ **Note:** Replace `data_factory_ops` with your own database name everywhere. Ensure both Snowflake and PostgreSQL users have appropriate permissions.

---

## Commands

### migrate

Full migration: creates tables **and copies data**.

```bash
python manage.py sf_migrate migrate --schema SOURCE_SCHEMA --target TARGET_SCHEMA
```

### discover

Inspect tables, columns, and row counts in a Snowflake schema.

```bash
python manage.py sf_migrate discover --schema SOURCE_SCHEMA
```

### build

Create tables in PostgreSQL **without copying data**.

```bash
python manage.py sf_migrate build --schema SOURCE_SCHEMA --target TARGET_SCHEMA
```

### transfer

Copy data to **existing tables** in PostgreSQL.

```bash
python manage.py sf_migrate transfer --schema SOURCE_SCHEMA --target TARGET_SCHEMA
```

### destroy

Delete a schema and all its tables in PostgreSQL.

```bash
python manage.py sf_migrate destroy --target TARGET_SCHEMA --force
```

---

## Options

```bash
--table TABLE_NAME        # Migrate or transfer a single table only
--batch-size 50000        # Larger batch = faster migration
--dry-run                 # Preview without executing
--output file.sql         # Save DDL to file
--force                   # Skip confirmation prompts
```

---

## Examples

```bash
# Full migration
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi

# Fast migration with larger batches
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi --batch-size 100000

# Rebuild schema from scratch
python manage.py sf_migrate destroy --target n360dev_pi --force
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi

# Build a single table
python manage.py sf_migrate build --schema N360DEV_PI --target n360dev_pi --table CUSTOMERS

# Transfer a single table
python manage.py sf_migrate transfer --schema N360DEV_PI --target n360dev_pi --table CUSTOMERS

# Preview DDL before executing
python manage.py sf_migrate build --schema N360DEV_PI --output /tmp/preview.sql
```

---

## Migration Performance

- Typical speed: **10,000 - 100,000 rows/second** depending on network and table size.

---

## What’s Migrated

- Tables
- Data
- Column types
- Constraints (primary keys, foreign keys)

> **Note:** Views and stored procedures must be migrated manually.

---

## Verify Migration

Connect to PostgreSQL and inspect tables:

```bash
python manage.py dbshell --database data_factory_ops
```

```sql
\dt your_schema.*
SELECT COUNT(*) FROM your_schema."TABLE_NAME";
```

---

## Help

```bash
python manage.py sf_migrate --help
```

---

**With this setup, you can quickly migrate data from Snowflake to PostgreSQL, cut costs, and accelerate local development.**
