````markdown
# Snowflake to PostgreSQL Migration

Copy Snowflake schemas to PostgreSQL.


## Pre-requities :



## Commands

### migrate

Full migration: creates tables + copies data

```bash
python manage.py sf_migrate migrate --schema SOURCE_SCHEMA --target target_schema
```
````

### discover

See tables, columns, and row counts

```bash
python manage.py sf_migrate discover --schema SOURCE_SCHEMA
```

### build

Create tables only (no data)

```bash
python manage.py sf_migrate build --schema SOURCE_SCHEMA --target target_schema
```

### transfer

Copy data to existing tables

```bash
python manage.py sf_migrate transfer --schema SOURCE_SCHEMA --target target_schema
```

### destroy

Delete schema and all tables

```bash
python manage.py sf_migrate destroy --target target_schema --force
```

## Options

```bash
--table TABLE_NAME        # Single table only
--batch-size 50000        # Bigger = faster
--dry-run                 # Preview only
--output file.sql         # Save DDL to file
--force                   # Skip confirmations
```

## Examples

```bash
# Full migration
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi


# Fast migration (large batches)
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi --batch-size 100000


# Rebuild from scratch
python manage.py sf_migrate destroy --target n360dev_pi --force
python manage.py sf_migrate migrate --schema N360DEV_PI --target n360dev_pi

# Build single table only
python manage.py sf_migrate build --schema N360DEV_PI --target n360dev_pi --table CUSTOMERS

#  Transfer single table only
python manage.py sf_migrate transfer --schema N360DEV_PI --target n360dev_pi --table CUSTOMERS

# Preview DDL before executing
python manage.py sf_migrate build --schema N360DEV_PI --output /tmp/preview.sql


```

## Speed

10,000 - 100,000 rows/second typical.

## What's Migrated

- Tables
- Data
- Column types
- Constraints (primary keys, foreign keys)

Views and procedures need manual work.

## Verify Migration

```bash
python manage.py dbshell --database data_factory_ops
```

```sql
\dt your_schema.*
SELECT COUNT(*) FROM your_schema."TABLE_NAME";
```

## Help

```bash
python manage.py sf_migrate --help
```

```

Perfect - all commands in same format, super clean.
```

```

```
