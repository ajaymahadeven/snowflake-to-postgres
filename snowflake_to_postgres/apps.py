from django.apps import AppConfig


class SnowflakeToPostgresConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "snowflake_to_postgres"
    verbose_name = "Snowflake to PostgreSQL Migration"
