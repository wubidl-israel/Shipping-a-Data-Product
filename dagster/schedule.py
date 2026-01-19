from dagster import ScheduleDefinition
from pipeline import telegram_pipeline

daily_schedule = ScheduleDefinition(
    job=telegram_pipeline,
    cron_schedule="0 7 * * *",  # every day at 7AM
    execution_timezone="Africa/Addis_Ababa",
)
