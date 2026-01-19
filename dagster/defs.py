from pipeline import telegram_pipeline
from schedule import daily_schedule
from dagster import Definitions

defs = Definitions(jobs=[telegram_pipeline], schedules=[daily_schedule])
