from dagster import op, In, job
from dotenv import load_dotenv
import subprocess
import psycopg2
import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(script_dir, "..", ".env"))
load_dotenv(env_path)


@op
def validate_test_db(context):
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB_TEST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    cur = conn.cursor()
    cur.execute("SELECT current_database();")
    active_db = cur.fetchone()[0]
    cur.close()
    conn.close()

    context.log.info(f"Connected to DB: {active_db}")
    if active_db != "telegram_health_test":
        raise Exception("Abort: Connected to production DB!")


@op
def scrape_telegram(context) -> str:
    # Call scraping script here
    context.log.info("Starting Telegram scraping...")
    python_path = sys.executable  # path to virtual env python
    result = subprocess.run(
        [python_path, "../scripts/_01_data_scraper.py", "--test"],
        capture_output=True,
        text=True,
    )
    context.log.info(f"Dagster running with: {sys.executable}")
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Telegram scraping failed.")
    context.log.info("Scraping complete.")
    return "Scraped"


@op
def load_to_postgres(context, status: str):
    # Call PostgreSQL loading script here
    context.log.info(f"Received status from scraper: {status}")
    context.log.info("Running raw data loader...")
    python_path = sys.executable
    result = subprocess.run(
        [python_path, "../scripts/_02_data_loader.py", "--test"],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Raw data loader failed.")
    context.log.info("Data loading complete.")
    return "Loaded"


@op(ins={"previous_status": In()})
def run_dbt(context, previous_status: str):
    # Call dbt transformations here
    context.log.info(f"dbt starting after: {previous_status}")
    context.log.info("Running dbt transformations...")
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            "../medical_insights",
            "--profile",
            "mock_medical_insights",
        ],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("dbt run failed.")
    context.log.info("dbt complete.")
    return "dbt"


@op(ins={"previous_status": In()})
def test_dbt(context, previous_status: str):
    context.log.info("Running dbt tests...")
    result = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            "../medical_insights",
            "--profile",
            "mock_medical_insights",
        ],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("dbt tests failed.")


@op(ins={"previous_status": In()})
def run_YOLO(context, previous_status: str) -> str:
    # Call YOLO enrichment script here
    context.log.info(f"YOLO running after: {previous_status}")
    context.log.info("Running YOLO enrichment...")
    python_path = sys.executable
    result = subprocess.run(
        [python_path, "../scripts/_03_data_enricher.py", "--test"],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Enrichment failed.")
    context.log.info("Enrichment complete.")
    return "Enriched"


@op(ins={"previous_status": In()})
def yolo_loader(context, previous_status: str) -> str:
    # This function is used to load YOLO enriched data
    context.log.info(f"Loading enriched detections after: {previous_status}")
    context.log.info("Loading YOLO enriched data...")
    python_path = sys.executable
    result = subprocess.run(
        [python_path, "../scripts/_03_enriched_data_loader.py", "--test"],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Enriched data loader failed.")
    context.log.info("Enriched data loading complete.")
    return "Detections loaded"


@job
def telegram_pipeline():
    validate_test_db()
    status = scrape_telegram()
    dbt_status = load_to_postgres(status)
    yolo_status = run_YOLO(dbt_status)
    loader_status = yolo_loader(yolo_status)
    dbt_run_result = run_dbt(loader_status)
    test_dbt(dbt_run_result)


# ✅ Scrape Telegram messages
# ✅ Load them into mock Postgres
# ✅ Enrich with YOLO
# ✅ Insert enrichment results into mock DB
# ✅ Run dbt transformations once enriched data is present
# ✅ Test dbt after run
