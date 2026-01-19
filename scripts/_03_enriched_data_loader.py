import json, os
import psycopg2
import logging
import argparse
from dotenv import load_dotenv

# Specify directory
root_dir = os.path.abspath(os.path.join(".."))

# Set up and configure logging
log_dir = os.path.join(root_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "enriched_loader.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
)

# Set up test
parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Run loader in test mode")
args = parser.parse_args()


class EnrichedDataLoader:
    def __init__(
        self,
        path=(
            "../data/test/fct_image_detections.json"
            if args.test
            else "../data/processed/fct_image_detections.json"
        ),
    ):
        """
        Initialise the EnrichedDataLoader.
        """
        load_dotenv(os.path.join(os.path.abspath(os.path.join("..")), ".env"))
        logging.info("EnrichedDataLoader initialised.")
        self.path = path

    def load_enriched_messages(self):
        """
        Load enriched messages from the JSON file and insert them into the PostgreSQL database.
        """
        logging.info("Loading environment variables...")

        load_dotenv(os.path.join(os.path.abspath(os.path.join("..")), ".env"))
        try:
            conn = psycopg2.connect(
                dbname=(
                    os.getenv("POSTGRES_DB_TEST")
                    if args.test
                    else os.getenv("POSTGRES_DB")
                ),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
            )
            cursor = conn.cursor()
            logging.info("Connected to PostgreSQL database.")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return

        cursor.execute("SELECT current_database();")
        active_db = cursor.fetchone()[0]
        if args.test and active_db != "telegram_health_test":
            logging.error("Aborting: connected to wrong database for test mode.")
            conn.close()
            return

        try:
            logging.info(
                "Ensuring enriched schema and fct_image_detections table exist..."
            )
            cursor.execute("CREATE SCHEMA IF NOT EXISTS enriched;")
            cursor.execute("DROP TABLE IF EXISTS enriched.fct_image_detections;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS enriched.fct_image_detections (
                    message_id TEXT,
                    detected_object TEXT,
                    confidence_score FLOAT,
                    bbox JSONB
                );
            """
            )
            logging.info("Schema and table setup completed.")
        except Exception as e:
            logging.error(f"Error during schema/table creation: {e}")
            return
        # Load messages from JSON files
        logging.info("Loading messages from JSON files...")

        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for obj in data:
            cursor.execute(
                """
                INSERT INTO enriched.fct_image_detections (message_id, detected_object, confidence_score, bbox)
                VALUES (%s, %s, %s, %s);
            """,
                (
                    obj["message_id"],
                    obj["detected_object"],
                    obj["confidence_score"],
                    json.dumps(obj["bbox"]),
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(
            f"{len(data)} detections loaded into enriched.fct_image_detections."
        )
        logging.info("Enriched messages loaded successfully.")


if __name__ == "__main__":
    enriched_loader = EnrichedDataLoader()
    enriched_loader.load_enriched_messages()
