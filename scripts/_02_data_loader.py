import json, os
import psycopg2
import logging
import argparse
from dotenv import load_dotenv
from datetime import datetime

# -------------------- Setup -------------------- #
parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Run loader in test mode")
args = parser.parse_args()

script_dir = os.path.dirname(os.path.abspath(__file__))
base_path = (
    os.path.join(script_dir, "..", "data", "test")
    if args.test
    else os.path.join(script_dir, "..", "data", "raw", "telegram_messages")
)
today = datetime.today().strftime("%Y-%m-%d")
folder_path = os.path.abspath(os.path.join(base_path, today))


# Set up and configure logging
log_dir = os.path.join("..", "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "loader.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
)


# -------------------- Main Function --------------------#
def load_telegram_messages():
    logging.info("Loading environment variables...")
    env_path = os.path.abspath(os.path.join(script_dir, "..", ".env"))
    load_dotenv(env_path)

    logging.info(f"POSTGRES_DB_TEST from env: {os.getenv('POSTGRES_DB_TEST')}")

    table_name = "raw.telegram_messages_test" if args.test else "raw.telegram_messages"

    try:
        conn = psycopg2.connect(
            dbname=(
                os.getenv("POSTGRES_DB_TEST") if args.test else os.getenv("POSTGRES_DB")
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

    try:
        logging.info("Ensuring raw schema and telegram_messages table exist...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                channel_title TEXT,
                channel_username TEXT,
                id BIGINT,
                text TEXT,
                date TIMESTAMP,
                views INTEGER,
                media_type TEXT
            );
        """
        )
        logging.info("Schema and table setup completed.")
    except Exception as e:
        logging.error(f"Error during schema/table creation: {e}")
        return

    # Load messages from JSON files
    logging.info("Loading messages from JSON files...")
    data_paths = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".json")
    ]

    total_inserted = 0
    for path in data_paths:
        if not os.path.exists(path):
            logging.warning(f"File not found: {path}")
            continue

        logging.info(f"Loading messages from {path}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                messages = json.load(f)
                for msg in messages:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name} (
                            channel_title, channel_username, id, text, date, views, media_type
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            msg["channel_title"],
                            msg["channel_username"],
                            msg["id"],
                            msg["text"],
                            msg["date"],
                            msg["views"],
                            msg["media_type"],
                        ),
                    )
                    total_inserted += 1
        except Exception as e:
            logging.error(f"Failed to process {path}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Load complete: {total_inserted} messages inserted.")


# -------------------- Execute --------------------#
if __name__ == "__main__":
    load_telegram_messages()
