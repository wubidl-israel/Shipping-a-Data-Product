import os
import json
import psycopg2
import logging
import argparse
from dotenv import load_dotenv
from ultralytics import YOLO

# Specify directory
root_dir = os.path.abspath(os.path.join(".."))

# Set up and configure logging
log_dir = os.path.join(root_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "enricher.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
)

# Set up test
parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Run enricher in test mode")
args = parser.parse_args()

# Class input directory
image_base_path = (
    os.path.join(root_dir, "data", "test", "images")
    if args.test
    else os.path.join(root_dir, "data", "images")
)

output_base_path = (
    os.path.join(root_dir, "data", "test", "fct_image_detections.json")
    if args.test
    else os.path.join(root_dir, "data", "processed", "fct_image_detections.json")
)


class DataEnricher:
    def __init__(
        self,
        model_path="yolov8n.pt",
        image_dir=image_base_path,
        output_path=output_base_path,
    ):
        """
        Initialise the DataEnricher with model path, image directory, and output file path.

        Args:
            model_path (str): Path to the YOLO model.
            image_dir (str): Directory containing images to process.
            output_path (str): Path to save the enriched data.
        """
        load_dotenv(os.path.join(os.path.abspath(os.path.join("..")), ".env"))

        self.image_dir = image_dir
        self.output_path = output_path
        self.model = YOLO(model_path)
        self.results = []

        logging.info("YOLO model initialised.")

    def connect_db(self):
        """
        Connect to the PostgreSQL database.
        """
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
            logging.info("Connected to PostgreSQL.")
            return conn
        except Exception as e:
            logging.error(f"DB connection error: {e}")
            return None

    def fetch_messages_with_images(self):
        """
        Fetch messages from the database that contain images.
        """
        conn = self.connect_db()
        if not conn:
            return []

        query = """
            SELECT message_id
            FROM raw_marts.fct_messages
            WHERE has_image IS TRUE;
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                conn.close()
                return [row[0] for row in rows]
        except Exception as e:
            logging.error(f"Failed to fetch messages: {e}")
            return []

    def enrich_image(self, message_id):
        """
        Enrich the image associated with a message ID by performing object detection.
        """
        image_path = os.path.join(self.image_dir, f"{message_id}.jpg")
        if not os.path.exists(image_path):
            logging.warning(f"Missing image: {image_path}")
            return

        try:
            results = self.model(image_path, verbose=False)[0]
            for box in results.boxes:
                obj = {
                    "message_id": message_id,
                    "detected_object": self.model.names[int(box.cls[0])],
                    "confidence_score": round(float(box.conf[0]), 4),
                    "bbox": box.xyxy[0].tolist(),
                }
                self.results.append(obj)
        except Exception as e:
            logging.error(f"Failed detection for {message_id}: {e}")

    def process_all(self):
        """
        Process all images for enrichment.
        """
        logging.info("Starting enrichment...")
        message_ids = self.fetch_messages_with_images()
        for message_id in message_ids:
            self.enrich_image(message_id)
        logging.info(f"Finished enrichment for {len(message_ids)} images.")

    def save_results(self):
        """
        Save the enriched results to a JSON file.
        """
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        if not self.results:
            logging.warning("No results to save.")
            return
        try:
            with open(self.output_path, "w", encoding="utf-8") as f:
                json.dump(self.results, f, indent=2)

            logging.info(
                f"Saved {len(self.results)} detections to {os.path.relpath(self.output_path)}"
            )
        except Exception as e:
            logging.error(f"Failed to save detections: {e}")


if __name__ == "__main__":
    enricher = DataEnricher()
    enricher.process_all()
    enricher.save_results()
