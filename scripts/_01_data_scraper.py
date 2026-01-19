import json
import os
import time
import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telethon import TelegramClient

# -------------------- Setup -------------------- #

parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Run scraper in test mode")
args = parser.parse_args()

# Load environment variables from parent directory
load_dotenv(os.path.join(os.path.abspath(os.path.join("..")), ".env"))

# Get Telegram API credentials from environment variables
api_id = os.getenv("TG_API_ID")
api_hash = os.getenv("TG_API_HASH")

# Check if API credentials are provided
if not api_id or not api_hash:
    raise ValueError("Missing Telegram API credentials. Check your .env file.")

# Configure logging
log_dir = os.path.abspath(os.path.join("..", "logs"))
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "scraper.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# Define path for the Telegram session file
session_path = os.path.abspath(os.path.join("..", "scraper", "scraping_session"))
os.makedirs(os.path.dirname(session_path), exist_ok=True)

# Initalise Telegram client
client = TelegramClient(session_path, api_id, api_hash)


# -------------------- Scrape Logic --------------------
async def scrape_channel(client, channel_username, msg_limit, test_mode=False):
    """
    Scrapes messages from a given Telegram channel and writes them to a CSV.

    Args:
        client (TelegramClient): The Telegram client.
        channel_username (str): The username of the Telegram channel.
        msg_limit (int): Messgae Limit for test.
    """
    # Start timer
    start = time.time()

    # Get scraping day
    today = datetime.today().strftime("%Y-%m-%d")

    base_dir = "../data/test" if test_mode else "../data/raw/telegram_messages"
    output_dir = os.path.abspath(os.path.join(base_dir, today))
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{channel_username[1:]}.json")
    pretty_path = Path(output_path).as_posix()  # Polished path print

    if os.path.exists(output_path):
        logging.info(f"Skipped (already exists): {pretty_path}.")
        print(f"\n{channel_username} already scraped — skipping.")
        return

    logging.info(f"Starting scrape: {channel_username}.")
    print((f"\nStarted scraping from {channel_username} ..."))

    messages = []
    if test_mode:
        # Simulate dummy message
        for i in range(1, 6):  # Generate 5 messages
            messages.append(
                {
                    "channel_title": f"Mock Channel {channel_username[1:]}",
                    "channel_username": channel_username,
                    "id": 1000 + i,
                    "text": f"This is mock message #{i} from {channel_username}",
                    "date": (datetime.now() - timedelta(minutes=i * 5)).isoformat(),
                    "views": i * 10,
                    "media_type": "document" if i == 3 else "photo" if i % 2 else None,
                    "media_path": (
                        None
                        if i % 2 == 0
                        else f"/mock/path/{channel_username[1:]}_{i}.jpg"
                    ),
                }
            )
        print(f"Pretended to scrape 5 messages from {channel_username}")

    else:

        # Get the channel entity
        entity = await client.get_entity(channel_username)
        channel_title = entity.title  # Get the channel title
        # Iterate through messages in the channel (up to a limit)
        async for msg in client.iter_messages(entity, limit=msg_limit):
            # Scrap message
            msg_dict = {
                "channel_title": channel_title,
                "channel_username": channel_username,
                "id": msg.id,
                "text": msg.message,
                "date": msg.date.isoformat() if msg.date else None,
                "views": msg.views or 0,  # Fallback if views is None
                "media_type": None,
            }
            # Check if the message has media and determine its type
            if msg.media and hasattr(msg.media, "photo"):
                msg_dict["media_type"] = "photo"
                image_path = os.path.abspath(
                    os.path.join(
                        "..", "data/images", f"{channel_username[1:]}_{msg.id}.jpg"
                    )
                )
                os.makedirs(os.path.dirname(image_path), exist_ok=True)
                await client.download_media(msg, image_path)
                msg_dict["media_path"] = image_path  # Optional: include in JSON

            elif msg.media and hasattr(msg.media, "document"):
                msg_dict["media_type"] = "document"

            messages.append(msg_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)

    duration = time.time() - start  # End timing after all scraping and saving
    logging.info(f"{channel_username} scraped in {duration:.2f} seconds.")
    print(f"{channel_username} scraped in {duration:.2f} seconds.")

    logging.info(f"Scraped {len(messages)} messages from {channel_username}.")
    print(f"Scraped {len(messages)} messages from {channel_username}.")

    logging.info(f"Saved to {pretty_path}.")
    print(f"Scraped data from {channel_username} saved to {pretty_path}.\n")


# -------------------- Main Routine --------------------#
async def main():
    """
    Main function to initialise the client, set up directories,
    and scrape data from a list of channels.
    """
    if not args.test:
        await client.start()  # Initialises the connection

    # List of Telegram channels to scrape
    channels = (
        ["@mock_pharma", "@mock_food", "@mock_optics", "@mock_tena", "@mock_drug"]
        if args.test
        else [
            "@lobelia4cosmetics",
            "@tikvahpharma",
            "@yetenaweg",
            "@ethiopianfoodanddrugauthority",
            "@CheMed123",
            "@newoptics",
        ]
    )
    msg_limit = 1 if args.test else 10000
    if args.test:
        print("Test mode ON — reduced scraping for speed.")

    # Iterate through each channel
    for channel in channels:
        try:
            await scrape_channel(client, channel, msg_limit, test_mode=args.test)
        except Exception as e:
            # Catch and print any errors during scraping
            logging.error(f"Error scraping {channel}: {e}.")
            print(f"Skipping {channel} due to error: {e}.\n")

    logging.info("All channels scraped successfully.")
    print("All channels scraped successfully.")


# -------------------- Execute --------------------#
# Run the main asynchronous function
if __name__ == "__main__":
    asyncio.run(main())
