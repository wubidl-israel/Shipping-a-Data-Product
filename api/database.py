# Postgres connector
import os, psycopg2
from dotenv import load_dotenv

load_dotenv()


# Function to get a connection to the PostgreSQL database
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
