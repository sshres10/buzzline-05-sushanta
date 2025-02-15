"""
consumer_yourname.py

Consumes JSON messages from a live data file.
Processes the messages and stores them in an SQLite database.

Example JSON message:
{
    "message": "I have a dream.",
    "author": "Martin Luther King Jr.",
    "timestamp": "1963-08-28 15:00:00",
    "category": "civil rights",
    "keyword_mentioned": "dream",
    "message_length": 15
}
"""

#####################################
# Import Modules
#####################################

import json
import pathlib
import sys
import time
import sqlite3
import matplotlib.pyplot as plt  # Import for chart generation

# Local modules
import utils.utils_config as config
from utils.utils_logger import logger
from consumers.db_sqlite_case import init_db, insert_message

#####################################
# Helper Function: Categorize Message Length
#####################################

def categorize_message_length(message_length):
    """Categorizes messages as Short, Medium, or Long based on length."""
    if message_length < 20:
        return "Short"
    elif message_length < 50:
        return "Medium"
    else:
        return "Long"

#####################################
# Function to Process a Single Message
#####################################

def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): JSON message.

    Returns:
        dict: Processed message or None if an error occurs.
    """
    try:
        message_length = int(message.get("message_length", 0))  # Ensure message_length is an integer

        processed_message = {
            "author": message.get("author", "Unknown"),
            "timestamp": message.get("timestamp", ""),
            "message": message.get("message", ""),
            "category": message.get("category", "Uncategorized"),
            "keyword_mentioned": message.get("keyword_mentioned", "None"),
            "message_length": message_length,
            "length_category": categorize_message_length(message_length)  # Store message length category
        }

        logger.info(f"Processed message: {processed_message}")
        return processed_message

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

#####################################
# Consume Messages from Live Data File
#####################################

def consume_messages_from_file(live_data_path, sql_path, interval_secs):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    """
    logger.info(f"Starting file-based consumer, reading from {live_data_path}")

    # Initialize database
    init_db(sql_path)

    message_lengths = {"Short": 0, "Medium": 0, "Long": 0}  # Track message length categories

    while True:
        try:
            with open(live_data_path, "r", encoding="utf-8") as file:
                messages = json.load(file)  # Read entire JSON file

            for message in messages:
                processed_message = process_message(message)
                if processed_message:
                    insert_message(processed_message, sql_path)
                    logger.info(f"Inserted message into database: {processed_message}")

                    # Track message length distribution
                    length_category = processed_message["length_category"]
                    message_lengths[length_category] += 1

            # Generate a chart after processing messages
            generate_chart(message_lengths)

            logger.info("Waiting for new messages...")
            time.sleep(interval_secs)  # Wait before checking for new messages

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            time.sleep(interval_secs)
            continue  # Skip this iteration
        except json.JSONDecodeError as e:
            logger.error(f"ERROR: Invalid JSON format in live data file: {e}")
            time.sleep(interval_secs)
            continue  # Skip this iteration
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(interval_secs)
            continue  # Skip this iteration

#####################################
# Function to Generate Chart
#####################################

def generate_chart(message_lengths):
    """
    Generates a bar chart for message length categories.

    Args:
        message_lengths (dict): Dictionary containing count of Short, Medium, and Long messages.
    """
    categories = list(message_lengths.keys())
    counts = list(message_lengths.values())

    plt.figure(figsize=(8, 5))
    plt.bar(categories, counts, color=["green", "blue", "red"])
    plt.xlabel("Message Length Category")
    plt.ylabel("Number of Messages")
    plt.title("Message Length Distribution")
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Save the chart as an image
    plt.savefig("message_length_distribution.png")
    plt.close()

    logger.info("Chart generated: message_length_distribution.png")

#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting File Consumer...")

    # Read environment variables
    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        live_data_path = config.get_live_data_path()
        sqlite_path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # Initialize database and start consuming messages
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

#####################################
# Execute Main Function
#####################################

if __name__ == "__main__":
    main()
