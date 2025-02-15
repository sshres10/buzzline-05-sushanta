"""
db_sqlite_case.py

Handles SQLite database operations:
- Initializes the database
- Inserts processed messages
- Deletes messages

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

import os
import pathlib
import sqlite3
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database by creating the 'streamed_messages' table
    without the 'sentiment' column.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    
    try:
        # Ensure the directories exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            # Drop the existing table if it exists
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            # Create a new table without the 'sentiment' column
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    keyword_mentioned TEXT,
                    message_length INTEGER,
                    length_category TEXT
                )
            """)

            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite database at {db_path}: {e}")

#####################################
# Function to Insert a Processed Message
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a processed message into the SQLite database.

    Args:
    - message (dict): The processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, keyword_mentioned, message_length, length_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                message["message"],
                message["author"],
                message["timestamp"],
                message["category"],
                message["keyword_mentioned"],
                message["message_length"],
                message["length_category"]
            ))

            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

#####################################
# Function to Delete a Message by ID
#####################################

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database using its ID.

    Args:
    - message_id (int): The ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")

#####################################
# Testing Functions (Main)
#####################################

def main():
    logger.info("Starting database testing.")

    # Define the path to the test database
    DATA_PATH = config.get_base_data_path()
    TEST_DB_PATH = DATA_PATH / "test_buzz.sqlite"

    # Initialize the database
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    # Test message
    test_message = {
        "message": "I have a dream.",
        "author": "Martin Luther King Jr.",
        "timestamp": "1963-08-28 15:00:00",
        "category": "civil rights",
        "keyword_mentioned": "dream",
        "message_length": 15,
        "length_category": "Short"
    }

    # Insert test message
    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")

#####################################
# Execute main() for testing
#####################################

if __name__ == "__main__":
    main()
