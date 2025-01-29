import os
import sqlite3
import logging
import hashlib
from datetime import datetime, timezone

class ChromaDBManager:
    def __init__(self, sqlite_db_path: str):
        self.sqlite_db_path = sqlite_db_path
        # Configure SQLite to use adapters for datetime
        sqlite3.register_adapter(datetime, self._adapt_datetime)
        sqlite3.register_converter('TIMESTAMP', self._convert_datetime)
        
        self._initialize_sqlite_db()
        self._setup_logger()

    @staticmethod
    def _adapt_datetime(val):
        """Convert datetime to ISO8601 string for SQLite storage."""
        return val.isoformat().encode('utf-8')

    @staticmethod
    def _convert_datetime(val):
        """Convert ISO8601 string back to datetime object."""
        return datetime.fromisoformat(val.decode('utf-8'))

    def _initialize_sqlite_db(self):
        """Create or connect to the SQLite database with the necessary schema."""
        # Use detect_types to enable automatic datetime conversion
        conn = sqlite3.connect(
            self.sqlite_db_path, 
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                path TEXT NOT NULL,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_size INTEGER,
                checksum TEXT UNIQUE,
                last_modified TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def _setup_logger(self):
        """Set up logging."""
        # Ensure the logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        # Configure logging with more robust settings
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("logs/file_check.log"),
                logging.StreamHandler()  # Optional: also print to console
            ]
        )

    def calculate_checksum(self, file_path: str) -> str:
        """Calculate the SHA256 checksum of a file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def get_tracked_files(self) -> set:
        """Retrieve all tracked files from the SQLite database."""
        conn = sqlite3.connect(
            self.sqlite_db_path, 
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = conn.cursor()
        cursor.execute("SELECT path, checksum FROM file_metadata")
        tracked_files = set(cursor.fetchall())
        conn.close()
        return tracked_files

    def check_for_new_files(self, directory: str):
        """Check for new or modified files in the given directory."""
        tracked_files = self.get_tracked_files()
        new_files = []
        
        logging.info(f"Checking for new or modified files in {directory}")

        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)

                if not os.path.isfile(file_path):
                    continue

                # File metadata
                file_size = os.path.getsize(file_path)
                last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                checksum = self.calculate_checksum(file_path)

                # Skip already tracked files if checksum matches
                if (file_path, checksum) in tracked_files:
                    continue

                # Log as a new or updated file
                new_files.append((file_path, file_size, checksum, last_modified))
                logging.info(f"New or modified file detected: {file_path}")

        # Update SQLite with new files
        if new_files:
            self._update_file_metadata(new_files)
        else:
            logging.info("No new or modified files found.")

    def _update_file_metadata(self, new_files: list):
        """Update SQLite with new file metadata."""
        conn = sqlite3.connect(
            self.sqlite_db_path, 
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = conn.cursor()

        for file_path, file_size, checksum, last_modified in new_files:
            # Ensure last_modified is timezone-aware
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=timezone.utc)
            
            cursor.execute("""
                INSERT OR REPLACE INTO file_metadata (filename, path, file_size, checksum, last_modified)
                VALUES (?, ?, ?, ?, ?)
            """, (os.path.basename(file_path), file_path, file_size, checksum, last_modified))

        conn.commit()
        conn.close()

        logging.info(f"Metadata updated for {len(new_files)} new files.")


# Example Usage
if __name__ == "__main__":
    SQLITE_DB_PATH = r"db\file_metadata.db"
    DATA_DIRECTORY = r"data"

    manager = ChromaDBManager(sqlite_db_path=SQLITE_DB_PATH)
    manager.check_for_new_files(DATA_DIRECTORY)
