import datetime
import json
import pyodbc
from typing import Optional, Callable


def initialize(cls, config, database, table):
    """
    Initialize MSSQL connection.

    Args:
        config: Configuration dictionary
        database: Database name
        table: Table name
    """
    server = config.get("mssql_server", "localhost")
    database_name = config.get("mssql_database", database)
    username = config.get("mssql_username", None)
    password = config.get("mssql_password", None)
    driver = config.get("mssql_driver", "{ODBC Driver 17 for SQL Server}")

    return cls(server, database_name, username, password, driver, table)


class MSSQLMixin:
    def __init__(self, server="localhost", database="scrapyd", username=None, password=None,
                 driver="{ODBC Driver 17 for SQL Server}", table="queue"):
        self.server = server
        self.database = database
        self.table = table

        # Build connection string
        if username and password:
            connection_string = (
                f"DRIVER={driver};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )
        else:
            # Use Windows Authentication
            connection_string = (
                f"DRIVER={driver};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )

        self.conn = pyodbc.connect(connection_string, autocommit=False)

    def __len__(self):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, obj):
        return json.loads(obj)


class MSSQLJsonPriorityQueue(MSSQLMixin):
    """
    MSSQL priority queue with brand and references support.

    .. versionadded:: 1.0.0
    """

    def __init__(self, server="localhost", database="scrapyd", username=None, password=None,
                 driver="{ODBC Driver 17 for SQL Server}", table="queue"):
        super().__init__(server, database, username, password, driver, table)

        # Create table if not exists
        cursor = self.conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
            CREATE TABLE {table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                priority FLOAT NOT NULL,
                message NVARCHAR(MAX) NOT NULL,
                brand NVARCHAR(255),
                references NVARCHAR(MAX),
                created_at DATETIME2 DEFAULT GETDATE(),
                INDEX idx_priority (priority DESC)
            )
        """)
        self.conn.commit()
        cursor.close()

    def put(self, message, priority=0.0, brand=None, references=None):
        """
        Add a message to the queue with priority, brand, and references.

        Args:
            message: The message dictionary to store
            priority: Priority value (higher = more priority)
            brand: Brand identifier (optional)
            references: List of references (optional)
        """
        cursor = self.conn.cursor()

        references_json = json.dumps(references) if references else None

        cursor.execute(
            f"INSERT INTO {self.table} (priority, message, brand, references) VALUES (?, ?, ?, ?)",
            (priority, self.encode(message), brand, references_json)
        )
        self.conn.commit()
        cursor.close()

    def pop(self):
        """
        Pop the highest priority message from the queue.

        Returns:
            Dictionary with message, brand, and references, or None if queue is empty
        """
        cursor = self.conn.cursor()

        # Select and delete in a single transaction
        cursor.execute(f"""
            DELETE TOP(1) FROM {self.table}
            OUTPUT DELETED.id, DELETED.message, DELETED.brand, DELETED.references, DELETED.created_at
            WHERE id = (
                SELECT TOP 1 id FROM {self.table} WITH (UPDLOCK, READPAST)
                ORDER BY priority DESC, id ASC
            )
        """)

        row = cursor.fetchone()
        self.conn.commit()
        cursor.close()

        if row is None:
            return None

        _id, message, brand, references, created_at = row

        result = {
            "id": _id,
            "data": self.decode(message),
            "brand": brand,
            "references": json.loads(references) if references else [],
            "created_at": created_at.isoformat() if created_at else None
        }

        return result

    def remove(self, func: Callable):
        """
        Remove messages that match the given function.

        Args:
            func: Function that returns True for messages to remove

        Returns:
            Number of deleted messages
        """
        deleted = 0
        cursor = self.conn.cursor()

        cursor.execute(f"SELECT id, message, brand, references FROM {self.table}")
        rows = cursor.fetchall()

        for row in rows:
            _id, message, brand, references = row
            msg_obj = {
                "id": _id,
                "data": self.decode(message),
                "brand": brand,
                "references": json.loads(references) if references else []
            }

            if func(msg_obj):
                cursor.execute(f"DELETE FROM {self.table} WHERE id = ?", (_id,))
                deleted += 1

        self.conn.commit()
        cursor.close()
        return deleted

    def clear(self):
        """Clear all messages from the queue."""
        cursor = self.conn.cursor()
        cursor.execute(f"DELETE FROM {self.table}")
        self.conn.commit()
        cursor.close()

    def __iter__(self):
        """
        Iterate over all messages in priority order.

        Yields:
            Tuple of (message_dict, priority)
        """
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT message, priority, brand, references, created_at FROM {self.table} ORDER BY priority DESC, id ASC"
        )

        for row in cursor:
            message, priority, brand, references, created_at = row
            msg_obj = {
                "data": self.decode(message),
                "brand": brand,
                "references": json.loads(references) if references else [],
                "created_at": created_at.isoformat() if created_at else None
            }
            yield (msg_obj, priority)

        cursor.close()


class MSSQLFinishedJobs(MSSQLMixin):
    """
    MSSQL finished jobs storage.

    .. versionadded:: 1.3.0
    """

    def __init__(self, server="localhost", database="scrapyd", username=None, password=None,
                 driver="{ODBC Driver 17 for SQL Server}", table="finished_jobs"):
        super().__init__(server, database, username, password, driver, table)

        # Create table if not exists
        cursor = self.conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
            CREATE TABLE {table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                project NVARCHAR(255) NOT NULL,
                spider NVARCHAR(255) NOT NULL,
                job NVARCHAR(255) NOT NULL,
                start_time DATETIME2 NOT NULL,
                end_time DATETIME2 NOT NULL,
                INDEX idx_end_time (end_time DESC)
            )
        """)
        self.conn.commit()
        cursor.close()

    def add(self, job):
        """
        Add a finished job to storage.

        Args:
            job: Job object with project, spider, job, start_time, end_time attributes
        """
        cursor = self.conn.cursor()
        cursor.execute(
            f"INSERT INTO {self.table} (project, spider, job, start_time, end_time) VALUES (?, ?, ?, ?, ?)",
            (job.project, job.spider, job.job, job.start_time, job.end_time)
        )
        self.conn.commit()
        cursor.close()

    def clear(self, finished_to_keep: Optional[int] = None):
        """
        Clear finished jobs, optionally keeping the most recent ones.

        Args:
            finished_to_keep: Number of most recent jobs to keep, or None to delete all
        """
        cursor = self.conn.cursor()

        if finished_to_keep:
            limit = len(self) - finished_to_keep
            if limit <= 0:
                cursor.close()
                return  # Nothing to delete

            cursor.execute(f"""
                DELETE FROM {self.table}
                WHERE id IN (
                    SELECT TOP ({limit}) id FROM {self.table}
                    ORDER BY end_time ASC
                )
            """)
        else:
            cursor.execute(f"DELETE FROM {self.table}")

        self.conn.commit()
        cursor.close()

    def __iter__(self):
        """
        Iterate over all finished jobs in reverse chronological order.

        Yields:
            Tuple of (project, spider, job, start_time, end_time)
        """
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT project, spider, job, start_time, end_time FROM {self.table} ORDER BY end_time DESC"
        )

        for row in cursor:
            yield tuple(row)

        cursor.close()
