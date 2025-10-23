import datetime
import json
import redis
from typing import Optional, Callable
from twisted.logger import Logger

log = Logger()

def initialize(cls, config, database, table):
    """
    Initialize Redis connection.

    Args:
        config: Configuration dictionary
        database: Database name (used as key prefix)
        table: Table name (used as key prefix)
    """
    host = config.get("redis_host", "localhost")
    port = config.get("redis_port", 6379)
    db = config.get("redis_db", 0)
    password = config.get("redis_password", "")

    return cls(host, port, db, password, database, table)


class RedisMixin:
    def __init__(self, host="localhost", port=6379, db=0, password=None, database="default", table="queue"):
        self.database = database
        self.table = table
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=False
        )
        self.key_prefix = f"{database}:{table}"

    def __len__(self):
        return self.redis_client.zcard(self.key_prefix)

    def encode(self, obj):
        return json.dumps(obj).encode("utf-8")

    def decode(self, obj):
        if isinstance(obj, bytes):
            return json.loads(obj.decode("utf-8"))
        return json.loads(obj)


class RedisJsonPriorityQueue(RedisMixin):
    """
    Redis priority queue using sorted sets.

    Includes additional fields: brand and references list.

    .. versionadded:: 1.0.0
    """

    def __init__(self, host="localhost", port=6379, db=0, password=None, database="default", table="queue"):
        super().__init__(host, port, db, password, database, table)
        self.counter_key = f"{self.key_prefix}:counter"

    def put(self, message, priority=0.0, brand=None, references=None):
        """
        Add a message to the queue with priority, brand, and references.

        Args:
            message: The message dictionary to store
            priority: Priority value (higher = more priority)
            brand: Brand identifier (optional)
            references: List of references (optional)
        """
        # Generate unique ID
        msg_id = self.redis_client.incr(self.counter_key)
        # Enhance message with metadata
        enhanced_message = {
            "id": msg_id,
            "brand": brand,
            "references": references or [],
            "created_at": datetime.datetime.now().isoformat(),
            **message
        }
        log.info(f"Adding message to queue: {enhanced_message}")

        # Use negative priority for DESC ordering (Redis sorts ascending)
        self.redis_client.zadd(
            self.key_prefix,
            {self.encode(enhanced_message): -priority}
        )

    def pop(self):
        """
        Pop the highest priority message from the queue.

        Returns:
            Dictionary with message, brand, and references, or None if queue is empty
        """
        # Get the highest priority item (lowest score due to negative priority)
        result = self.redis_client.zpopmin(self.key_prefix, 1)

        if not result:
            return None

        message_bytes, _ = result[0]
        return self.decode(message_bytes)

    def remove(self, func: Callable):
        """
        Remove messages that match the given function.

        Args:
            func: Function that returns True for messages to remove

        Returns:
            Number of deleted messages
        """
        deleted = 0

        # Get all messages
        all_messages = self.redis_client.zrange(self.key_prefix, 0, -1)

        for message_bytes in all_messages:
            message = self.decode(message_bytes)
            if func(message):
                self.redis_client.zrem(self.key_prefix, message_bytes)
                deleted += 1

        return deleted

    def clear(self):
        """Clear all messages from the queue."""
        self.redis_client.delete(self.key_prefix)
        self.redis_client.delete(self.counter_key)

    def __iter__(self):
        """
        Iterate over all messages in priority order.

        Yields:
            Tuple of (message, priority)
        """
        # Get all messages with scores (reversed priority)
        for message_bytes, score in self.redis_client.zrange(self.key_prefix, 0, -1, withscores=True):
            yield self.decode(message_bytes), -score


class RedisFinishedJobs(RedisMixin):
    """
    Redis finished jobs storage using hashes and sorted sets.

    .. versionadded:: 1.3.0
    """

    def __init__(self, host="localhost", port=6379, db=0, password=None, database="default", table="finished_jobs"):
        super().__init__(host, port, db, password, database, table)
        self.counter_key = f"{self.key_prefix}:counter"
        self.sorted_set_key = f"{self.key_prefix}:sorted"

    def add(self, job):
        """
        Add a finished job to storage.

        Args:
            job: Job object with project, spider, job, start_time, end_time attributes
        """
        job_id = self.redis_client.incr(self.counter_key)

        job_data = {
            "id": job_id,
            "project": job.project,
            "spider": job.spider,
            "job": job.job,
            "start_time": job.start_time.isoformat() if isinstance(job.start_time, datetime.datetime) else job.start_time,
            "end_time": job.end_time.isoformat() if isinstance(job.end_time, datetime.datetime) else job.end_time,
        }

        # Store job data in hash
        job_key = f"{self.key_prefix}:{job_id}"
        self.redis_client.hset(job_key, mapping={k: str(v) for k, v in job_data.items()})

        # Add to sorted set for ordering by end_time
        end_time_timestamp = job.end_time.timestamp() if isinstance(job.end_time, datetime.datetime) else \
                            datetime.datetime.fromisoformat(job.end_time).timestamp()
        self.redis_client.zadd(self.sorted_set_key, {str(job_id): end_time_timestamp})

    def clear(self, finished_to_keep: Optional[int] = None):
        """
        Clear finished jobs, optionally keeping the most recent ones.

        Args:
            finished_to_keep: Number of most recent jobs to keep, or None to delete all
        """
        if finished_to_keep:
            total = self.redis_client.zcard(self.sorted_set_key)
            limit = total - finished_to_keep

            if limit <= 0:
                return  # Nothing to delete

            # Get IDs to delete (oldest jobs)
            to_delete = self.redis_client.zrange(self.sorted_set_key, 0, limit - 1)

            if to_delete:
                # Delete job hashes
                for job_id in to_delete:
                    job_key = f"{self.key_prefix}:{job_id.decode() if isinstance(job_id, bytes) else job_id}"
                    self.redis_client.delete(job_key)

                # Remove from sorted set
                self.redis_client.zremrangebyrank(self.sorted_set_key, 0, limit - 1)
        else:
            # Delete all
            job_ids = self.redis_client.zrange(self.sorted_set_key, 0, -1)
            for job_id in job_ids:
                job_key = f"{self.key_prefix}:{job_id.decode() if isinstance(job_id, bytes) else job_id}"
                self.redis_client.delete(job_key)

            self.redis_client.delete(self.sorted_set_key)
            self.redis_client.delete(self.counter_key)

    def __iter__(self):
        """
        Iterate over all finished jobs in reverse chronological order.

        Yields:
            Tuple of (project, spider, job, start_time, end_time)
        """
        # Get all job IDs in reverse order (most recent first)
        job_ids = self.redis_client.zrevrange(self.sorted_set_key, 0, -1)

        for job_id in job_ids:
            job_key = f"{self.key_prefix}:{job_id.decode() if isinstance(job_id, bytes) else job_id}"
            job_data = self.redis_client.hgetall(job_key)

            if job_data:
                yield (
                    job_data[b"project"].decode() if isinstance(job_data[b"project"], bytes) else job_data["project"],
                    job_data[b"spider"].decode() if isinstance(job_data[b"spider"], bytes) else job_data["spider"],
                    job_data[b"job"].decode() if isinstance(job_data[b"job"], bytes) else job_data["job"],
                    datetime.datetime.fromisoformat(
                        job_data[b"start_time"].decode() if isinstance(job_data[b"start_time"], bytes) else job_data["start_time"]
                    ),
                    datetime.datetime.fromisoformat(
                        job_data[b"end_time"].decode() if isinstance(job_data[b"end_time"], bytes) else job_data["end_time"]
                    ),
                )
