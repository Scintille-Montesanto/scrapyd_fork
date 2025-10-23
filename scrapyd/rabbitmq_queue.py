import datetime
import json
import pika
from typing import Optional, Callable
from collections import defaultdict


def initialize(cls, config, database, table):
    """
    Initialize RabbitMQ connection.

    Args:
        config: Configuration dictionary
        database: Database name (used as virtual host)
        table: Table name (used as queue name)
    """
    host = config.get("rabbitmq_host", "localhost")
    port = config.get("rabbitmq_port", 5672)
    username = config.get("rabbitmq_username", "guest")
    password = config.get("rabbitmq_password", "guest")
    vhost = config.get("rabbitmq_vhost", database if database != ":memory:" else "/")

    return cls(host, port, username, password, vhost, table)


class RabbitMQMixin:
    def __init__(self, host="localhost", port=5672, username="guest", password="guest",
                 vhost="/", queue_name="queue"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.queue_name = queue_name

        # Setup connection
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=credentials
        )

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def __len__(self):
        """Get the number of messages in the queue."""
        method = self.channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
        return method.method.message_count

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, obj):
        if isinstance(obj, bytes):
            return json.loads(obj.decode("utf-8"))
        return json.loads(obj)

    def close(self):
        """Close the connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitMQJsonPriorityQueue(RabbitMQMixin):
    """
    RabbitMQ priority queue with brand and references support.

    Uses RabbitMQ's native priority queue feature (max priority: 10).

    .. versionadded:: 1.0.0
    """

    def __init__(self, host="localhost", port=5672, username="guest", password="guest",
                 vhost="/", queue_name="queue"):
        super().__init__(host, port, username, password, vhost, queue_name)

        # Declare priority queue with max priority of 10
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={"x-max-priority": 10}
        )

    def put(self, message, priority=0.0, brand=None, references=None):
        """
        Add a message to the queue with priority, brand, and references.

        Args:
            message: The message dictionary to store
            priority: Priority value (0-10, higher = more priority)
            brand: Brand identifier (optional)
            references: List of references (optional)
        """
        # Normalize priority to 0-10 range
        normalized_priority = max(0, min(10, int(priority)))

        # Enhance message with metadata
        enhanced_message = {
            "data": message,
            "brand": brand,
            "references": references or [],
            "created_at": datetime.datetime.now().isoformat()
        }

        # Publish message with priority
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=self.encode(enhanced_message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                priority=normalized_priority
            )
        )

    def pop(self):
        """
        Pop the highest priority message from the queue.

        Returns:
            Dictionary with message, brand, and references, or None if queue is empty
        """
        method_frame, header_frame, body = self.channel.basic_get(
            queue=self.queue_name,
            auto_ack=True
        )

        if method_frame is None:
            return None

        return self.decode(body)

    def remove(self, func: Callable):
        """
        Remove messages that match the given function.

        Warning: This requires consuming all messages and re-queuing non-matching ones.
        This operation is expensive and may affect performance.

        Args:
            func: Function that returns True for messages to remove

        Returns:
            Number of deleted messages
        """
        deleted = 0
        messages_to_keep = []

        # Consume all messages
        while True:
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=True
            )

            if method_frame is None:
                break

            message = self.decode(body)
            if func(message):
                deleted += 1
            else:
                # Keep message with its original priority
                priority = header_frame.priority if header_frame.priority is not None else 0
                messages_to_keep.append((message, priority))

        # Re-queue messages that should be kept
        for message, priority in messages_to_keep:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=self.encode(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    priority=priority
                )
            )

        return deleted

    def clear(self):
        """Clear all messages from the queue."""
        self.channel.queue_purge(queue=self.queue_name)

    def __iter__(self):
        """
        Iterate over all messages in priority order.

        Warning: This consumes messages from the queue and re-queues them.
        This operation is expensive and may affect performance.

        Yields:
            Tuple of (message, priority)
        """
        messages = []

        # Consume all messages
        while True:
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=True
            )

            if method_frame is None:
                break

            message = self.decode(body)
            priority = header_frame.priority if header_frame.priority is not None else 0
            messages.append((message, priority))

        # Sort by priority (descending)
        messages.sort(key=lambda x: x[1], reverse=True)

        # Re-queue all messages
        for message, priority in messages:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=self.encode(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    priority=priority
                )
            )
            yield (message, priority)


class RabbitMQFinishedJobs(RabbitMQMixin):
    """
    RabbitMQ finished jobs storage.

    Note: RabbitMQ is not ideal for job storage. This implementation uses an in-memory
    structure combined with a durable queue for persistence. For production use,
    consider using a database backend instead.

    .. versionadded:: 1.3.0
    """

    def __init__(self, host="localhost", port=5672, username="guest", password="guest",
                 vhost="/", queue_name="finished_jobs"):
        super().__init__(host, port, username, password, vhost, queue_name)

        # Declare durable queue for finished jobs
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # In-memory cache for iteration
        self._cache = []
        self._cache_loaded = False

    def _load_cache(self):
        """Load all jobs from the queue into cache."""
        if self._cache_loaded:
            return

        self._cache = []

        # Consume all messages without ack
        while True:
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=False
            )

            if method_frame is None:
                break

            job_data = self.decode(body)
            self._cache.append((method_frame.delivery_tag, job_data))

            # Requeue the message
            self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)

        # Sort by end_time (descending)
        self._cache.sort(
            key=lambda x: x[1]["end_time"],
            reverse=True
        )

        self._cache_loaded = True

    def add(self, job):
        """
        Add a finished job to storage.

        Args:
            job: Job object with project, spider, job, start_time, end_time attributes
        """
        job_data = {
            "project": job.project,
            "spider": job.spider,
            "job": job.job,
            "start_time": job.start_time.isoformat() if isinstance(job.start_time, datetime.datetime) else job.start_time,
            "end_time": job.end_time.isoformat() if isinstance(job.end_time, datetime.datetime) else job.end_time,
        }

        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=self.encode(job_data),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )

        # Invalidate cache
        self._cache_loaded = False

    def clear(self, finished_to_keep: Optional[int] = None):
        """
        Clear finished jobs, optionally keeping the most recent ones.

        Args:
            finished_to_keep: Number of most recent jobs to keep, or None to delete all
        """
        if finished_to_keep is None:
            # Delete all
            self.channel.queue_purge(queue=self.queue_name)
            self._cache = []
            self._cache_loaded = False
            return

        # Load all jobs
        self._load_cache()

        total = len(self._cache)
        limit = total - finished_to_keep

        if limit <= 0:
            return  # Nothing to delete

        # Purge queue
        self.channel.queue_purge(queue=self.queue_name)

        # Re-queue only the jobs to keep (most recent ones)
        for _, job_data in self._cache[:finished_to_keep]:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=self.encode(job_data),
                properties=pika.BasicProperties(delivery_mode=2)
            )

        # Update cache
        self._cache = [(None, job) for _, job in self._cache[:finished_to_keep]]

    def __iter__(self):
        """
        Iterate over all finished jobs in reverse chronological order.

        Yields:
            Tuple of (project, spider, job, start_time, end_time)
        """
        self._load_cache()

        for _, job_data in self._cache:
            yield (
                job_data["project"],
                job_data["spider"],
                job_data["job"],
                datetime.datetime.fromisoformat(job_data["start_time"]),
                datetime.datetime.fromisoformat(job_data["end_time"]),
            )

    def __len__(self):
        """Get the number of finished jobs."""
        method = self.channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
        return method.method.message_count
