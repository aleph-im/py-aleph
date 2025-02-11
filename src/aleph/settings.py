from dataclasses import dataclass, field
from threading import Lock


@dataclass(frozen=True)
class Settings:
    """Immutable configuration settings."""
    use_executors: bool = field(default=True)

    _instance = None  # Private class-level variable for Singleton instance
    _lock = Lock()  # Lock to ensure thread-safe initialization

    @classmethod
    def instance(cls):
        """
        Get the Singleton instance of the Settings class.
        Ensures only one instance is created even in multithreaded environments.
        """
        if cls._instance is None:
            with cls._lock:  # Double-checked locking
                if cls._instance is None:
                    cls._instance = cls()  # Initialize the Singleton instance
        return cls._instance


# Access the Singleton instance
settings = Settings.instance()
