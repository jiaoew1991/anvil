"""Anvil Python bindings - single-queue multi-consumer work queue."""

# Import Rust implementations
from anvil_py.anvil_py import (  # type: ignore
    BrokerConfig as _BrokerConfig,
)
from anvil_py.anvil_py import (
    BrokerError as _BrokerError,
)
from anvil_py.anvil_py import (
    AnvilBroker as _AnvilBroker,
)
from anvil_py.anvil_py import (
    AnvilStorageReader as _AnvilStorageReader,
)
from anvil_py.anvil_py import (
    AnvilRustClient as _AnvilRustClient,
)
from anvil_py.anvil_py import (
    RustMessage as _RustMessage,
)

# Re-export for better IDE support
BrokerConfig = _BrokerConfig
BrokerError = _BrokerError
AnvilBroker = _AnvilBroker
AnvilStorageReader = _AnvilStorageReader
AnvilRustClient = _AnvilRustClient
RustMessage = _RustMessage


class BrokerEventHandler:
    """Base class for broker event callbacks.

    Subclass this and override methods to handle broker lifecycle events.

    Example:
        class MyHandler(BrokerEventHandler):
            def on_started(self, port: int) -> None:
                print(f"Broker started on port {port}")

            def on_fatal(self, error: BrokerError) -> None:
                print(f"Fatal error: {error}")

        handler = MyHandler()
        broker = AnvilBroker(config, event_handler=handler)
        broker.start()
    """

    def on_started(self, port: int) -> None:
        """Called when broker successfully starts.

        Args:
            port: The actual port the broker is listening on
        """
        pass

    def on_stopped(self) -> None:
        """Called when broker stops normally."""
        pass

    def on_error(self, error: "BrokerError") -> None:
        """Called when a recoverable error occurs.

        Args:
            error: The error that occurred
        """
        pass

    def on_fatal(self, error: "BrokerError") -> None:
        """Called when a fatal error occurs (broker will crash).

        Args:
            error: The error that occurred
        """
        pass


__all__ = [
    "BrokerConfig",
    "BrokerError",
    "AnvilBroker",
    "AnvilStorageReader",
    "AnvilRustClient",
    "RustMessage",
    "BrokerEventHandler",
]
