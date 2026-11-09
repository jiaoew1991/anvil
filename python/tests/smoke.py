"""Round-trips one message through a broker built from the installed wheel.

Exercises the Python bindings end to end: start a broker on a temporary
file:// store, push, claim, check the delivery count, renew, ack, see a
second ack fenced, dead-letter a poison message and redrive it, stop, and
reopen the store, which now carries the format marker, directly.
"""

import sys
import tempfile
import time

from anvil_py import (
    AnvilBroker,
    AnvilRustClient,
    AnvilStorageReader,
    BrokerConfig,
    ClaimLostError,
    StorageFormatError,
)

DEADLINE_SECS = 30.0


def wait_for(poll):
    deadline = time.monotonic() + DEADLINE_SECS
    while time.monotonic() < deadline:
        value = poll()
        if value:
            return value
        time.sleep(0.05)
    sys.exit(f"timed out after {DEADLINE_SECS}s waiting for {poll.__name__}")


def main():
    with tempfile.TemporaryDirectory() as db_dir:
        broker = AnvilBroker(BrokerConfig(f"file://{db_dir}", host="127.0.0.1"))
        broker.start()
        # The broker binds asynchronously; the port is unset until it listens.
        wait_for(broker.get_port)

        client = AnvilRustClient(broker.get_broker_url(), "smoke-worker")
        client.start()
        client.create_queue("work")
        client.push("work", b"payload")

        def claim_one():
            messages, _drained = client.claim("work", batch_size=1)
            return messages

        (message,) = wait_for(claim_one)
        assert message.payload == b"payload", message.payload
        assert message.delivery_count == 1, message.delivery_count
        client.renew("work", [message.msg_id], [message.claim_token])
        client.ack("work", [message.msg_id], claim_tokens=[message.claim_token])
        # The message is settled, so a second ack is fenced: a worker must
        # be able to tell this from a broker failure it would retry.
        try:
            client.ack("work", [message.msg_id], claim_tokens=[message.claim_token])
        except ClaimLostError:
            pass
        else:
            sys.exit("a second ack of a settled message was not fenced")

        # A poison message dies at the queue's limit, is listed with the
        # reason of its last nack, and is redriven as a first delivery.
        client.create_queue("poison", max_deliveries=1)
        client.push("poison", b"bad")
        (poison,) = wait_for(lambda: client.claim("poison", batch_size=1)[0])
        client.nack(
            "poison", [poison.msg_id], claim_tokens=[poison.claim_token], reason="bad input"
        )
        stats = client.get_stats("poison")["queues"]["poison"]
        assert stats["dead_count"] == 1 and stats["pending_count"] == 0, stats
        (letter,) = client.list_dead_letters("poison")
        assert (letter.msg_id, letter.cause, letter.reason) == (
            poison.msg_id,
            "nack_limit",
            "bad input",
        ), letter
        redriven = client.redrive_dead_letters("poison", [letter.msg_id, "unknown"])
        assert redriven == {"redriven": [letter.msg_id], "missing": ["unknown"]}, redriven
        (again,) = wait_for(lambda: client.claim("poison", batch_size=1)[0])
        assert (again.payload, again.delivery_count) == (b"bad", 1), again
        client.nack("poison", [again.msg_id], claim_tokens=[again.claim_token], dead_letter=True)
        purged = client.purge_dead_letters("poison", [again.msg_id])
        assert purged == {"purged": [again.msg_id], "missing": []}, purged

        client.stop()
        broker.stop()

        reader = AnvilStorageReader(f"file://{db_dir}")
        assert reader.list_queues() == ["poison", "work"], reader.list_queues()
        assert reader.get_queue_stats("poison")["dead_count"] == 0
    # A store this release cannot read raises a RuntimeError like any other
    # failure to open, so existing handlers still catch it.
    assert issubclass(StorageFormatError, RuntimeError)
    print("smoke ok")


if __name__ == "__main__":
    main()
