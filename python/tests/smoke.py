"""Round-trips one message through a broker built from the installed wheel.

Exercises the Python bindings end to end: start a broker on a temporary
file:// store, push, claim, check the delivery count, renew, ack, see a
second ack fenced, dead-letter a poison message, redrive it (into a
finished queue too) and purge it, claim from a queue group, stop, and
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
    ClaimResult,
    QueueFinishedError,
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

        def claim_one(queue):
            return lambda: client.claim(queue, batch_size=1).messages

        (message,) = wait_for(claim_one("work"))
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
        (poison,) = wait_for(claim_one("poison"))
        client.nack(
            "poison", [poison.msg_id], claim_tokens=[poison.claim_token], reason="bad input"
        )
        stats = client.get_stats("poison")["queues"]["poison"]
        assert stats["dead_count"] == 1 and stats["pending_count"] == 0, stats
        # Stats tell a queue's own limit from the broker's default in effect.
        assert (stats["max_deliveries"], stats["effective_max_deliveries"]) == (1, 1), stats
        stats = client.get_stats("work")["queues"]["work"]
        assert (stats["max_deliveries"], stats["effective_max_deliveries"]) == (None, 20), stats
        (letter,), next_start_after = client.list_dead_letters("poison")
        assert next_start_after is None
        assert (letter.msg_id, letter.cause, letter.reason) == (
            poison.msg_id,
            "nack_limit",
            "bad input",
        ), letter
        redriven = client.redrive_dead_letters("poison", [letter.msg_id, "unknown"])
        assert redriven == {"redriven": [letter.msg_id], "missing": ["unknown"]}, redriven
        (again,) = wait_for(claim_one("poison"))
        assert (again.payload, again.delivery_count) == (b"bad", 1), again
        client.nack("poison", [again.msg_id], claim_tokens=[again.claim_token], dead_letter=True)

        # A finished queue drains with its dead letter reported, and refuses
        # a redrive with an error of its own, which a worker's handler for
        # lost claims does not swallow, unless the redrive reopens it.
        client.mark_queue_finished("poison")
        result = client.claim("poison", batch_size=1)
        assert isinstance(result, ClaimResult), result
        assert (result.messages, result.drained, result.dead_count) == ([], True, 1), result
        # A claim result is read by name and cannot be changed.
        try:
            result.dead_count = 0
        except AttributeError:
            pass
        else:
            sys.exit("a claim result was changed in place")
        assert client.is_queue_finished("poison")["dead_count"] == 1
        try:
            client.redrive_dead_letters("poison", [again.msg_id])
        except ClaimLostError:
            sys.exit("a refused redrive was reported as a lost claim")
        except QueueFinishedError:
            pass
        else:
            sys.exit("a redrive into a finished queue was not refused")
        redriven = client.redrive_dead_letters("poison", [again.msg_id], reopen=True)
        assert redriven == {"redriven": [again.msg_id], "missing": []}, redriven
        assert not client.is_queue_finished("poison")["finished"]
        (third,) = wait_for(claim_one("poison"))
        client.nack("poison", [third.msg_id], claim_tokens=[third.claim_token], dead_letter=True)
        (ids,), _ = client.list_dead_letters("poison", ids_only=True)
        assert (ids.msg_id, ids.payload) == (third.msg_id, b""), ids
        purged = client.purge_dead_letters("poison", [third.msg_id])
        assert purged == {"purged": [third.msg_id], "missing": []}, purged

        # A group claim takes messages only from the partitions assigned.
        client.create_queue_group("fan", 2)
        client.push("fan_p1", b"keyed")
        result = client.claim_from_group("fan", [0], timeout_ms=0)
        assert (result.messages, result.source_partition) == ([], None), result
        result = client.claim_from_group("fan", [1])
        (keyed,) = result.messages
        assert (keyed.payload, keyed.queue, result.source_partition) == (b"keyed", "fan_p1", 1), result

        client.stop()
        broker.stop()

        reader = AnvilStorageReader(f"file://{db_dir}")
        assert reader.list_queues() == ["fan_p0", "fan_p1", "poison", "work"], reader.list_queues()
        stats = reader.get_queue_stats("poison")
        assert (stats["dead_count"], stats["total_purged"]) == (0, 1), stats
        assert stats["max_deliveries"] == 1, stats
    # A store this release cannot read raises a RuntimeError like any other
    # failure to open, so existing handlers still catch it.
    assert issubclass(StorageFormatError, RuntimeError)
    print("smoke ok")


if __name__ == "__main__":
    main()
