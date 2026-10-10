import unittest
import uuid
from datetime import datetime

class EventEnvelope:
    def __init__(self, event_type, order_id, payload, event_id=None, version="1.0"):
        self.event_id = event_id or str(uuid.uuid4())
        self.event_type = event_type
        self.order_id = order_id
        self.payload = payload
        self.occurred_at = datetime.utcnow().isoformat()
        self.version = version

class TestIdempotency(unittest.TestCase):
    def test_event_envelope_creation(self):
        envelope = EventEnvelope(
            event_type="OrderCreated",
            order_id="ord-999",
            payload={"pickup": [22.5, 88.3], "weight": 2.5}
        )
        self.assertIsNotNone(envelope.event_id)
        self.assertEqual(envelope.event_type, "OrderCreated")
        self.assertEqual(envelope.order_id, "ord-999")
        self.assertEqual(envelope.version, "1.0")
        self.assertEqual(envelope.payload["weight"], 2.5)

    def test_idempotency_token_consistency(self):
        fixed_id = "evt-unique-abc-123"
        envelope1 = EventEnvelope(
            event_id=fixed_id,
            event_type="OrderCreated",
            order_id="ord-1",
            payload={"amount": 100}
        )
        envelope2 = EventEnvelope(
            event_id=fixed_id,
            event_type="OrderCreated",
            order_id="ord-1",
            payload={"amount": 100}
        )
        self.assertEqual(envelope1.event_id, envelope2.event_id)

if __name__ == "__main__":
    unittest.main()
