import unittest
import uuid

from app.security import (
    create_access_token,
    create_refresh_token,
    create_task_event_token,
    decode_task_event_token,
    decode_token,
    hash_password,
    verify_password,
)


class PasswordHashTests(unittest.TestCase):
    def test_hash_and_verify_success(self) -> None:
        raw = "UnitTestPass123!"
        password_hash = hash_password(raw)
        self.assertTrue(verify_password(raw, password_hash))
        self.assertNotEqual(raw, password_hash)

    def test_verify_failure_with_wrong_password(self) -> None:
        password_hash = hash_password("UnitTestPass123!")
        self.assertFalse(verify_password("WrongPass123!", password_hash))

    def test_create_access_token_accepts_uuid_user_id(self) -> None:
        user = {
            "id": uuid.uuid4(),
            "email": "owner@example.com",
            "role": "owner",
            "tenant_id": "default",
        }
        token = create_access_token(user)
        payload = decode_token(token)
        self.assertEqual(str(user["id"]), payload["sub"])

    def test_task_event_token_is_task_scoped(self) -> None:
        token = create_task_event_token(
            user_id="00000000-0000-0000-0000-000000000001",
            tenant_id="default",
            task_id="00000000-0000-0000-0000-000000000002",
            ttl_seconds=60,
        )
        payload = decode_task_event_token(token)
        self.assertEqual("task_event", payload["type"])
        self.assertEqual("00000000-0000-0000-0000-000000000002", payload["task_id"])

    def test_refresh_tokens_are_unique_even_when_issued_back_to_back(self) -> None:
        user = {
            "id": uuid.uuid4(),
            "email": "user@example.com",
            "role": "user",
            "tenant_id": "default",
        }
        token_1, _ = create_refresh_token(user)
        token_2, _ = create_refresh_token(user)
        self.assertNotEqual(token_1, token_2)

        payload_1 = decode_token(token_1)
        payload_2 = decode_token(token_2)
        self.assertEqual("refresh", payload_1["type"])
        self.assertEqual("refresh", payload_2["type"])
        self.assertTrue(payload_1.get("jti"))
        self.assertTrue(payload_2.get("jti"))
        self.assertNotEqual(payload_1["jti"], payload_2["jti"])


if __name__ == "__main__":
    unittest.main()
