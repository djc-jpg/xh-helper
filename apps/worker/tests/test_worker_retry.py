import unittest

from worker import _next_retry_sleep


class RetryBackoffTests(unittest.TestCase):
    def test_retry_before_60_seconds_uses_progressive_backoff(self) -> None:
        sleep_s, next_delay = _next_retry_sleep(previous_delay_s=2.0, elapsed_s=12.0)
        self.assertEqual(2.0, sleep_s)
        self.assertEqual(4.0, next_delay)

    def test_retry_after_60_seconds_uses_stable_10s_sleep(self) -> None:
        sleep_s, next_delay = _next_retry_sleep(previous_delay_s=8.0, elapsed_s=61.0)
        self.assertEqual(10.0, sleep_s)
        self.assertEqual(10.0, next_delay)

    def test_backoff_is_capped_at_10_seconds(self) -> None:
        sleep_s, next_delay = _next_retry_sleep(previous_delay_s=15.0, elapsed_s=20.0)
        self.assertEqual(10.0, sleep_s)
        self.assertEqual(10.0, next_delay)


if __name__ == "__main__":
    unittest.main()

