import socket
import unittest
from unittest.mock import patch

from app.tool_gateway import ToolGateway


class ToolGatewayEgressTests(unittest.TestCase):
    def test_blocks_private_ip_after_dns_resolution(self) -> None:
        gateway = ToolGateway()
        with patch.object(gateway, "_resolve_host_ips", return_value=["10.1.2.3"]):
            with self.assertRaises(PermissionError) as exc:
                gateway._enforce_egress("api.example.com", {"allow_domains": ["example.com"]})
        self.assertEqual("EGRESS_DNS_PRIVATE_IP", str(exc.exception))

    def test_allowlist_supports_subdomains(self) -> None:
        gateway = ToolGateway()
        with patch.object(gateway, "_resolve_host_ips", return_value=["93.184.216.34"]):
            gateway._enforce_egress("api.example.com", {"allow_domains": ["example.com"]})

    def test_dns_cache_avoids_repeat_resolution_within_ttl(self) -> None:
        gateway = ToolGateway()
        fake_answer = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        with patch("app.tool_gateway.socket.getaddrinfo", return_value=fake_answer) as mocked:
            first = gateway._resolve_host_ips("example.com")
            second = gateway._resolve_host_ips("example.com")

        self.assertEqual(["93.184.216.34"], first)
        self.assertEqual(first, second)
        self.assertEqual(1, mocked.call_count)

    def test_dns_rebinding_private_ip_is_blocked(self) -> None:
        gateway = ToolGateway()
        with patch.object(gateway, "_resolve_host_ips", return_value=["10.0.0.1"]):
            with self.assertRaises(PermissionError) as exc:
                gateway._enforce_egress("evil.example.com", {"allow_domains": ["example.com"]})
        self.assertEqual("EGRESS_DNS_PRIVATE_IP", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
