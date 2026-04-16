"""
Unit tests for email_bot.py

Run:  python -m pytest test_email_bot.py -v
"""

import unittest
from unittest.mock import patch, MagicMock
import smtplib

from email_bot import validate_email, build_greeting_email, send_greeting_email


class TestValidateEmail(unittest.TestCase):
    """Tests for validate_email()."""

    def test_valid_emails(self):
        valid = [
            "user@example.com",
            "first.last@domain.org",
            "name+tag@sub.domain.co",
            "a@b.cd",
        ]
        for addr in valid:
            with self.subTest(addr=addr):
                self.assertTrue(validate_email(addr))

    def test_invalid_emails(self):
        invalid = [
            "",
            "plainstring",
            "@no-local.com",
            "no-at-sign.com",
            "user@",
            "user@.com",
            "user@domain",
            None,
            123,
        ]
        for addr in invalid:
            with self.subTest(addr=addr):
                self.assertFalse(validate_email(addr))


class TestBuildGreetingEmail(unittest.TestCase):
    """Tests for build_greeting_email()."""

    def test_content_structure(self):
        result = build_greeting_email("Alice", "Bot")
        self.assertIn("subject", result)
        self.assertIn("body", result)

    def test_personalisation(self):
        result = build_greeting_email("Bob", "MyBot")
        self.assertEqual(result["subject"], "Greeting")
        self.assertIn("Hi Bob,", result["body"])
        self.assertIn("MyBot", result["body"])

    def test_exact_body_format(self):
        result = build_greeting_email("Charlie", "Sender")
        expected_body = (
            "Hi Charlie,\n"
            "\n"
            "I hope you're doing well. Just wanted to send a quick greeting!\n"
            "\n"
            "Best regards,\n"
            "Sender"
        )
        self.assertEqual(result["body"], expected_body)


class TestSendGreetingEmail(unittest.TestCase):
    """Tests for send_greeting_email() with mocked SMTP."""

    @patch("email_bot.smtplib.SMTP")
    def test_success(self, mock_smtp_cls):
        """Happy path: SMTP send succeeds → status 'success'."""
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = send_greeting_email("Alice", "alice@example.com")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["email_content"]["subject"], "Greeting")
        self.assertIn("Alice", result["email_content"]["body"])
        self.assertNotIn("error", result)

    def test_invalid_email_returns_failure(self):
        """Bad email → instant failure, no SMTP call."""
        result = send_greeting_email("Bob", "not-an-email")

        self.assertEqual(result["status"], "failure")
        self.assertIn("error", result)
        self.assertIn("Invalid email", result["error"])

    def test_empty_name_returns_failure(self):
        """Empty recipient name → failure."""
        result = send_greeting_email("", "ok@example.com")

        self.assertEqual(result["status"], "failure")
        self.assertIn("recipient_name", result["error"])

    @patch("email_bot.smtplib.SMTP")
    def test_smtp_auth_failure(self, mock_smtp_cls):
        """SMTP auth error → graceful failure."""
        mock_server = MagicMock()
        mock_server.login.side_effect = smtplib.SMTPAuthenticationError(
            535, b"Bad credentials"
        )
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = send_greeting_email("Eve", "eve@example.com")

        self.assertEqual(result["status"], "failure")
        self.assertIn("authentication", result["error"].lower())

    @patch("email_bot.smtplib.SMTP")
    def test_network_error(self, mock_smtp_cls):
        """Network timeout → graceful failure."""
        mock_smtp_cls.side_effect = OSError("Connection timed out")

        result = send_greeting_email("Dave", "dave@example.com")

        self.assertEqual(result["status"], "failure")
        self.assertIn("Network error", result["error"])

    @patch("email_bot.smtplib.SMTP")
    def test_does_not_modify_inputs(self, mock_smtp_cls):
        """Verify input strings are not mutated."""
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        name = "Zara"
        email = "zara@example.com"
        name_before, email_before = name, email

        send_greeting_email(name, email)

        self.assertEqual(name, name_before)
        self.assertEqual(email, email_before)


if __name__ == "__main__":
    unittest.main()
