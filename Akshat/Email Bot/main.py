"""
CLI entry point for the Email Bot.

Usage:
    python main.py --name "John Doe" --email "john@example.com"
"""

import argparse
import json
import sys

from email_bot import send_greeting_email


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send a greeting email to the specified recipient."
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Recipient's name (e.g. 'John Doe').",
    )
    parser.add_argument(
        "--email",
        required=True,
        help="Recipient's email address.",
    )

    args = parser.parse_args()

    result = send_greeting_email(args.name, args.email)

    # Pretty-print the structured result
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Exit with non-zero code on failure
    if result["status"] != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
