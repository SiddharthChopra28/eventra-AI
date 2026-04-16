from dotenv import load_dotenv
import os

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
NGROK_HOST = os.environ.get("NGROK_HOST")
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")

TWILIO_FROM = os.environ.get("TWILIO_FROM")
TWILIO_TO = os.environ.get("TWILIO_TO")

