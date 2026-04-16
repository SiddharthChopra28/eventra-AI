from flask import Flask
# ... your existing imports ...

from agents.calling_agent import TwilioAgent  # ← add this

app = Flask(__name__)

# ... all your existing routes ...

twilio_agent = TwilioAgent(app)

if __name__ == "__main__":
    # Optionally trigger a call on startup:
    twilio_agent.make_call()
    app.run(host="0.0.0.0", port=5000, use_reloader=False)