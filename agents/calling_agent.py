import json
import base64
import threading
import audioop
import numpy as np
import whisper

from flask import Response
from flask_sock import Sock
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.rest import Client as TwilioClient
from piper.voice import PiperVoice

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, AIMessage

from config import GEMINI_API_KEY, NGROK_HOST, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM, TWILIO_TO
from tools import ALL_TOOLS

SYSTEM_PROMPT = """You are a helpful voice assistant on a phone call.
Keep your responses concise and conversational — this is spoken audio, not text.
Avoid bullet points, markdown, or long paragraphs.
Respond naturally as if speaking to the caller."""

class TwilioAgent:
    def __init__(self, app):
        """
        Pass in the existing Flask app instance — no new Flask app is created.
        """
        self.app = app
        self.sock = Sock(app)  # attaches flask-sock to your existing app

        self.tw_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

        # Piper TTS
        self.global_piper_voice = PiperVoice.load("agents/static/en_US-danny-low.onnx")

        # Whisper STT
        print("Loading Whisper model...")
        self.whisper_model = whisper.load_model("base")
        print("Whisper model loaded.")

        # LangChain Gemini agent
        print("Setting up LangChain Gemini agent...")
        self.agent_executor = self._build_agent()
        print("Agent ready.")

        self._register_routes()

    def _build_agent(self):
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=GEMINI_API_KEY,
            temperature=0.7,
        )
        # Reference: https://langchain-ai.github.io/langgraph/how-tos/create-react-agent/
        agent = create_react_agent(
            llm, 
            tools=ALL_TOOLS, 
            prompt=SYSTEM_PROMPT
        )
        return agent

    def _register_routes(self):
        # HTTP route — uses self.app directly
        self.app.add_url_rule(
            "/twilio-webhook",
            view_func=self.twilio_webhook,
            methods=["POST"]
        )
        # WebSocket route — uses self.sock
        self.sock.route("/audiostream")(self.audio_stream)

    def twilio_webhook(self):
        response = VoiceResponse()
        connect = Connect()
        connect.stream(url=f"wss://{NGROK_HOST}/audiostream")
        response.append(connect)
        return Response(str(response), mimetype='text/xml')

    def make_call(self):
        print("Dialing the number...")
        call = self.tw_client.calls.create(
            to=TWILIO_TO,
            from_=TWILIO_FROM,
            url=f"https://{NGROK_HOST}/twilio-webhook"
        )
        print(f"Call initiated. SID: {call.sid}")

    def audio_stream(self, ws):
        print("WebSocket Connection Opened!")
        stream_sid = None
        audio_buffer = bytearray()
        chat_history = []
        CHUNK_THRESHOLD = 8000 * 2

        while True:
            message = ws.receive()
            if message is None:
                break

            data = json.loads(message)
            event_type = data.get("event")

            if event_type == "start":
                stream_sid = data["start"]["streamSid"]
                print(f"Stream started: {stream_sid}")
                audio_buffer.clear()
                chat_history.clear()

            elif event_type == "media":
                mulaw_bytes = base64.b64decode(data["media"]["payload"])
                audio_buffer.extend(mulaw_bytes)

                if len(audio_buffer) >= CHUNK_THRESHOLD:
                    chunk = bytes(audio_buffer)
                    audio_buffer.clear()
                    threading.Thread(
                        target=self.transcribe_and_respond,
                        args=(chunk, stream_sid, ws, chat_history),
                        daemon=True
                    ).start()

            elif event_type == "stop":
                print("Stream closed.")
                break

    def transcribe_and_respond(self, mulaw_bytes, stream_sid, ws, chat_history):
        try:
            pcm_8k = audioop.ulaw2lin(mulaw_bytes, 2)
            pcm_16k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 16000, None)
            audio_np = np.frombuffer(pcm_16k, dtype=np.int16).astype(np.float32) / 32768.0

            print("Transcribing...")
            result = self.whisper_model.transcribe(audio_np, language="en", fp16=False)
            transcript = result["text"].strip()
            print(f"Transcript: '{transcript}'")

            if not transcript or len(transcript) == 0:
                print("Empty transcript detected. Skipping LLM call.")
                return

            chat_history.append(HumanMessage(content=transcript))

            # https://langchain-ai.github.io/langgraph/how-tos/create-react-agent/
            agent_result = self.agent_executor.invoke({
                "messages": chat_history,
            })
            
            # The result dict contains the updated "messages" list
            updated_messages = agent_result["messages"]
            
            # The final AI response
            llm_response = ""
            if updated_messages and isinstance(updated_messages[-1], AIMessage):
                llm_response = updated_messages[-1].content.strip()
                
            print(f"LLM Response: '{llm_response}'")

            # Update history to retain full conversation state
            chat_history.clear()
            chat_history.extend(updated_messages)

            reply_audio = self.generate_twilio_base64_audio(llm_response)
            if reply_audio and stream_sid:
                ws.send(json.dumps({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {"payload": reply_audio}
                }))

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"ERROR in transcribe_and_respond: {e}")

    def generate_twilio_base64_audio(self, text: str) -> str:
        try:
            if hasattr(self.global_piper_voice, 'synthesize'):
                raw_pcm_bytes = b"".join(
                    chunk.audio_int16_bytes
                    for chunk in self.global_piper_voice.synthesize(text)
                )
            elif hasattr(self.global_piper_voice, 'synthesize_stream_raw'):
                raw_pcm_bytes = b"".join(self.global_piper_voice.synthesize_stream_raw(text))
            else:
                return ""
        except Exception as e:
            print(f"ERROR in Piper: {e}")
            return ""

        if not raw_pcm_bytes:
            return ""

        try:
            downsampled, _ = audioop.ratecv(
                raw_pcm_bytes, 2, 1,
                self.global_piper_voice.config.sample_rate, 8000, None
            )
            mulaw = audioop.lin2ulaw(downsampled, 2)
            return base64.b64encode(mulaw).decode('utf-8')
        except Exception as e:
            print(f"ERROR in audio conversion: {e}")
            return ""