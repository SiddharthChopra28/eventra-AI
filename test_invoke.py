from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from config import GEMINI_API_KEY
from tools import ALL_TOOLS
from langgraph.prebuilt import create_react_agent

llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=GEMINI_API_KEY,
    temperature=0.7,
)

agent = create_react_agent(llm, tools=ALL_TOOLS, prompt="You are a helpful voice assistant.")
try:
    print("Invoking agent...")
    res = agent.invoke({"messages": [HumanMessage(content="Hello!")]})
    print(res["messages"][-1].content)
except Exception as e:
    import traceback
    traceback.print_exc()

