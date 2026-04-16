from langchain_core.tools import tool
from langchain_community.tools.tavily_search import TavilySearchResults

TAVILY_API_KEY = "YOUR_TAVILY_API_KEY_HERE"

@tool
def web_search(query: str) -> str:
    """Search the web for current information on a topic.
    Use this when the caller asks about recent events, facts, or anything
    that requires up-to-date information from the internet.

    Args:
        query: The search query to look up.

    Returns:
        A summary of the top search results.
    """
    try:
        search = TavilySearchResults(max_results=3)
        results = search.invoke(query)

        if not results:
            return "No results found for that query."

        # Format results into a clean readable string for the LLM
        formatted = []
        for i, r in enumerate(results, 1):
            title = r.get("title", "No title")
            content = r.get("content", "No content")
            url = r.get("url", "")
            formatted.append(f"[{i}] {title}\n{content}\nSource: {url}")

        return "\n\n".join(formatted)

    except Exception as e:
        return f"Search failed: {str(e)}"