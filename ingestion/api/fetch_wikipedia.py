import requests
import json
import logging
from bs4 import BeautifulSoup
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Wikipedia-API-Ingestion")

def fetch_driver_wiki_summary(wiki_url):
    """
    Fetches the Wikipedia summary for a driver given their wiki_url.
    Example URL: http://en.wikipedia.org/wiki/Lewis_Hamilton
    """
    if not wiki_url or "wikipedia" not in wiki_url:
        return None
        
    try:
        # Extract page title from URL
        page_title = wiki_url.split("/wiki/")[-1]
        
        # Call Wikipedia Action API
        api_url = f"https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "format": "json",
            "titles": page_title,
            "prop": "extracts",
            "exintro": True,
            "explaintext": True,
        }
        
        logger.info(f"Fetching summary for {page_title}")
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Parse JSON payload
        pages = data.get("query", {}).get("pages", {})
        for page_id, page_info in pages.items():
            if "extract" in page_info:
                summary = page_info["extract"]
                # Clean up newlines and spaces
                summary = re.sub(r'\s+', ' ', summary).strip()
                return summary
                
        return None
    except Exception as e:
        logger.error(f"Error fetching wiki summary for {wiki_url}: {e}")
        return None

if __name__ == "__main__":
    # Test JSON parsing & API integration
    test_url = "http://en.wikipedia.org/wiki/Lewis_Hamilton"
    summary = fetch_driver_wiki_summary(test_url)
    print(f"Summary for Lewis Hamilton:\n{summary}")
