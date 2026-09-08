import os
import requests
import psycopg2
import logging
import json
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Images-Ingestion")

# Mock function for downloading images in the lab
def fetch_driver_image(driver_name, wiki_url):
    """
    Given a driver name and wiki url, pretend to scrape and download the profile image.
    Stores the metadata in the Staging schema (Phase 24).
    """
    logger.info(f"Downloading profile image for {driver_name}...")
    
    # In a real scenario we would parse the Wikipedia page for the main infobox image
    # For this academic lab, we simply log the mock URL and save it to staging DB.
    mock_image_url = f"https://example.com/images/drivers/{driver_name.replace(' ', '_').lower()}.jpg"
    
    # Save to PostgreSQL staging schema
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO staging.images (entity_type, entity_id, entity_name, image_url, image_source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    image_url = EXCLUDED.image_url,
                    retrieved_at = CURRENT_TIMESTAMP
            """, (
                "driver", 
                driver_name.lower().replace(" ", "-"), 
                driver_name, 
                mock_image_url, 
                "Wikipedia"
            ))
            conn.commit()
            logger.info(f"Successfully staged image metadata for {driver_name}")
    except Exception as e:
        logger.error(f"Failed to stage image metadata: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fetch_driver_image("Lewis Hamilton", "http://en.wikipedia.org/wiki/Lewis_Hamilton")
