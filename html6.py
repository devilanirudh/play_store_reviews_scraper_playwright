from bs4 import BeautifulSoup
import json
import asyncio
from play9 import scrape_play_store_html
from typing import List, Dict

async def extract_reviews_from_html(html: str) -> List[Dict[str, str]]:
    """
    Extracts reviews from the provided HTML string and returns them as a list of dictionaries.
    Each dictionary contains review details like username, content, score, thumbs-up count, reviewed date, and replied content.
    
    :param html: The HTML content as a string.
    :return: A list of dictionaries containing review information.
    """
    soup = BeautifulSoup(html, 'html.parser')

    reviews = []

    # First loop for extracting username and content
    for user_div in soup.find_all('div', class_="X5PpBb"):
        user_name = user_div.text.strip()  # Extract user's name
        review_content_div = user_div.find_next('div', class_="h3YV2d")  # Locate the corresponding review content
        review_content = review_content_div.text.strip() if review_content_div else None

        # Add an entry to the reviews list
        reviews.append({
            "username": user_name,
            "content": review_content,
            "score": None,  # Placeholder for score
            "thumbsupcount": None,  # Placeholder for thumbs-up count
            "reviewedat": None,  # Placeholder for reviewed date
            "repliedcontent": None  # Placeholder for replied content
        })

    # Second loop for extracting score, thumbs-up count, reviewed date, and replied content
    for i, container in enumerate(soup.find_all('div', class_="Jx4nYe")):
        if i < len(reviews):  # Ensure data matches with existing review entries
            # Extract score
            score_div = container.find('div', class_="iXRFPc")
            score = score_div["aria-label"].split()[1] if score_div and "aria-label" in score_div.attrs else "0"

            # Extract thumbs-up count
            thumbs_up_div = container.find('div', jscontroller="wW2D8b")
            thumbs_up_count = thumbs_up_div.text.strip() if thumbs_up_div else None

            # Extract reviewed date
            reviewed_at_span = container.find('span', class_="bp9Aid")
            reviewed_at = reviewed_at_span.text.strip() if reviewed_at_span else None

            # Extract replied content
            replied_content_div = container.find('div', class_="ras4vb")
            replied_content = replied_content_div.get_text(strip=True) if replied_content_div else None

            # Update the corresponding review entry
            reviews[i].update({
                "score": score,
                "thumbsupcount": thumbs_up_count,
                "reviewedat": reviewed_at,
                "repliedcontent": replied_content
            })

    return reviews

