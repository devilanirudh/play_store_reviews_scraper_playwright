from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime, timezone
from play9 import scrape_play_store_html  # Import the scraping function
from database import get_review_pool, get_status_pool  # Database connection pool imports
from bs4 import BeautifulSoup
from html6 import extract_reviews_from_html
from uuid import uuid4
import json
import ast
from arq import create_pool
from arq.connections import RedisSettings  # Scraping function
from html6 import extract_reviews_from_html  # Review extraction
from main5 import update_job_status
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def scrape_reviews_task(ctx,app_id: str, job_id: str):
    try:
        # Update job status to "in_progress"
        logger.info(f"Task {job_id} started for app {app_id} at {datetime.now()}")

        await update_job_status(job_id, "pending")
        logger.info(f"Job {job_id} status updated to 'pending'")

        # Scrape HTML content
        html = await scrape_play_store_html(app_id)
        logger.info(f"HTML scraped for app {app_id} at {datetime.now()}")

        # Parse reviews (you can add your parse logic here, if needed)
        reviews  = await extract_reviews_from_html(html)
        logger.info(f"Total reviews scraped for {app_id}: {len(reviews)}")

        # Insert reviews into the database
        await insert_reviews_into_db(app_id, reviews)
        logger.info(f"Reviews inserted into DB for app {app_id}")

        # Update job status to "completed"
        done = await update_job_status(job_id, "completed", total_reviews=len(reviews))
        logger.info(f"Job {job_id} completed at {datetime.now()} with {len(reviews)} reviews.")
    except Exception as e:
        # Update job status to "failed" with error message
        await update_job_status(job_id, "failed", error_message=str(e))
        logger.error(f"Job {job_id} status updated to 'failed' due to error: {e}")






async def insert_reviews_into_db(app_id: str, reviews: list):
    """
    Inserts reviews into the database. Handles null values by using default values.
    
    :param app_id: The app ID to associate the reviews with.
    :param reviews: The list of reviews to insert.
    :param job_id: Job identifier for tracking.
    """
    try:
        pool = await get_review_pool()  # Assumed to return a connection pool
        async with pool.acquire() as conn:
            async with conn.transaction():
                for review in reviews:
                    # Handle null or missing fields with default values
                    username = str(review.get("username", "Anonymous"))
                    content = str(review.get("content", "No review content provided"))
                    
                    # Ensure score is an integer and handle invalid cases (e.g., score being a string)
                    score = review.get("score", 0)
                    if isinstance(score, str):
                        try:
                            score = int(score)  # Convert to int if it's a string
                        except ValueError:
                            score = 0  # Default to 0 if conversion fails
                    
                    # Handle thumbsupcount being None or invalid
                    thumbs_up_count = review.get("thumbsupcount", 0)
                    if thumbs_up_count is None:
                        thumbs_up_count = 0  # Default to 0 if None
                    
                    # Handle reviewed_at date string and parse it if necessary
                    reviewed_at = review.get("reviewedat")
                    if reviewed_at:
                        try:
                            # Parse reviewed_at (e.g., 'January 3, 2025' -> %B %d, %Y)
                            reviewed_at = datetime.strptime(reviewed_at, "%B %d, %Y")
                        except ValueError:
                            reviewed_at = None  # Default to None if parsing fails
                    
                    # Handle the case where 'repliedcontent' is None
                    replied_content = review.get("repliedcontent", "No reply content")
                    if replied_content is None:
                        replied_content = "No reply content"

                    # Generate a unique review ID
                    review_id = str(uuid4())

                    # Insert or update the review in the database
                    await conn.execute('''
                        INSERT INTO reviews (
                            app_id, review_id, user_name, content, 
                            score, thumbs_up_count, reviewed_at, 
                            reply_content, replied_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (review_id) 
                        DO UPDATE SET
                            content = EXCLUDED.content,
                            score = EXCLUDED.score,
                            thumbs_up_count = EXCLUDED.thumbs_up_count,
                            reply_content = EXCLUDED.reply_content,
                            replied_at = EXCLUDED.replied_at
                    ''',
                    app_id,
                    review_id,  # Unique review ID
                    username[:255],  # Truncate username to fit database constraints
                    content[:10000],  # Truncate content to fit database constraints
                    score,
                    thumbs_up_count,
                    reviewed_at,
                    replied_content[:10000],  # Truncate reply content
                    None)  # No replied_at provided
        print(f"Successfully stored reviews for app_id {app_id}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting reviews: {str(e)}")
