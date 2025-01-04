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

app = FastAPI()

# Models
class ScrapeRequest(BaseModel):
    app_id: str

class ScrapeResponse(BaseModel):
    jobId: str
    status: str
    message: str
    

# Helper function to update job status in the database
# async def update_job_status(job_id: str, status: str, error_message: str = None, total_reviews: int = None):
    
#     pool = await get_status_pool()
#     async with pool.acquire() as conn:
#         current_time = datetime.now(timezone.utc)
#         completed_at = current_time if status in ['completed', 'failed'] else None
        
#         await conn.execute('''
#             UPDATE scrape_jobs 
#             SET status = $1,
#                 error_message = $2,
#                 total_reviews = COALESCE($3, total_reviews),
#                 completed_at = $4
#             WHERE job_id = $5
#         ''', status, error_message, total_reviews, completed_at, job_id)
#         return {"status": "success", "message": "Reviews scraped successfully"}

async def update_job_status(job_id: str, status: str, error_message: str = None, total_reviews: int = None):
    """
    Updates the job status in the database. Returns a response indicating success.
    """
    try:
        # Get a connection pool
        pool = await get_status_pool()

        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            # Get the current UTC time
            current_time = datetime.now(timezone.utc)

            # Determine completion time
            completed_at = current_time if status in ['completed', 'failed'] else None

            # Update the scrape_jobs table
            await conn.execute('''
                UPDATE scrape_jobs 
                SET status = $1,
                    error_message = $2,
                    total_reviews = COALESCE($3, total_reviews),
                    completed_at = $4
                WHERE job_id = $5
            ''', status, error_message, total_reviews, completed_at, job_id)

        # Explicitly return a success response
        return {"status": "success", "message": "Job status updated successfully"}

    except Exception as e:
        # Log the error (optional) and return a failure response
        print(f"Error updating job status: {e}")
        return {"status": "error", "message": str(e)}


# Route for initiating the scraping task
@app.post("/scrape", response_model=ScrapeResponse)
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    try:
        job_id = str(uuid4())
        current_time = datetime.now(timezone.utc)

        # Initialize job in the database
        pool = await get_status_pool()
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO scrape_jobs (
                    job_id, app_id, status, started_at, created_at
                ) VALUES ($1, $2, $3, $4, $5)
            ''', job_id, request.app_id, "pending", current_time, current_time)

        # Add background task for scraping
        background_tasks.add_task(scrape_reviews_task, request.app_id, job_id)

        return ScrapeResponse(
            jobId=job_id,
            status="started",
            message="Review scraping has started"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/scrape/{job_id}/status")
async def get_scrape_status(job_id: str):
    """Get the current status of a scraping job"""
    try:
        pool = await get_status_pool()
        async with pool.acquire() as conn:
            job = await conn.fetchrow('''
                SELECT 
                    job_id, status, started_at, completed_at, 
                    total_reviews, error_message
                FROM scrape_jobs 
                WHERE job_id = $1
            ''', job_id)
            
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            
            return dict(job)
                    
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Error checking job status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Background task to scrape reviews and update job status
async def scrape_reviews_task(app_id: str, job_id: str):
    try:
        # Update job status to "in_progress"
        await update_job_status(job_id, "pending")

        # Scrape HTML content
        html = await scrape_play_store_html(app_id)

        # Parse reviews (you can add your parse logic here, if needed)
        reviews  = await extract_reviews_from_html(html)
        print(len(reviews)) 

        # Insert reviews into the database
        await insert_reviews_into_db(app_id, reviews)
        print("done")

        # Update job status to "completed"
        done = await update_job_status(job_id, "completed", total_reviews=len(reviews))
        print(done)
    except Exception as e:
        # Update job status to "failed" with error message
        await update_job_status(job_id, "failed", error_message=str(e))






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
