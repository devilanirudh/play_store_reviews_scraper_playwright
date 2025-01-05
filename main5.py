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
from arq.connections import RedisSettings
import logging

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Models
class ScrapeRequest(BaseModel):
    app_id: str

class ScrapeResponse(BaseModel):
    jobId: str
    status: str
    message: str

class Settings:
    redis_settings = RedisSettings(host="localhost", port=6379)
    

# Initialize Redis pool
@app.on_event("startup")
async def startup():
    global redis
    redis = await create_pool(Settings.redis_settings)

@app.on_event("shutdown")
async def shutdown():
    await redis.close()

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
        await redis.enqueue_job("scrape_reviews_task", request.app_id, job_id)


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
