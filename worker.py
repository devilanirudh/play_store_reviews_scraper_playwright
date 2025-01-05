import asyncio
import logging
from arq import create_pool, Worker
from arq.connections import RedisSettings
from scraping_task import scrape_reviews_task  # Import your task
from database import get_status_pool  # Database connection pool
from datetime import datetime,timezone
import os

# Set up logging to track progress
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis settings
REDIS_SETTINGS = RedisSettings(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    password=os.getenv("REDIS_PASSWORD", None)
)

async def startup(ctx):
    logger.info("Arq worker starting...")

async def shutdown(ctx):
    logger.info("Arq worker shutting down...")

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
        logger.error(f"Error updating job status: {e}")
        return {"status": "error", "message": str(e)}

async def main():
    redis = await create_pool(REDIS_SETTINGS)

    # Worker setup to listen for incoming scraping tasks
    worker = Worker(
        functions=[scrape_reviews_task],  # Register scrape_reviews_task function
        on_startup=startup,
        on_shutdown=shutdown,
        redis_settings=REDIS_SETTINGS,
    )

    # Run the worker
    await worker.main()

if __name__ == '__main__':
    logger.info("Starting the worker...")
    asyncio.run(main())  # Start the worker loop to process jobs
