import asyncpg
from config import settings

review_pool = None
status_pool = None

async def init_db():
    global review_pool, status_pool
    
    print(f"Attempting to connect with URL: {settings.DATABASE_URL}")
    
    if not settings.DATABASE_URL:
        raise ValueError("DATABASE_URL is not set in environment variables")
        
    if not settings.DATABASE_URL.startswith(('postgresql://', 'postgres://')):
        raise ValueError(f"Invalid DATABASE_URL format. Must start with postgresql:// or postgres://")
    
    review_pool = await asyncpg.create_pool(settings.DATABASE_URL)
    status_pool = await asyncpg.create_pool(settings.DATABASE_URL)

async def close_db():
    if review_pool:
        await review_pool.close()
    if status_pool:
        await status_pool.close()

async def get_review_pool():
    if review_pool is None:
        await init_db()
    return review_pool

async def get_status_pool():
    if status_pool is None:
        await init_db()
    return status_pool