import os
import uvicorn
import hashlib
from fastapi import FastAPI
from pydantic import BaseModel  # ✅ FIX: Import BaseModel
from supabase import create_client

# Load Supabase credentials from a `.env` file for local testing
from dotenv import load_dotenv

load_dotenv()  # ✅ Load environment variables from .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Debugging: Check if variables are being loaded
if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ ERROR: SUPABASE_URL or SUPABASE_KEY is missing!")

# ✅ Correct way to initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running locally and connected to Supabase!"}


# Define the Job schema for incoming job data
class Job(BaseModel):
    job_id: str
    source: str
    job_title: str
    company_name: str
    company_website: str | None = None
    job_url: str
    location_city: str | None = None
    location_state: str | None = None
    location_country: str | None = "AU"
    salary_min: float | None = None
    salary_max: float | None = None
    date_published: str
    contact_email: str | None = None


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Use Railway's assigned port
    uvicorn.run(app, host="0.0.0.0", port=port)
