import os
import uvicorn
import hashlib
from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ ERROR: SUPABASE_URL or SUPABASE_KEY is missing!")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running locally and connected to Supabase!"}

# add an api to fetch stored jobs
@app.get("/jobs")
def get_jobs():
    try:
        response = supabase.table("jobs").select("*").execute()
        return response
    except Exception as e:
        print("❌ ERROR fetching jobs:", e)
        return {"error": str(e)}

#add duplicat job detection and logging



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

# Helper function: Normalize text for deduplication
def normalize_text(text):
    return ''.join(e for e in text.lower() if e.isalnum()) if text else ""

# Helper function: Generate job hash for deduplication
def generate_job_hash(job):
    base_string = f"{normalize_text(job.company_name)}_{normalize_text(job.job_title)}_{normalize_text(job.location_city)}"
    return hashlib.md5(base_string.encode()).hexdigest()

@app.post("/process_job")
def process_job(job: Job):
    """Detects duplicate jobs across job boards and stores them in Supabase."""

    # ✅ Generate `normalized_hash`
    job_hash = generate_job_hash(job)

    # ✅ Check if job exists in `jobs` table
    existing_job = supabase.table("jobs").select("job_id").eq("normalized_hash", job_hash).execute()

    if existing_job.data:
        # ✅ Log duplicate in `job_duplicates`
        supabase.table("job_duplicates").insert({
            "original_job_id": existing_job.data[0]['job_id'],
            "duplicate_job_id": job.job_id,
            "match_score": 1.0
        }).execute()

        return {"message": "Duplicate job detected", "job_id": job.job_id}

    # ✅ Insert new job
    job_data = job.dict()
    job_data["normalized_hash"] = job_hash

    try:
        supabase.table("jobs").insert(job_data).execute()
        return {"message": "Job stored successfully", "job_id": job.job_id}
    except Exception as e:
        return {"error": str(e)}



if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Use Railway's assigned port
    uvicorn.run(app, host="0.0.0.0", port=port)