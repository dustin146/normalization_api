import os
from supabase import create_client

# Load Supabase credentials from Railway environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Debugging: Check if variables are being loaded
if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ ERROR: SUPABASE_URL or SUPABASE_KEY is missing!")

# ✅ Correct way to initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running on Railway and connected to Supabase!"}


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


# Helper function: Normalize text for deduplication
def normalize_text(text):
    return ''.join(e for e in text.lower() if e.isalnum())


# Helper function: Generate job hash for deduplication
def generate_job_hash(job: Job):
    base_string = f"{normalize_text(job.company_name)}_{normalize_text(job.job_title)}_{normalize_text(job.location_city or '')}"
    return hashlib.md5(base_string.encode()).hexdigest()


# Helper function: Check if job already exists
def check_duplicate(job_hash):
    response = supabase.table("jobs").select("job_id").eq("normalized_hash", job_hash).execute()
    return response.data if response.data else None


# API Route: Process & Deduplicate Jobs
@app.post("/process_job")
def process_job(job: Job):
    job_hash = generate_job_hash(job)
    duplicate = check_duplicate(job_hash)

    if duplicate:
        supabase.table("job_duplicates").insert({
            "original_job_id": duplicate[0]['job_id'],
            "duplicate_job_id": job.job_id,
            "match_score": 1.0
        }).execute()
        return {"message": "Duplicate job detected", "job_id": job.job_id}

    # Insert new job into Supabase
    job_data = job.dict()
    job_data["normalized_hash"] = job_hash
    supabase.table("jobs").insert(job_data).execute()

    return {"message": "Job stored successfully", "job_id": job.job_id}
