import os
import uvicorn
import hashlib
import re
import html
from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ✅ Ensure Supabase credentials exist
if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ ERROR: SUPABASE_URL or SUPABASE_KEY is missing!")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running and normalizing job data!"}


# ✅ Define the Job schema
class Job(BaseModel):
    job_id: str
    source: str
    job_title: str
    company_name: str
    company_website: str | None = None
    job_url: str
    location: str | None = None  # Incoming raw location
    salary_min: float | None = None
    salary_max: float | None = None
    currency: str | None = None
    date_published: str
    contact_email: str | None = None


# ✅ Helper function: Normalize location
def normalize_location(location: str | None):
    """Converts different location formats into city, state, country"""
    if not location:
        return None, None, "AU"  # Default to Australia

    # Handle "Sydney, NSW" or "Austin, TX"
    match = re.search(r"([^,]+),?\s?([A-Z]{2})?", location)
    if match:
        city, state = match.groups()
        return city.strip() if city else None, state.strip() if state else None, "AU"

    # Default fallback
    return location, None, "AU"


# ✅ Helper function: Normalize salary
def normalize_salary(salary_min: float | None, salary_max: float | None, currency: str | None):
    """Standardizes salary to include currency"""
    return salary_min, salary_max, currency or "AUD"  # Default to AUD


# ✅ Helper function: Clean job description (strip HTML)
def clean_text(text: str | None):
    """Removes HTML tags and normalizes text"""
    if not text:
        return None
    return html.unescape(re.sub(r"<.*?>", "", text))


# ✅ Helper function: Normalize text for deduplication
def normalize_text(text: str | None) -> str:
    return ''.join(e for e in text.lower() if e.isalnum()) if text else ""


# ✅ Helper function: Generate job hash for deduplication
def generate_job_hash(job: Job) -> str:
    base_string = f"{normalize_text(job.company_name)}_{normalize_text(job.job_title)}_{normalize_text(job.location)}"
    return hashlib.md5(base_string.encode()).hexdigest()


# ✅ Helper function: Get or insert company and return `company_id`
def get_or_create_company(company_name: str, company_website: str | None):
    """Checks if a company exists, otherwise inserts it and returns company_id"""
    existing_company = supabase.table("companies").select("company_id").eq("company_name", company_name).execute()

    if existing_company.data:
        return existing_company.data[0]["company_id"]

    # Insert new company and return ID
    company_data = {"company_name": company_name, "company_website": company_website}
    response = supabase.table("companies").insert(company_data).execute()
    return response.data[0]["company_id"]


# ✅ Process job route: Normalize & store job data
@app.post("/process_job")
def process_job(job: Job):
    """Normalizes job data, handles duplicates, and stores jobs in Supabase."""

    # ✅ Generate `normalized_hash`
    job_hash = generate_job_hash(job)

    # ✅ Normalize location
    location_city, location_state, location_country = normalize_location(job.location)

    # ✅ Normalize salary
    salary_min, salary_max, currency = normalize_salary(job.salary_min, job.salary_max, job.currency)

    # ✅ Check if job already exists
    existing_job = supabase.table("jobs").select("job_id").eq("normalized_hash", job_hash).execute()

    if existing_job.data:
        # ✅ Log duplicate in `job_duplicates`
        try:
            supabase.table("job_duplicates").insert({
                "original_job_id": existing_job.data[0]['job_id'],
                "duplicate_job_id": job.job_id,
                "match_score": 1.0
            }).execute()
            return {"message": "Duplicate job detected and logged", "job_id": job.job_id}
        except Exception as e:
            return {"error": f"Failed to log duplicate: {str(e)}"}

    # ✅ Get or create company_id
    company_id = get_or_create_company(job.company_name, job.company_website)

    # ✅ Insert new job
    job_data = {
        "job_id": job.job_id,
        "source": job.source,
        "job_title": job.job_title,
        "company_id": company_id,
        "job_url": job.job_url,
        "location_city": location_city,
        "location_state": location_state,
        "location_country": location_country,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "currency": currency,
        "date_published": job.date_published,
        "contact_email": job.contact_email,
        "normalized_hash": job_hash
    }

    try:
        supabase.table("jobs").insert(job_data).execute()
        return {"message": "Job stored successfully", "job_id": job.job_id}
    except Exception as e:
        return {"error": f"Failed to insert job: {str(e)}"}


# ✅ Ensure FastAPI runs on Railway's assigned port
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
