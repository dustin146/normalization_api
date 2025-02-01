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


# ✅ Define the Job schema (expected after normalization)
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


# ✅ Helper function: Normalize location data
def normalize_location(location: str | None):
    """Converts different location formats into city, state, country"""
    if not location:
        return None, None, "AU"  # Default to Australia

    match = re.search(r"([^,]+),?\s?([A-Z]{2})?", location)
    if match:
        city, state = match.groups()
        return city.strip() if city else None, state.strip() if state else None, "AU"

    return location, None, "AU"


# ✅ Helper function: Normalize salary data
def normalize_salary(salary_min: float | None, salary_max: float | None, currency: str | None):
    """Standardizes salary and ensures a default currency"""
    return salary_min, salary_max, currency or "AUD"


# ✅ Helper function: Clean job description (strip HTML)
def clean_text(text: str | None):
    """Removes HTML tags and normalizes text"""
    if not text:
        return None
    return html.unescape(re.sub(r"<.*?>", "", text))


# ✅ Helper function: Generate a hash for deduplication
def generate_job_hash(company_name: str, job_title: str, location_city: str | None):
    """Creates a unique hash based on company, job title, and location"""
    base_string = f"{company_name.lower()}_{job_title.lower()}_{location_city.lower() if location_city else ''}"
    return hashlib.md5(base_string.encode()).hexdigest()


# ✅ Helper function: Get or create company ID
def get_or_create_company(company_name: str, company_website: str | None):
    """Checks if a company exists, otherwise inserts it and returns company_id"""
    existing_company = supabase.table("companies").select("company_id").eq("company_name", company_name).execute()

    if existing_company.data:
        return existing_company.data[0]["company_id"]

    response = supabase.table("companies").insert({"company_name": company_name, "company_website": company_website}).execute()
    return response.data[0]["company_id"]


# ✅ Process job route: Normalize raw data before inserting
@app.post("/process_job")
def process_job(job: dict):
    """Handles raw job postings, normalizes fields, and stores them in Supabase."""

    # ✅ Field Mapping: Standardize Key Names Based on Source
    source = job.get("source")

    job_data = {
        "job_id": job.get("id") or job.get("job_id") or job.get("job_link") or job.get("jobUrl"),
        "source": source,
        "job_title": job.get("title") or job.get("jobTitle") or job.get("position"),
        "company_name": job.get("company") or job.get("companyName") or job.get("advertiser", {}).get("name"),
        "company_website": job.get("company_url") or job.get("companyWebsite") or job.get("advertiser", {}).get("website"),
        "job_url": job.get("job_link") or job.get("jobUrl") or job.get("jobLink"),
        "location": job.get("location") if isinstance(job.get("location"), str) else f"{job.get('location', {}).get('city', '')}, {job.get('location', {}).get('state', '')}",
        "salary_min": job.get("salary_min") or job.get("compensation", {}).get("min") or job.get("payRange", {}).get("min"),
        "salary_max": job.get("salary_max") or job.get("compensation", {}).get("max") or job.get("payRange", {}).get("max"),
        "currency": job.get("currency") or job.get("compensation", {}).get("currency") or "AUD",
        "date_published": job.get("date_posted") or job.get("postedDate") or job.get("published"),
        "contact_email": job.get("contact_email")
    }

    # ✅ Normalize Location
    job_data["location_city"], job_data["location_state"], job_data["location_country"] = normalize_location(job_data["location"])

    # ✅ Normalize Salary
    job_data["salary_min"], job_data["salary_max"], job_data["currency"] = normalize_salary(job_data["salary_min"], job_data["salary_max"], job_data["currency"])

    # ✅ Get or Create Company ID
    job_data["company_id"] = get_or_create_company(job_data["company_name"], job_data["company_website"])

    # ✅ Generate `normalized_hash`
    job_data["normalized_hash"] = generate_job_hash(job_data["company_name"], job_data["job_title"], job_data["location_city"])

    # ✅ Check for Duplicate Jobs
    existing_job = supabase.table("jobs").select("job_id").eq("normalized_hash", job_data["normalized_hash"]).execute()

    if existing_job.data:
        supabase.table("job_duplicates").insert({
            "original_job_id": existing_job.data[0]['job_id'],
            "duplicate_job_id": job_data["job_id"],
            "match_score": 1.0
        }).execute()
        return {"message": "Duplicate job detected and logged", "job_id": job_data["job_id"]}

    # ✅ Insert New Job
    supabase.table("jobs").insert(job_data).execute()
    return {"message": "Job stored successfully", "job_id": job_data["job_id"]}


# ✅ Ensure FastAPI runs on Railway's assigned port
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
