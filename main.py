import os
import uvicorn
import hashlib
import re
import html
from fastapi import FastAPI, Request
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


# ✅ Helper function: Normalize location data (Handles string & dictionary formats)
def normalize_location(location):
    """Converts location data into city, state, country. Handles both string and object formats."""
    if not location:
        return None, None, "AU"  # Default to Australia

    # ✅ If location is a dictionary (LinkedIn format)
    if isinstance(location, dict):
        city = location.get("city")
        state = location.get("state")
        country = location.get("country", "AU")  # Default to AU if missing
        return city, state, country

    # ✅ If location is a string, parse it (e.g., "Sydney, NSW")
    if isinstance(location, str):
        match = re.search(r"([^,]+),?\s?([A-Z]{2})?", location)
        if match:
            city, state = match.groups()
            return city.strip() if city else None, state.strip() if state else None, "AU"

    # 🚨 If format is unknown, return raw data safely
    return location, None, "AU"


# ✅ Helper function: Normalize salary data
def normalize_salary(salary_min, salary_max, currency):
    """Standardizes salary and ensures a default currency"""
    return salary_min, salary_max, currency or "AUD"


# ✅ Helper function: Clean job description (strip HTML)
def clean_text(text):
    """Removes HTML tags and normalizes text"""
    if not text:
        return None
    return html.unescape(re.sub(r"<.*?>", "", text))


# ✅ Helper function: Generate a hash for deduplication
def generate_job_hash(company_name, job_title, location_city):
    """Creates a unique hash based on company, job title, and location"""
    base_string = f"{company_name.lower()}_{job_title.lower()}_{location_city.lower() if location_city else ''}"
    return hashlib.md5(base_string.encode()).hexdigest()


# ✅ Helper function: Get or create company ID
def get_or_create_company(company_name, company_website):
    """Checks if a company exists, otherwise inserts it and returns company_id."""

    # 🚨 Handle missing company_name - Log it and return None
    if not company_name or company_name.strip() == "":
        print("⚠️ WARNING: Missing company_name. Job will not be inserted.")
        return None  # Do NOT insert a NULL company_name

    # ✅ Check if company already exists
    existing_company = supabase.table("companies").select("company_id").eq("company_name", company_name).execute()

    if existing_company.data:
        return existing_company.data[0]["company_id"]

    # ✅ Insert new company (only if company_name is valid)
    response = supabase.table("companies").insert({"company_name": company_name, "company_website": company_website}).execute()
    return response.data[0]["company_id"]


def extract_seek_location(job):
    if job.get("sourcePlatform") == "SEEK" and "locations" in job:
        locations = job["locations"]
        if locations and isinstance(locations, list):
            location = locations[0]  # Assume the first location
            label = location.get("label", "")
            country_code = location.get("countryCode", "AU")

            # Extract city from seoHierarchy if available
            seo_hierarchy = location.get("seoHierarchy", [])
            city = seo_hierarchy[0].get("contextualName", "") if seo_hierarchy else label

            return f"{city}, {country_code}"

    return None

@app.post("/process_job")
async def process_job(request: Request):
    """Handles raw job postings, normalizes fields, and stores them in Supabase."""

    job = await request.json()  # ✅ Read JSON explicitly

    # ✅ Extract and Normalize Job Data
    job_id = job.get("job_id") or job.get("id") or job.get("job_link") or job.get("jobUrl")

    # ✅ Ensure source is always set
    sourcePlatform = job.get("sourcePlatform") or job.get("platform") or "Unknown"

    # ✅ Prioritize `shortTitle` when source is LinkedIn
    if sourcePlatform.lower() == "linkedin":
        job_title = job.get("shortTitle") or job.get("title") or job.get("job_title") or job.get("jobTitle")
    else:
        job_title = job.get("job_title") or job.get("title") or job.get("jobTitle") or job.get("position")

    # ✅ Fix company_name extraction (Handles both string & dict)
    company = job.get("company") or {}
    company_name = job.get("company_name") or job.get("company") or job.get("companyName") or company.get("name")
    company_website = job.get("company_website") or job.get("company_url") or job.get("companyWebsite") or company.get(
        "url")

    # 🚨 Ensure `company_name` is a **string** before calling `.strip()`
    if isinstance(company_name, dict):
        company_name = company_name.get("name")  # Extract from dictionary if needed

    if not company_name or not isinstance(company_name, str) or company_name.strip() == "":
        return {"error": "Missing company_name, job cannot be inserted."}

    # ✅ Fix job_url extraction (Ensures it's always present)
    job_url = job.get("job_url") or job.get("job_link") or job.get("jobUrl") or job.get("jobLink") or job.get("url")

    # 🚨 Prevent inserting null `job_url`
    if not job_url or not isinstance(job_url, str):
        return {"error": "Missing job_url, job cannot be inserted."}

    # ✅ Extract location
    seek_location = extract_seek_location(job)
    if seek_location:
        location = seek_location
    else:
        location = job.get("location") or f"{job.get('location_city', '')}, {job.get('location_state', '')}"

    # ✅ Normalize Location
    location_city, location_state, location_country = normalize_location(location)

    salary_min = job.get("salary_min") or job.get("compensation", {}).get("min") or job.get("payRange", {}).get("min")
    salary_max = job.get("salary_max") or job.get("compensation", {}).get("max") or job.get("payRange", {}).get("max")
    currency = job.get("currency") or job.get("compensation", {}).get("currency") or "AUD"
    date_published = job.get("datePublished") or job.get("datePosted") or job.get("postedDate") or job.get(
        "published") or job.get("listingDate")
    contact_email = job.get("contact_email")



    # ✅ Normalize Salary
    salary_min, salary_max, currency = normalize_salary(salary_min, salary_max, currency)

    # ✅ Get or Create Company ID
    company_id = get_or_create_company(company_name, company_website)

    # ✅ Generate `normalized_hash`
    normalized_hash = generate_job_hash(company_name, job_title, location_city)

    # 🚨 **Check if `job_id` already exists before inserting**
    existing_job = supabase.table("jobs").select("job_id").eq("job_id", job_id).execute()

    if existing_job.data:
        # ✅ **Log duplicate instead of failing**
        supabase.table("job_duplicates").insert({
            "original_job_id": existing_job.data[0]['job_id'],
            "duplicate_job_id": job_id,
            "match_score": 1.0
        }).execute()

        return {"message": "Duplicate job_id detected and logged", "job_id": job_id}

    # ✅ Insert New Job
    job_data = {
        "job_id": job_id,
        "source": sourcePlatform,
        "job_title": job_title,  # ✅ Always prioritizing `shortTitle` for LinkedIn
        "company_id": company_id,
        "job_url": job_url,
        "location_city": location_city,
        "location_state": location_state,
        "location_country": location_country,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "currency": currency,
        "date_published": date_published,
        "contact_email": contact_email,
        "normalized_hash": normalized_hash
    }

    supabase.table("jobs").insert(job_data).execute()
    return {"message": "Job stored successfully", "job_id": job_id}


# ✅ Ensure FastAPI runs on Railway's assigned port
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
