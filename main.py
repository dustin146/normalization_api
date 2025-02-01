import os
import uvicorn
import hashlib
import re
import html
import logging
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional, Tuple, Dict, Any, Union

# --- Setup logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load environment variables ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL or SUPABASE_KEY is missing!")
    raise Exception("Missing Supabase credentials!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running and normalizing job data!"}


# --- Job Schema ---
class Job(BaseModel):
    job_id: str
    source: str
    job_title: str
    company_name: str
    company_website: Optional[str] = None
    job_url: str
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    location_country: Optional[str] = "AU"
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    date_published: str
    contact_email: Optional[str] = None


# --- Helper Functions ---
def normalize_location(location: Union[str, Dict[str, Any], None]) -> Tuple[Optional[str], Optional[str], str]:
    """
    Convert location data into (city, state, country).
    Default country is AU.
    """
    if not location:
        return None, None, "AU"
    if isinstance(location, dict):
        city = location.get("city")
        state = location.get("state")
        country = location.get("country", "AU")
        return city, state, country
    if isinstance(location, str):
        match = re.search(r"([^,]+),?\s?([A-Z]{2})?", location)
        if match:
            city, state = match.groups()
            return city.strip() if city else None, state.strip() if state else None, "AU"
    return location, None, "AU"


def normalize_salary(
    salary_min: Optional[float],
    salary_max: Optional[float],
    currency: Optional[str]
) -> Tuple[Optional[float], Optional[float], str]:
    """
    Standardize salary values. Defaults to AUD if currency missing.
    """
    return salary_min, salary_max, currency if currency else "AUD"


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Remove HTML tags and unescape HTML entities.
    """
    if not text:
        return None
    return html.unescape(re.sub(r"<.*?>", "", text))


def generate_job_hash(company_name: str, job_title: str, location_city: Optional[str]) -> str:
    """
    Create a unique hash from company, job title, and city.
    """
    base_string = f"{company_name.lower()}_{job_title.lower()}_{location_city.lower() if location_city else ''}"
    return hashlib.md5(base_string.encode()).hexdigest()


def get_or_create_company(company_name: str, company_website: Optional[str]) -> Optional[int]:
    """
    Return company_id if found, or insert a new record and return its company_id.
    """
    if not company_name.strip():
        logger.warning("Missing company_name. Cannot insert job.")
        return None

    try:
        existing_company = supabase.table("companies") \
            .select("company_id") \
            .eq("company_name", company_name) \
            .execute()
    except Exception as e:
        logger.error(f"Error checking company: {e}")
        raise HTTPException(status_code=500, detail="Database error on company lookup.")

    if existing_company.data:
        return existing_company.data[0]["company_id"]

    try:
        response = supabase.table("companies") \
            .insert({"company_name": company_name, "company_website": company_website}) \
            .execute()
        return response.data[0]["company_id"]
    except Exception as e:
        logger.error(f"Error inserting company: {e}")
        raise HTTPException(status_code=500, detail="Database error on company insertion.")


def extract_seek_location(job: Dict[str, Any]) -> Optional[str]:
    """
    If job source is SEEK, extract the location from its locations list.
    """
    if job.get("sourcePlatform") == "SEEK" and "locations" in job:
        locations = job["locations"]
        if isinstance(locations, list) and locations:
            location = locations[0]
            label = location.get("label", "")
            country_code = location.get("countryCode", "AU")
            return f"{label}, {country_code}"
    return None


# --- Main Endpoint ---
<<<<<<<<<<<<<<  ✨ Codeium Command 🌟 >>>>>>>>>>>>>>>>
@app.post("/process_job")
async def process_job(request: Request):
    """
    Process a job object from a webhook event
    """
    try:
        data = await request.json()

        job = data.get("body", data)

        job_id = (job.get("job_id") or job.get("id") or
                  job.get("job_link") or job.get("jobUrl") or
                  job.get("jobID") or job.get("job_url"))

        if not job_id:
            raise HTTPException(status_code=400, detail="Missing job_id.")

        # Log the extracted job_id for debugging
        logger.info(f"Extracted job_id: {job_id}")


    except json.JSONDecodeError as e:

        logger.error(f"JSON Decode Error: {str(e)}")

        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    except Exception as e:

        logger.error(f"Error processing job: {str(e)}")

        raise HTTPException(status_code=500, detail="Internal server error")
<<<<<<<  fabe5489-15e2-40e5-bae8-1d2be2c909ea  >>>>>>>

    # --- Extract essential fields ---

    source_platform = (job.get("sourcePlatform") or job.get("platform") or "Unknown").strip()

    if source_platform.lower() == "linkedin":
        job_title = job.get("shortTitle") or job.get("title") or job.get("job_title") or job.get("jobTitle")
    else:
        job_title = job.get("job_title") or job.get("title") or job.get("jobTitle") or job.get("position")
    if not job_title:
        raise HTTPException(status_code=400, detail="Missing job_title.")

    # --- Extract company info ---
    company_info = job.get("company") or {}
    company_name = (
        job.get("company_name")
        or job.get("company")
        or job.get("companyName")
        or (company_info.get("name") if isinstance(company_info, dict) else None)
    )
    company_website = (
        job.get("company_website")
        or job.get("company_url")
        or job.get("companyWebsite")
        or (company_info.get("url") if isinstance(company_info, dict) else None)
    )
    if isinstance(company_name, dict):
        company_name = company_name.get("name")
    if not company_name or not isinstance(company_name, str) or not company_name.strip():
        raise HTTPException(status_code=400, detail="Missing company_name; job cannot be inserted.")

    # --- Validate job URL ---
    job_url = (
        job.get("job_url")
        or job.get("job_link")
        or job.get("jobUrl")
        or job.get("jobLink")
        or job.get("url")
    )
    if not job_url or not isinstance(job_url, str):
        raise HTTPException(status_code=400, detail="Missing job_url; job cannot be inserted.")

    # --- Determine Location ---
    seek_location = extract_seek_location(job)
    location = seek_location or job.get("location") or f"{job.get('location_city', '')}, {job.get('location_state', '')}"
    location_city, location_state, location_country = normalize_location(location)

    # --- Salary, Date, and Contact ---
    salary_min = job.get("salary_min") or job.get("compensation", {}).get("min") or job.get("payRange", {}).get("min")
    salary_max = job.get("salary_max") or job.get("compensation", {}).get("max") or job.get("payRange", {}).get("max")
    currency = job.get("currency") or job.get("compensation", {}).get("currency") or "AUD"
    date_published = (
        job.get("datePublished")
        or job.get("datePosted")
        or job.get("postedDate")
        or job.get("published")
        or job.get("listingDate")
    )
    contact_email = job.get("contact_email")

    salary_min, salary_max, currency = normalize_salary(salary_min, salary_max, currency)

    # --- Company & Deduplication ---
    company_id = get_or_create_company(company_name, company_website)
    if company_id is None:
        raise HTTPException(status_code=400, detail="Invalid company data.")

    normalized_hash = generate_job_hash(company_name, job_title, location_city)

    try:
        existing_job = supabase.table("jobs") \
            .select("job_id") \
            .eq("job_id", job_id) \
            .execute()
    except Exception as e:
        logger.error(f"Error checking for duplicate job: {e}")
        raise HTTPException(status_code=500, detail="Error checking job duplication.")

    if existing_job.data:
        try:
            supabase.table("job_duplicates").insert({
                "original_job_id": existing_job.data[0]["job_id"],
                "duplicate_job_id": job_id,
                "match_score": 1.0
            }).execute()
        except Exception as e:
            logger.error(f"Error logging duplicate: {e}")
        return {"message": "Duplicate job_id detected and logged", "job_id": job_id}

    # --- Insert Job ---
    job_data = {
        "job_id": job_id,
        "source": source_platform,
        "job_title": job_title,
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

    try:
        supabase.table("jobs").insert(job_data).execute()
    except Exception as e:
        logger.error(f"Error inserting job: {e}")
        raise HTTPException(status_code=500, detail="Error inserting job data.")

    return {"message": "Job stored successfully", "job_id": job_id}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
