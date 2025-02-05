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
from datetime import datetime, timezone
import traceback
import json

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
    Handles multiple formats including Seek's location structure.
    Default country is AU.
    """
    if not location:
        return None, None, "AU"

    # Handle Seek's location format
    if isinstance(location, dict):
        # Check if it's a Seek location structure with 'locations' array
        if 'locations' in location and isinstance(location['locations'], list) and location['locations']:
            seek_location = location['locations'][0]
            if 'label' in seek_location:
                # Parse "Adelaide SA" into city and state
                parts = seek_location['label'].split()
                if len(parts) >= 2:
                    city = ' '.join(parts[:-1])  # Everything except the last part
                    state = parts[-1]  # Last part is the state
                    country = seek_location.get('countryCode', 'AU')
                    return city, state, country
        
        # Handle regular dictionary format
        city = location.get("city")
        state = location.get("state")
        country = location.get("country", "AU")
        return city, state, country

    # Handle string format
    if isinstance(location, str):
        match = re.search(r"([^,]+),?\s?([^,\\s]{2,})?", location)
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
        company_data = {"company_name": company_name, "company_website": company_website, "created_at": datetime.now(timezone.utc)}
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
@app.post("/process_job")
async def process_job(request: Request):
    """
    Process a job object from a webhook event.
    """
    try:
        data = await request.json()
        
        # Handle nested data structure - job data might be in body or at root
        job = data.get("body", data)
        
        # Extract source platform early to determine parsing logic
        source_platform = job.get("sourcePlatform", "").lower()
        logger.info(f"Processing job from source: {source_platform}")
        
        # Extract job_id based on source
        if source_platform == "seek":
            job_id = job.get("id") or job.get("jobLink") or job.get("applyLink")
            company_info = job.get("advertiser") or job.get("companyProfile") or {}
            company_name = company_info.get("name", "Unknown Company")
            company_website = company_info.get("website")
            job_title = job.get("title", "")
            location_info = job.get("joblocationInfo", {})
            location_city = location_info.get("suburb") or location_info.get("location")
            location_state = location_info.get("area")
            location_country = location_info.get("countryCode", "AU")
            salary_info = job.get("salary", {})
            salary_amount = salary_info.get("amount", "")
            salary_min, salary_max = parse_salary_range(salary_amount) if salary_amount else (None, None)
            job_url = job.get("jobLink") or job.get("applyLink", "")
            contacts = job.get("contacts", [])
            contact_email = next((contact["value"] for contact in contacts if contact.get("type", "").lower() == "email"), None)
        elif source_platform == "linkedin":
            job_id = job.get("id")  # LinkedIn uses 'id' field
            company_name = job.get("companyName", "Unknown Company")
            company_website = job.get("companyWebsite") or job.get("companyLinkedinUrl")
            job_title = job.get("title", "")
            location = job.get("location", "")
            location_parts = location.split(", ") if location else []
            location_city = location_parts[0] if len(location_parts) > 0 else None
            location_state = location_parts[1] if len(location_parts) > 1 else None
            location_country = location_parts[2] if len(location_parts) > 2 else "AU"
            salary_info = job.get("salaryInfo", [])
            salary_min = None
            salary_max = None
            job_url = job.get("link") or job.get("applyUrl", "")  # LinkedIn uses 'link' field
            contact_email = None  # LinkedIn typically doesn't provide contact email
        elif source_platform == "indeed":
            job_id = job.get("jobKey")  # Indeed uses 'jobKey' field
            company_name = job.get("companyName", "Unknown Company")
            company_website = job.get("companyUrl")
            job_title = job.get("title", "")
            location_info = job.get("location", {})
            location_city = location_info.get("city")
            location_state = None  # Extract from formattedAddressLong if needed
            if location_info.get("formattedAddressLong"):
                state_parts = location_info["formattedAddressLong"].split(" ")
                if len(state_parts) > 1:
                    location_state = state_parts[-1]
            location_country = location_info.get("countryCode", "AU")
            salary_min = None  # Indeed salary info needs to be parsed from description if available
            salary_max = None
            job_url = job.get("jobUrl", "")
            contact_email = None  # Indeed typically doesn't provide contact email
        else:
            # Handle original/default format
            job_id = (job.get("job_id") or job.get("jobID") or 
                     job.get("job_url") or job.get("jobUrl"))
            company_name = job.get("company_name", "Unknown Company")
            company_website = job.get("company_website")
            job_title = job.get("job_title", "")
            location_city = job.get("location_city")
            location_state = job.get("location_state")
            location_country = job.get("location_country", "AU")
            salary_min = job.get("salary_min")
            salary_max = job.get("salary_max")
            job_url = job.get("job_url") or job.get("jobUrl", "")
            contact_email = job.get("contact_email")

        if not job_id:
            logger.error("Job skipped: Missing job_id")
            raise HTTPException(status_code=400, detail="Missing job_id.")
        logger.info(f"Extracted job_id: {job_id}")

        if not job_title:
            raise HTTPException(status_code=400, detail="Missing job title")

        # Extract dates - handle both formats
        date_published = (
            job.get("listedAt") or 
            job.get("date_published") or 
            datetime.now(timezone.utc).isoformat()
        )
        
        # Create standardized job object
        processed_job = {
            "job_id": job_id,
            "source": source_platform or job.get("source", "unknown"),
            "job_title": job_title,
            "company_name": company_name,
            "company_website": company_website,
            "job_url": job_url,
            "location_city": location_city,
            "location_state": location_state,
            "location_country": location_country,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "date_published": date_published,
            "contact_email": contact_email
        }

        # Log incoming job data
        logger.info(f"Processing job: {job_title} from {company_name}")
        
        # --- Validate company info ---
        if not company_name or not isinstance(company_name, str) or not company_name.strip():
            logger.error(f"Job {job_id} skipped: Missing company_name")
            raise HTTPException(status_code=400, detail="Missing company_name; job cannot be inserted.")

        # --- Validate job URL ---
        if not job_url or not isinstance(job_url, str):
            logger.error(f"Job {job_id} skipped: Missing job_url")
            raise HTTPException(status_code=400, detail="Missing job_url; job cannot be inserted.")

        # --- Determine Location ---
        location_city, location_state, location_country = normalize_location({"city": location_city, "state": location_state, "country": location_country})
        logger.info(f"Normalized location: city={location_city}, state={location_state}, country={location_country}")

        # --- Salary, Date, and Contact ---
        salary_min, salary_max, currency = normalize_salary(salary_min, salary_max, "AUD")
        
        # --- Company & Deduplication ---
        company_id = get_or_create_company(company_name, company_website)
        if company_id is None:
            logger.error(f"Job {job_id} skipped: Invalid company data")
            raise HTTPException(status_code=400, detail="Invalid company data.")

        normalized_hash = generate_job_hash(company_name, job_title, location_city)
        
        # Check for existing job
        existing_job = supabase.table("jobs").select("*").eq("normalized_hash", normalized_hash).execute()
        if existing_job.data:
            logger.info(f"Job {job_id} already exists with hash {normalized_hash}")
            return {"message": "Job already exists", "job_id": existing_job.data[0]['job_id']}
        else:
            logger.info(f"New job found with hash {normalized_hash}")

        # If we get here, it's a new job
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
            "normalized_hash": normalized_hash,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        try:
            result = supabase.table("jobs").insert(job_data).execute()
            logger.info(f"Inserted new job with ID: {result.data[0]['job_id']}")
        except Exception as e:
            logger.error(f"Error inserting job: {e}")
            logger.error(f"Full error details: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

        return {"message": "Job processed successfully", "job_id": job_id}

    except Exception as e:
        logger.error(f"Error processing job: {str(e)}")
        logger.error(f"Full error details: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


def parse_salary_range(salary_amount: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse salary range from string.
    """
    # Implement salary range parsing logic here
    # For now, just return None for both min and max
    return None, None


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
