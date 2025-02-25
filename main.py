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
    Handles minor name variations and checks company website to prevent duplicates.
    """
    if not company_name or not isinstance(company_name, str):
        logger.warning(f"Invalid company_name: {company_name}")
        return None

    company_name = company_name.strip()
    if not company_name:
        logger.warning("Empty company_name after stripping whitespace")
        return None

    # Normalize company name by removing common suffixes
    normalized_name = re.sub(r"\b(Inc|LLC|Ltd|Pty Ltd|Corp|Co)\.?$", "", company_name, flags=re.IGNORECASE).strip()

    try:
        # Step 1: Check for exact company name match (case insensitive)
        query = supabase.table("companies") \
            .select("company_id, company_name, company_website") \
            .ilike("company_name", company_name) \
            .execute()

        if not query.data:
            # Step 2: Check for normalized name match
            query = supabase.table("companies") \
                .select("company_id, company_name, company_website") \
                .ilike("company_name", normalized_name) \
                .execute()

        # If we find a match by name, return it
        if query.data:
            company = query.data[0]
            if company_website and company["company_website"]:
                if company_website.strip().lower() == company["company_website"].strip().lower():
                    logger.info(f"Found existing company by website match: {company['company_id']}")
                    return company["company_id"]
            else:
                logger.info(f"Found existing company by name match: {company['company_id']}")
                return company["company_id"]

        # Step 3: If no name match, check for website match
        if company_website:
            query = supabase.table("companies") \
                .select("company_id") \
                .ilike("company_website", company_website.strip()) \
                .execute()

            if query.data:
                logger.info(f"Found existing company by website: {query.data[0]['company_id']}")
                return query.data[0]["company_id"]

        # Step 4: If no match found, create a new company
        logger.info(f"Creating new company: {company_name}")
        company_data = {
            "company_name": company_name,
            "company_website": company_website.strip() if company_website else None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        response = supabase.table("companies") \
            .insert(company_data) \
            .execute()

        if not response.data:
            logger.error("Company insertion returned no data")
            raise Exception("Company insertion failed - no data returned")

        new_company_id = response.data[0]["company_id"]
        logger.info(f"Successfully created company with ID: {new_company_id}")
        return new_company_id

    except Exception as e:
        logger.error(f"Error in get_or_create_company: {str(e)}")
        logger.error(f"Full error details: {traceback.format_exc()}")
        logger.error(f"Company data being processed: name='{company_name}', website='{company_website}'")
        
        # If it's a duplicate key error, try to fetch the existing record
        if isinstance(e, Exception) and "duplicate key value" in str(e):
            try:
                query = supabase.table("companies") \
                    .select("company_id") \
                    .eq("company_name", company_name) \
                    .execute()
                if query.data:
                    logger.info(f"Recovered existing company ID after duplicate error: {query.data[0]['company_id']}")
                    return query.data[0]["company_id"]
            except Exception as recovery_error:
                logger.error(f"Failed to recover from duplicate error: {str(recovery_error)}")

        raise HTTPException(
            status_code=500, 
            detail=f"Database error on company insertion. Error: {str(e)}"
        )


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
            # Log raw data for debugging
            logger.info(f"Raw Seek job data: {job}")
            
            # Get content and advertiser sections
            content = job.get("content", {}) if isinstance(job.get("content"), dict) else {}
            advertiser = job.get("advertiser", {}) if isinstance(job.get("advertiser"), dict) else {}
            
            # Extract basic job info
            job_id = str(advertiser.get("id", ""))
            company_name = str(advertiser.get("name", "Unknown Company"))
            company_website = None  # Not provided in current Seek data structure
            
            # Extract job details from content
            job_title = str(content.get("jobHook", ""))
            
            # Get location from the first location in locations list or default empty
            location_info = {}  # Location info not directly available in current structure
            location_city = ""  # Would need to be parsed from content if needed
            location_state = ""  # Would need to be parsed from content if needed
            location_country = "AU"  # Default to AU as per requirements
            
            # Salary info not directly available in current structure
            salary_min = None
            salary_max = None
            
            # Construct Seek job URL using advertiser ID
            # Format: https://www.seek.com.au/job/{advertiser_id}
            job_url = f"https://www.seek.com.au/job/{job_id}" if job_id else ""
            
            # Contact info not directly available in current structure
            contact_email = None
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
            # Log raw Indeed data for debugging
            logger.info(f"Processing job from source: {source_platform}")
            
            # Extract job ID from the new Indeed format
            job_id = (
                job.get("jobkey") or           # New Indeed format
                job.get("jobId") or            # Alternative format
                job.get("jk") or               # URL parameter format
                generate_job_hash(              # Generate hash as last resort
                    job.get("company", "Unknown Company"),
                    job.get("displayTitle", ""),
                    job.get("formattedLocation", "")
                )
            )
            logger.info(f"Processing Indeed job with ID: {job_id}")
            
            # Extract job details using new field names
            job_title = job.get("displayTitle") or job.get("title", "")
            company_name = job.get("company", "Unknown Company")
            logger.info(f"Raw company info from Indeed: {company_name}")
            
            # Company website - try to extract from company details or branding
            company_details = job.get("companyDetails", {})
            company_branding = job.get("companyBrandingAttributes", {})
            company_website = (
                company_details.get("website") or
                company_branding.get("websiteUrl") or
                None
            )
            
            # Location handling
            location_city = job.get("jobLocationCity") or job.get("formattedLocation", "").split()[0]
            location_state = job.get("jobLocationState")
            location_country = "AU"  # Default to Australia based on the data
            
            # Salary handling - check both new and old formats
            salary_info = job.get("salarySnippet", {})
            salary_text = salary_info.get("text", "") if salary_info else ""
            salary_min, salary_max = parse_salary_range(salary_text) if salary_text else (None, None)
            
            # Job URL handling
            job_url = (
                job.get("link") or 
                job.get("clickLoggingUrl") or 
                job.get("noJsUrl", "")
            )
            
            # Contact info is typically not provided in Indeed format
            contact_email = None
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
        
        # Check for existing job by either hash or job_id
        try:
            existing_job = supabase.table("jobs") \
                .select("*") \
                .or_(f"normalized_hash.eq.{normalized_hash},job_id.eq.{job_id}") \
                .execute()
            
            if existing_job.data:
                existing = existing_job.data[0]
                if existing['job_id'] == job_id:
                    logger.info(f"Job {job_id} already exists")
                    return {"message": "Job already exists", "job_id": job_id}
                elif existing['normalized_hash'] == normalized_hash:
                    logger.info(f"Similar job already exists with hash {normalized_hash}")
                    return {"message": "Similar job already exists", "job_id": existing['job_id']}
                
            logger.info(f"No existing job found with ID {job_id} or hash {normalized_hash}")
        except Exception as e:
            logger.error(f"Error checking for existing job: {e}")
            # Continue with insertion attempt even if check fails

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
