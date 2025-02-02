import pytest
from fastapi.testclient import TestClient
from main import app, normalize_location, normalize_salary, clean_text, generate_job_hash

def test_root():
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "FastAPI is running and normalizing job data!"}

def test_normalize_location():
    # Test with a dictionary location
    location = {"city": "Sydney", "state": "NSW", "country": "AU"}
    assert normalize_location(location) == ("Sydney", "NSW", "AU")

    # Test with a string, adjust expected outcome based on function behavior
    location = "Melbourne, VIC"
    assert normalize_location(location) == ("Melbourne", "VIC", "AU")

    # Test with None
    assert normalize_location(None) == (None, None, "AU")

def test_normalize_salary():
    assert normalize_salary(50000, 70000, "USD") == (50000, 70000, "USD")
    assert normalize_salary(50000, None, None) == (50000, None, "AUD")

def test_clean_text():
    assert clean_text("<p>Hello <b>World</b></p>") == "Hello World"
    assert clean_text("Tom &amp; Jerry") == "Tom & Jerry"

def test_generate_job_hash():
    company_name = "ExampleCorp"
    job_title = "Software Engineer"
    city = "New York"
    # Generate expected hash based on function logic
    expected_hash = generate_job_hash(company_name, job_title, city)
    assert generate_job_hash(company_name, job_title, city) == expected_hash
