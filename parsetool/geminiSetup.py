from google import genai
from pydantic import BaseModel, Field
from typing import Optional

class BWAReport(BaseModel):
    start_date: Optional[str] = Field(None, description="The date the advisory started, or when the article was published")
    end_date: Optional[str] = Field(None, description="The date the advisory ended or was lifted")
    affected_population: Optional[int] = Field(None, description="Number of customers or people affected")
    city: Optional[str] = Field(None, description="City where the advisory was issued")
    state: Optional[str] = Field(None, description="State of the affected city")
    county: Optional[str] = Field(None, description="County affected by the advisory")
    utility_name: Optional[str] = Field(None, description="Name of the water utility, if mentioned")
    cause: Optional[str] = Field(None, description="Reason for the advisory (e.g., water main break, contamination, weather)")
    title: Optional[str] = Field(None, description="Title or headline of the news story")
    source: Optional[str] = Field(None, description="Source of the news article (e.g., The Associated Press)")

class BWAReportList(BaseModel):
    reports: list[BWAReport] = Field(description="A list of extracted boil water advisory reports from the articles.")

prompt = """You are an information extraction system. 
              Your task is to read news articles about a boil water advisory and extract key structured details. 
              Each article starts at START OF DOCUMENT and ends at END OF DOCUMENT. Each article MUST return one JSON object.
              You are to ensure 50 articles return 50 JSON objects. THIS IS STRICTLY MANDATORY - deviations are severe errors.
              Return a valid JSON object for each article matching this exact schema:

              {
                "start_date": "string or null",
                "end_date": "string or null",
                "affected_population": "integer or null",
                "city": "string or null",
                "state": "string or null",
                "county": "string or null",
                "utility_name": "string or null",
                "cause": "string or null",
                "title": "string or null",
                "source": "string or null"
              }

              ### Extraction Rules:
              - Publish/start date is ALWAYS found on the third line. Start date and end date should not both be null. THIS IS MANDATORY.
              - Use null for any field that is not mentioned or cannot be confidently inferred.
              - Dates should follow the format "Month DD, YYYY" if available, otherwise null.
              - City should be the primary city mentioned; if multiple cities are listed, choose the first one.
              - affected_population should be an integer if possible (e.g., “20,000 residents” → 20000).
              - cause should be concise, e.g., “water main break”, “contamination”, “power outage”, “flooding”.
              - If the article states that an advisory was *lifted*, treat that date as the `end_date`.
              - Do not include extra text, explanations, or commentary—only the JSON object.

              Once again - 50 articles MUST return 50 JSON objects. THIS IS STRICTLY MANDATORY.
              The importance of this cannot be overstated. Failure to comply indicates a critical error.

              Now extract the fields from the following articles:
              """
