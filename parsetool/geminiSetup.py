from google import genai
from pydantic import BaseModel, Field
from typing import Optional

class BWAReport(BaseModel):
    id: int= Field(None, description="Story ID, first line of analyzed article")
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
    relevance_level: Optional[int] = Field(None, description="Give a numeric confidence score (1–5) for how likely the article is about a boil-water advisory.")
    classification: Optional[int] = Field(None, description="Classify the document as one of the following six categories: Category 1 - Microbial violations, treatment failures, sanitary risks found. Category 2 - Long-term/extended deficiencies. Category 3 - Natural disasters. Category 4 - Backflow events. Category 5 - Main breaks, distribution systems repairs, loss of water pressure events. Category 6 - Unknown/unidentified/other")

class BWAReportList(BaseModel):
    reports: list[BWAReport] = Field(description="A list of extracted boil water advisory reports from the articles.")

prompt = """You are an information extraction system. 
              Your task is to read news articles about a boil water advisory and extract key structured details. 
              Each article starts at ArticleID: id START OF DOCUMENT and ends at END OF DOCUMENT. Each article MUST return one JSON object.
              You are to ensure an entry of X articles will return X JSON objects. THIS IS STRICTLY MANDATORY - deviations are severe errors.
              Return a valid JSON object for each article matching this exact schema:

              {
                "id": "integer"
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
                "relevance level": "integer 1-5"
                "classification": "integer 1-6"
              }

              ### Extraction Rules:
              - Publish date or start date is ALWAYS found on the fourth line. If no start date provided, use publish date.
              - If the document ONLY mentions that the advisory was lifted, use that date as the end_date and set start_date to null.
              - Start date and end date should not both be null. THIS IS MANDATORY. If no date is given or mentioned, use the publish date
              - Relevance level should not be null - provide a rating from 1-10 based on how likely the analyzed article is a boil water advisory
              - Use null for any field that is not mentioned or cannot be confidently inferred.
              - Dates should follow the format "Month DD, YYYY" if available, otherwise null.
              - City should be the primary city mentioned; if multiple cities are listed, choose the first one.
              - affected_population should be an integer if possible (e.g., “20,000 residents” → 20000).
              - Relevance level should be rated 1-5 with 5 being guaranteed boil water advisory, 4 mentioning a boil water advisory, 3 mentioning a potential cause for boil water advisory, 2 mentioning utilities, and 1 being irrelevant
              - cause should be concise, e.g., “water main break”, “contamination”, “power outage”, “flooding”.
              - If the article states that an advisory was *lifted*, treat that date as the `end_date`.
              - Do not include extra text, explanations, or commentary—only the JSON object.
              - For classification: Classify the document as one of the following six categories: 
              Category 1 - Microbial violations, treatment failures, sanitary risks found. Category 2 - Long-term/extended deficiencies. 
              Category 3 - Natural disasters. Category 4 - Backflow events. 
              Category 5 - Main breaks, distribution systems repairs, loss of water pressure events. Category 6 - Unknown/unidentified/other

              Now extract the fields from the following articles:
              """
