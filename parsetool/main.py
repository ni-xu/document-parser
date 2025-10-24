import re, json
import sys
import oracledb
from google import genai
from pydantic import BaseModel, Field
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.sql import text as sql_text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlSetup import articleParse, llmParse, Base
from geminiSetup import BWAReport, prompt, BWAReportList #

def connectDb():
    # Atttempt DB connection    
    try:
        engine = create_engine("mysql+pymysql://root:pass@localhost:3306/bwa_data")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        sys.exit(1)

def regexParse(text):
    # Split by LexisNexis delimiter
    articles = re.split(r"\n{2,}End of Document", text)

    data = []
    for article in articles:
        
        #takes first line 
        title = re.search(r"(?m)^\s*([A-Za-z0-9'\"()., :;?!/#@_.`'&-]+)\s*$", article)

        #word boundary, month names, day with optional comma, year
        date = re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}", article)

        #everything inbetween copyright and length:
        source = re.search(r"(?<=Copyright).*?(?=Length:)", article, re.DOTALL)

        #either takes information after dateline, or if not exists takes first capitalized words after body
        city = None
        if re.search(r"(?i)dateline", article):
            city = re.search(r"(?s)(?<=Dateline:)(.*?)(?=Body)", article, re.IGNORECASE)
        else:
            city = re.search(r"Body\s+([A-Z][a-zA-Z.-]*\s+[A-Z][a-zA-Z.-]*)", article)

        body = (f"START OF DOCUMENT\n{article}\nEND OF DOCUMENT\n")

        # save to data
        if body:
            data.append({
                "title": title.group(0).strip() if date else None,
                "date": date.group(0).strip() if date else None,
                "source": source.group(0).strip() if source else None,
                "city": city.group(0).strip() if city else None,
                "body": body if body else None
            })

    return data

def hashSort(data):
    # hash set to attempt the removal of duplicates
    # only partially effective
    # articles can be 1 day apart, be close cities, not have a city, consequence is requires more tokens during LLM step
    
    unique_articles = []
    seen_entries = set()

    for article in data:
        if article['city']:
            key = (article['city'].lower().strip(), article['date'])

        if key not in seen_entries:
            unique_articles.append(article)
            seen_entries.add(key)

    print(f"Original articles: {len(data)}")
    print(f"Articles after deduplication: {len(unique_articles)}")

    return unique_articles

def sqlCommit(framework, data, session):
    try:
        session.bulk_insert_mappings(framework, data)
        session.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

def batch_fetcher(session):
    full_batch = []
    offset = 0
    while True:
        query = sql_text(f"select body from bwa_articles limit 50 offset {offset}")
        
        result = session.execute(query).all()
        
        if not result:
            break
        
        segment = [item[0] for item in result if item and item[0] is not None]

        full_batch.append(segment)
        offset += 50

    return full_batch

def geminiParse(client, article_batch):
    articles_text = "\n\n".join(article_batch)
    input = (f"{prompt}\n\n{articles_text} \n Is this returning 50 JSON objects? If not, something has gone severely wrong, and you must restart the process.")

    print("Awaiting API response")
    response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents= input,
        config={
            "response_mime_type": "application/json",
            "response_schema": list[BWAReportList],
        },
    )
    
    print("Parse cycle complete")

    return response

if __name__ == "__main__":
    # connect to DB
    session = connectDb()

    # read in text file
    with open("parsetool/bwamini.txt", "r", encoding="utf-8") as f:
        text = f.read()

    # regex parse
    data = regexParse(text)

    # hash sort to remove duplicates
    unique_articles = hashSort(data)

    # commit to SQL
    sqlCommit(articleParse, unique_articles, session)
    print("Regex Section Complete \n\n")

    # prompt LLM
    client = genai.Client()

    to_do = batch_fetcher(session)
    print("Batching complete")

    #debug to_do
    with open("parsetool/debug_batches.txt", "w", encoding="utf-8") as f:
        for batch in to_do:
            f.write("------ New Batch -----\n")
            for article in batch:
                f.write(article + "\n\n")

    #for each article batch, 
    for article_batch in to_do:
        print("Loop entered")
        jsonMonster = geminiParse(client, article_batch)
        parsed_data = json.loads(jsonMonster.text)
        flattened_report = []
        for item in parsed_data:
            for report in item["reports"]:
                flattened_report.append(report)
        
        print(f"Began with {len(article_batch)} entries, Commiting {len(flattened_report)} entries to DB\n\n")
        sqlCommit(llmParse, flattened_report, session)

        print("Hunnid down")