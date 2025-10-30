import random
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
from geminiSetup import BWAReport, prompt, BWAReportList 

#*********************************************NOTES FOR SETUP**************************************************
# Make sure MySQL is installed and running on localhost:3306, with database "bwa_data" created
# Change root to whatever you have named your SQL user and change pass to your password
# Add your gemini API key to environment variables as GENAI_API_KEY (instructions can be found on gemini docs)
#***************************************************************************************************************

# The most common issue is gemini returning an unexpected number of articles, when this happens the batch that
# gemini failed on is sent to failed_articles.txt. This program will recurse with failed_articles.txt as the input
# until failed_articles.txt is empty. When initially prompted, the program will ask for an offset ID to start from
# unless you are continuing from a stopped batch input zero.


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
        #if artice contains "Start of Document, remove it"
        article = re.sub(r"Start of Document\n", "", article)

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

        city = city.group(0).strip() if city else None

        if city:
            if len(city) >50:
                city=city[:50]

        body = (f"Start of Document\n{article}\n\nEnd of Document")

        # save to data
        if body:
            data.append({
                "title": title.group(0).strip() if date else None,
                "date": date.group(0).strip() if date else None,
                "source": source.group(0).strip() if source else None,
                "city": city if city else None,
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
        if not article['city'] or not article['date']:
            continue  # skip incomplete records

        if article['city']:
            key = (article['city'].lower().strip(), article['date'])

        if key not in seen_entries:
            unique_articles.append(article)
            seen_entries.add(key)

    print(f"Original articles: {len(data)}")
    print(f"Articles after deduplication: {len(unique_articles)}")

    return unique_articles

def sqlCommit(framework, data, session):
    #submits given data to given table 
    try:
        session.bulk_insert_mappings(framework, data)
        session.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")


def batch_fetcher(session, start_id):
    # fetch articles of batch size from start_id to finish
    full_batch = []
    offset = start_id
    while True:
        query = sql_text(f"select body from bwa_articles limit 50 offset {offset}")
        
        result = session.execute(query).all()
        
        if not result:
            break
        
        segment = [item[0] for item in result if item and item[0] is not None]

        full_batch.append(segment)
        offset += 50

    return full_batch


def batch_processor(client, to_do):
    failure_count = 0
    #for each article batch, if gemini parse returns correct number of articles, commit to SQL
    #if not, log to failed_articles.txt for retry, which is prompted after all batches are processed
    for batch in to_do:
        jsonMonster = geminiParse(client, batch)
        parsed_data = json.loads(jsonMonster.text)
        flattened_report = []
        for item in parsed_data:
            for report in item["reports"]:
                flattened_report.append(report)
        
        if len(batch) != len(flattened_report):
            print(f"ERROR: MISMATCHED ARTICLE COUNT {len(batch)} vs {len(flattened_report)}")
            print("Inserting batch to failed_articles.txt")
            print("Skipping commit for this batch\n")
            with open("parsetool/debug_files/failed_articles.txt", "a", encoding="utf-8") as f:
                for article in batch:
                    f.write(article + "\n\n")
            failure_count += 1
        else:
            sqlCommit(llmParse, flattened_report, session)
            print("\n")

    return failure_count


    

def geminiParse(client, article_batch):
    # gemini prompt is found in geminiSetup.py
    articles_text = "\n\n".join(article_batch)
    input = (f"{prompt}\n\n{articles_text} \n Is this returning 50 JSON objects? If not, something has gone severely wrong, and you must restart the process. ")

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


def validate_entries(start_id, num_entries):
    # Take 5% of entries with id's from start_id to start_id + num_entries randomly
    num_list = random.sample(range(start_id, start_id + num_entries), max(1, num_entries // 20))
    original_query = f"select * from bwa_articles where id = {num_list[0]}"
    final_query = f"select * from bwa_final where id = {num_list[0]}"
    for item in num_list:
        original_query += f" or id = {item}"
        final_query += f" or id = {item}"
    
    original_result = session.execute(sql_text(original_query)).all()
    final_result = session.execute(sql_text(original_query)).all()    

    with open("parsetool/debug_files/validation.txt", "w", encoding="utf-8") as f:
        
        for item in original_result:
            indexer = item[0]
            f.write(f"Comparison on entry {indexer}:\n")
            f.write(f"Date: {item[2]} vs {final_result[indexer][2]}\n")
            f.write(f"City: {item[4]} vs {final_result[indexer][4]}\n")
            f.write("Original Article Body:\n")
            f.write(item[5] + "\n")


def mainloop(target_file):
        # read in text file
    with open(target_file, "r", encoding="utf-8") as f:
        text = f.read()
    print("File opened")

    # regex parse
    data = regexParse(text)
    print("Regex parse complete")

    # hash sort to remove duplicates
    unique_articles = hashSort(data)

    # commit to SQL
    sqlCommit(articleParse, unique_articles, session)
    print("Regex Section Complete \n\n")

    # prompt LLM
    client = genai.Client()

    start_id = int(input("Enter starting offset ID: "))

    to_do = batch_fetcher(session, start_id)
    print("Batching complete")

    #debug to_do
    with open("parsetool/debug_files/debug_batches.txt", "w", encoding="utf-8") as f:
        for batch in to_do:
            f.write("------ New Batch -----\n")
            for article in batch:
                f.write(article + "\n\n")

    failure_count = batch_processor(client, to_do)

    if failure_count > 0:
        #run program using failed_articles.txt
        runbool = input("Would you like to retry the failed articles? BE SURE TO MAKE A COPY OF debug_batches " \
        "OR IT WILL BE OVERWRITTEN")
        
        if runbool == "yes":
            mainloop("parsetool/debug_files/failed_articles.txt")

    validate_entries(start_id, len(unique_articles))


if __name__ == "__main__":
    # connect to DB
    session = connectDb()

    mainloop("parsetool/bwabugged.txt")

