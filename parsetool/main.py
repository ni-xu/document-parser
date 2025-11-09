import re, json
import sys
import asyncio
import random
import time
from datetime import datetime
from google import genai
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sql_text
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
    articles = [a.strip() for a in re.split(r"\n{2,}End of Document", text) if a.strip()]

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
            city = re.search(r"Body\s+([A-Z][A-Za-z.-]*(?:\s+[A-Z][A-Za-z.-]*)?)", article)

        city = city.group(0).strip() if city else None

        if city:
            if len(city) >50:
                city=city[:50]
            city = re.sub(r"(?i)\bBody\b", "", city).strip()


        body = (f"Start of Document\n{article}\n\nEnd of Document")

        # save to data
        if body:
            data.append({
                "title": title.group(0).strip() if title else None,
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



def dummy_hash_sort(data):
    unique_articles = []
    seen_entries = set()

    for article in data:
        unique_articles.append(article)

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
    counter = 0
    while True:
        query = sql_text(f"select id, body from bwa_articles limit 25 offset {offset}")
        
        result = session.execute(query).all()
        
        if not result:
            break
        
        entries = [f"Article ID: {item[0]}\n{item[1]}" for item in result if item and all(item[:2])]

        full_batch.append(entries)
        offset += 25

    mod_dict = {}
    for i in range(5):
        mini_batch = []
        for j in range (i, len(full_batch), 5):
            mini_batch.append((j, full_batch[j]))
        mod_dict[i] = mini_batch

    return mod_dict



async def batch_processor(client, to_do, session):
    #for each article batch, if gemini parse returns correct number of articles, commit to SQL
    #if not, log to failed_articles.txt for retry, which is prompted after all batches are processed
    limit = asyncio.Semaphore(1)
    tasks = []

    for key in to_do:
        for minibatch in to_do[key]:
            tasks.append(asyncio.create_task(batch_worker(client, minibatch, session, limit)))

    results = await asyncio.gather(*tasks)
    total = len(results)
    success = sum(results)
    print(f"\n Finished processing {total} batches. Success: {success}, Failed: {total - success}")



async def batch_worker(client, batch, session, limit):
    id,body = batch
    id = str(id)

    async with limit:   
        for attempt in range(0, 2):
            report = await asyncio.to_thread(geminiParse, client, body)
            flattened_report = await asyncio.to_thread(geminiCleaner, report)

            if len(body) == len(flattened_report):
                await asyncio.to_thread(sqlCommit, llmParse, flattened_report, session)
                print(f"[{datetime.now().isoformat()}] Batch {id} succeeded on attempt {attempt}")
                print("\n")
                return True
            else:
                print(f"ERROR: MISMATCHED ARTICLE COUNT {len(batch)} vs {len(flattened_report)}")
        
        print(f"[{datetime.now().isoformat()}] Batch {id} failed deterministically.")
        await asyncio.to_thread(log_failed_batch, body, id)
        return False

def log_failed_batch(batch, batch_id):
    with open("parsetool/debug_files/failed_articles.txt", "a", encoding="utf-8") as f:
        f.write(f"\n\n=== FAILED BATCH {batch_id} ===\n")
        for article in batch:
            f.write(article + "\n\n")
        f.write("=== END FAILED BATCH ===\n")



def geminiParse(client, article_batch):
    # gemini prompt is found in geminiSetup.py
    articles_text = "\n\n".join(article_batch)

    input = (f"{prompt}\n\n{articles_text}")

    print("Awaiting API response")
    response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents= input,
        config={
            "response_mime_type": "application/json",
            "response_schema": BWAReportList,
        },
    )
    
    print("Parse cycle complete")

    return response



def geminiCleaner(data):
    parsed_data = json.loads(data.text)
    flattened_report = []
    for report in parsed_data["reports"]:
        flattened_report.append(report)

    return flattened_report




def initial_db_report():
    len_query_original = "select count(*) from bwa_articles"
    len_query_processed = "select count(*) from bwa_final"
    len_original_result = session.execute(sql_text(len_query_original)).all()
    len_processed_result = session.execute(sql_text(len_query_processed)).all()

    print(f"Found {len_original_result[0][0]} entries in bwa_articles and {len_processed_result[0][0]} articles in bwa_final.")



def mainloop(target_file):
    
        # read in text file
    with open(target_file, "r", encoding="utf-8") as f:
        text = f.read()
    print("File opened")

    open("parsetool/debug_files/failed_articles.txt", "w").close()

    len_query_processed = "select count(*) from bwa_final"

    #inform state of bwa_articles and bwa_final:
    initial_db_report()

    sqlOverwrite = input("Is the target document being updated?")
    if sqlOverwrite == "yes":

        # regex parse
        # even if we dont end up using the hash sort, the regex parse is necessary to have a reliable comparison point for the LLM
        data = regexParse(text)
        print("Regex parse complete")

        # hash sort to remove duplicates
        # Dr. Harris suggests we keep duplicates for LLM step to have more context, this can be commented out if desired
        # If hashsort is not needed, change hashSort to dummy_hash_sort
        unique_articles = dummy_hash_sort(data)

        # commit to SQL
        sqlCommit(articleParse, unique_articles, session)
        print("Regex Section Complete \n\n")
        
        final_doc_overwrite = input("Would you like to purge bwa_final?")
        if final_doc_overwrite == "yes":
            session.execute(sql_text('drop table bwa_final'))


    # prompt LLM
    client = genai.Client()

    start_id = int(input("Enter starting offset ID: "))

    to_do = batch_fetcher(session, start_id)
    print("Batching complete")

    #debug to_do
    with open("parsetool/debug_files/debug_batches.txt", "w", encoding="utf-8") as f:
        for key in to_do:
            f.write(f"------ Parallel Batch {key}-----\n")
            for minibatch in to_do[key]:
                id,body = minibatch
                id = str(id)
                f.write("Minibatch ID " + id + "\n")
                for article in body:
                    f.write(article + "\n\n")

    print(f"Async process starting...")
    
    asyncio.run(batch_processor(client, to_do, session))


if __name__ == "__main__":
    # connect to DB
    session = connectDb()

    mainloop("parsetool/bwafinal.txt")

