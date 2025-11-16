from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from dateutil import parser as date_parser
import PyPDF2
import io
from urllib.parse import urljoin, urlparse
import uuid
import csv
import pandas as pd
import json
import os
from openai import OpenAI
from dotenv import load_dotenv


app = FastAPI(title="Government News Scraper API")

# Configure Hugging Face OpenAI-compatible API
HF_TOKEN = ''
if not HF_TOKEN:
    print("⚠ WARNING: HF_TOKEN environment variable not set!")
    print("Please set it using: export HF_TOKEN='your_token_here'")

hf_client = OpenAI(
    base_url="https://router.huggingface.co/v1",
    api_key=HF_TOKEN
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB configuration
MONGODB_URL = "mongodb://localhost:27017"
DATABASE_NAME = "gov_scraper_db"
COLLECTION_NAME = "documents"

# MongoDB client
mongodb_client = None
database = None
collection = None

# Thread pool for parallel execution
executor = ThreadPoolExecutor(max_workers=3)

class ScrapeRequest(BaseModel):
    sources: List[str]

class FilterRequest(BaseModel):
    sources: List[str]
    keywords: Optional[List[str]] = None

class Tile(BaseModel):
    heading: str
    description: str
    csv_insights: Optional[str] = None

class ProcessedDocument(BaseModel):
    id: str
    website: str
    link: str
    date: Optional[str] = None
    tiles: List[Tile]
    original_title: str

class Document(BaseModel):
    id: str
    website: str
    title: str
    description: Optional[str] = None
    link: str
    content: str
    date: Optional[str] = None
    scraped_at: str
    csv_data: Optional[str] = None
    table_index: Optional[int] = None

class ScrapeResponse(BaseModel):
    documents: List[Document]
    total: int
    sources: List[str]

# MongoDB connection
@app.on_event("startup")
async def startup_db_client():
    global mongodb_client, database, collection
    try:
        mongodb_client = AsyncIOMotorClient(MONGODB_URL)
        database = mongodb_client[DATABASE_NAME]
        collection = database[COLLECTION_NAME]
        await mongodb_client.admin.command('ping')
        print("✓ Connected to MongoDB successfully")
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        print("Make sure MongoDB is running on localhost:27017")

@app.on_event("shutdown")
async def shutdown_db_client():
    if mongodb_client:
        mongodb_client.close()
        print("✓ MongoDB connection closed")

# Reusable session with connection pooling
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
session.mount('http://', adapter)
session.mount('https://', adapter)

def extract_table_data(table):
    """Extract data from a single table and convert to CSV string"""
    rows = []
    for tr in table.find_all('tr'):
        cells = tr.find_all(['td', 'th'])
        if cells:
            row_data = []
            for cell in cells:
                text = cell.get_text(strip=True)
                text = ' '.join(text.split())
                row_data.append(text)
            if any(row_data):
                rows.append(row_data)
    
    if not rows:
        return None
    
    # Convert to CSV string
    output = io.StringIO()
    max_cols = max(len(row) for row in rows)
    
    # Pad rows to have equal columns
    padded_data = []
    for row in rows:
        padded_row = row + [''] * (max_cols - len(row))
        padded_data.append(padded_row)
    
    writer = csv.writer(output)
    writer.writerows(padded_data)
    
    return output.getvalue()

# RBI Scraper with Table Extraction
def scrape_rbi() -> List[Document]:
    """Scrape RBI press releases with table extraction"""
    try:
        print("Starting RBI scraper...")
        base_url = "https://rbi.org.in/Scripts/"
        main_url = "https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"
        
        response = session.get(main_url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        documents = []
        current_date = None
        
        for row in soup.find_all('tr'):
            date_header = row.find('td', class_='tableheader')
            if date_header:
                h2 = date_header.find('h2', class_='dop_header')
                if h2:
                    current_date = h2.get_text(strip=True)
                    continue
            
            link_td = row.find('a', class_='link2')
            if link_td and 'href' in link_td.attrs:
                title = link_td.get_text(strip=True)
                relative_url = link_td['href']
                full_url = urljoin(base_url, relative_url)
                
                content = ""
                description = ""
                try:
                    print(f"  Fetching detail page: {title[:60]}...")
                    detail_response = session.get(full_url, timeout=15)
                    detail_soup = BeautifulSoup(detail_response.content, 'html.parser')
                    
                    tables = detail_soup.find_all('table')
                    
                    if tables:
                        for table_idx, table in enumerate(tables):
                            table_csv = extract_table_data(table)
                            
                            if table_csv:
                                table_title = f"{title} - Table {table_idx + 1}" if len(tables) > 1 else title
                                
                                table_text = []
                                for tr in table.find_all('tr'):
                                    cells = tr.find_all(['td', 'th'])
                                    row_text = ' | '.join([cell.get_text(strip=True) for cell in cells if cell.get_text(strip=True)])
                                    if row_text:
                                        table_text.append(row_text)
                                
                                content = "\n".join(table_text)
                                description = content[:200] if content else table_title[:200]
                                
                                documents.append(Document(
                                    id=str(uuid.uuid4()),
                                    website="rbi.org.in",
                                    title=table_title,
                                    description=description,
                                    link=full_url,
                                    content=content,
                                    date=current_date,
                                    scraped_at=datetime.now().isoformat(),
                                    csv_data=table_csv,
                                    table_index=table_idx
                                ))
                                
                                print(f"    ✓ Extracted table {table_idx + 1}")
                    else:
                        first_p = detail_soup.find('p')
                        if first_p:
                            description = first_p.get_text(strip=True)[:200]
                            content = first_p.get_text(strip=True)
                        else:
                            description = title[:200]
                            content = title
                        
                        documents.append(Document(
                            id=str(uuid.uuid4()),
                            website="rbi.org.in",
                            title=title,
                            description=description,
                            link=full_url,
                            content=content,
                            date=current_date,
                            scraped_at=datetime.now().isoformat()
                        ))
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"    Error scraping RBI detail page: {e}")
                    documents.append(Document(
                        id=str(uuid.uuid4()),
                        website="rbi.org.in",
                        title=title,
                        description=title[:200],
                        link=full_url,
                        content=title,
                        date=current_date,
                        scraped_at=datetime.now().isoformat()
                    ))
        
        print(f"✓ RBI: Found {len(documents)} documents (including tables)")
        return documents
    except Exception as e:
        print(f"✗ RBI scraping error: {e}")
        return []

def scrape_income_tax() -> List[Document]:
    """Scrape Income Tax latest updates - ALL pages"""
    driver = None
    try:
        print("Starting Income Tax scraper...")
        base_url = "https://incometaxindia.gov.in/Pages/tps/latest-updates.aspx"
        
        chrome_options = ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        driver.maximize_window()
        
        print(f"  Loading website: {base_url}")
        driver.get(base_url)
        time.sleep(5)
        
        documents = []
        
        try:
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            page_info = soup.find(text=re.compile(r'Page\s*\[\s*\d+\s+of\s+\d+\s*\]'))
            
            if page_info:
                match = re.search(r'of\s+(\d+)', page_info)
                if match:
                    total_pages = int(match.group(1))
                else:
                    all_text = soup.get_text()
                    match = re.search(r'Page\s*\[\s*\d+\s+of\s+(\d+)\s*\]', all_text)
                    total_pages = int(match.group(1)) if match else 1
            else:
                total_pages = 1
            
            print(f"  ✓ Detected total pages: {total_pages}")
        except Exception as e:
            print(f"  ⚠ Could not detect total pages, defaulting to 1: {e}")
            total_pages = 1
        
        pdf_counter = 0
        
        for page_num in range(1, total_pages + 1):
            print(f"  {'='*60}")
            print(f"  Processing page {page_num}/{total_pages}...")
            print(f"  {'='*60}")
            
            page_html = driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')
            
            news_rows = soup.find_all('div', class_='news-rows')
            print(f"  ✓ Found {len(news_rows)} items on page {page_num}")
            
            for row in news_rows:
                try:
                    title_elem = row.find('h1')
                    if not title_elem:
                        continue
                    
                    title_link = title_elem.find('a')
                    if not title_link:
                        continue
                        
                    title = title_link.get_text(strip=True)
                    
                    date_elem = row.find('span', id=re.compile('publishDt'))
                    date = date_elem.get_text(strip=True) if date_elem else None
                    
                    onclick = title_link.get('onclick', '')
                    url_match = re.search(r"'(https://[^']+\.pdf)", onclick)
                    
                    if url_match:
                        pdf_url = url_match.group(1)
                        pdf_counter += 1
                        
                        print(f"    [{pdf_counter}] Downloading: {title[:60]}...")
                        
                        content = title
                        description = title[:200]
                        
                        try:
                            max_retries = 3
                            pdf_content = None
                            
                            for attempt in range(max_retries):
                                try:
                                    pdf_response = session.get(pdf_url, timeout=60)
                                    pdf_response.raise_for_status()
                                    pdf_content = pdf_response.content
                                    break
                                except Exception as e:
                                    if attempt < max_retries - 1:
                                        print(f"      Retry {attempt + 1}/{max_retries}...")
                                        time.sleep(2)
                                    else:
                                        raise e
                            
                            if pdf_content:
                                pdf_file = io.BytesIO(pdf_content)
                                pdf_reader = PyPDF2.PdfReader(pdf_file)
                                
                                text_content = []
                                for page_idx in range(len(pdf_reader.pages)):
                                    page = pdf_reader.pages[page_idx]
                                    page_text = page.extract_text()
                                    text_content.append(f"--- Page {page_idx + 1} ---\n{page_text}\n")
                                
                                content = "\n".join(text_content)
                                description = content[:200] if content else title[:200]
                                
                                print(f"      ✓ Extracted PDF text ({len(content)} chars)")
                            
                            time.sleep(1)
                            
                        except Exception as e:
                            print(f"      ⚠ Error extracting PDF: {e}")
                            content = title
                            description = title[:200]
                        
                        documents.append(Document(
                            id=str(uuid.uuid4()),
                            website="incometaxindia.gov.in",
                            title=title,
                            description=description,
                            link=pdf_url,
                            content=content,
                            date=date,
                            scraped_at=datetime.now().isoformat()
                        ))
                        
                except Exception as e:
                    print(f"      ✗ Error processing Income Tax row: {e}")
                    continue
            
            print(f"  ✓ Completed page {page_num}")
            
            if page_num < total_pages:
                try:
                    print(f"  Navigating to page {page_num + 1}...")
                    next_button = driver.find_element(By.CSS_SELECTOR, "input[id*='imgbtnNext']")
                    
                    if next_button.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        time.sleep(1)
                        next_button.click()
                        time.sleep(3)
                        
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "news-rows"))
                        )
                        print(f"  ✓ Successfully navigated to page {page_num + 1}")
                    else:
                        print(f"  Next button is disabled (last page reached)")
                        break
                        
                except Exception as e:
                    print(f"  ✗ Error navigating to next page: {e}")
                    break
                
                time.sleep(2)
        
        print(f"✓ Income Tax: Found {len(documents)} documents")
        return documents
        
    except Exception as e:
        print(f"✗ Income Tax scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if driver:
            print("  Closing browser...")
            driver.quit()

def scrape_gst_council() -> List[Document]:
    """Scrape GST Council press releases - ALL pages"""
    try:
        print("Starting GST Council scraper...")
        base_url = "https://gstcouncil.gov.in/press-release"
        
        documents = []
        total_pages = 9
        
        print(f"  Will scrape {total_pages} pages")
        
        for page_num in range(total_pages):
            url = f"{base_url}?page={page_num}"
            print(f"  Fetching page {page_num + 1}/{total_pages}...")
            
            try:
                response = session.get(url, timeout=30, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                rows = soup.find_all('tr')
                
                page_documents = 0
                
                for row in rows:
                    link_tag = row.find('a', href=lambda x: x and x.endswith('.pdf'))
                    if link_tag:
                        href = link_tag.get('href')
                        title = link_tag.get_text(strip=True)
                        
                        date_cell = row.find('td', class_='views-field-field-date-of-uploading')
                        date = date_cell.get_text(strip=True) if date_cell else None
                        
                        if href.startswith('/'):
                            parsed = urlparse(base_url)
                            full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
                        else:
                            full_url = href
                        
                        content = title
                        description = title[:200]
                        
                        try:
                            print(f"    Downloading: {title[:60]}...")
                            pdf_response = session.get(full_url, timeout=60)
                            pdf_response.raise_for_status()
                            
                            pdf_file = io.BytesIO(pdf_response.content)
                            pdf_reader = PyPDF2.PdfReader(pdf_file)
                            
                            text_content = []
                            for page_idx in range(len(pdf_reader.pages)):
                                page = pdf_reader.pages[page_idx]
                                page_text = page.extract_text()
                                text_content.append(page_text)
                            
                            content = "\n\n".join(text_content)
                            description = content[:200] if content else title[:200]
                            
                            print(f"      ✓ Extracted PDF text ({len(content)} chars)")
                            
                            time.sleep(1)
                            
                        except Exception as e:
                            print(f"      ⚠ Error extracting GST PDF: {e}")
                            content = title
                            description = title[:200]
                        
                        documents.append(Document(
                            id=str(uuid.uuid4()),
                            website="gstcouncil.gov.in",
                            title=title,
                            description=description,
                            link=full_url,
                            content=content,
                            date=date,
                            scraped_at=datetime.now().isoformat()
                        ))
                        
                        page_documents += 1
                
                print(f"  ✓ Found {page_documents} documents on page {page_num + 1}")
                time.sleep(2)
                
            except Exception as e:
                print(f"  ⚠ Error fetching page {page_num + 1}: {e}")
                continue
        
        print(f"✓ GST Council: Found {len(documents)} documents")
        return documents
        
    except Exception as e:
        print(f"✗ GST Council scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []

@app.get("/")
def read_root():
    return {"message": "Government News Scraper API is running", "mongodb": "connected" if collection is not None else "disconnected"}

@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_news(request: ScrapeRequest):
    if not request.sources:
        raise HTTPException(status_code=400, detail="At least one source must be selected")
    
    if collection is None:
        raise HTTPException(status_code=500, detail="MongoDB is not connected")
    
    scraper_map = {
        "RBI": scrape_rbi,
        "Income Tax": scrape_income_tax,
        "GST Council": scrape_gst_council
    }
    
    loop = asyncio.get_event_loop()
    tasks = []
    
    for source in request.sources:
        if source in scraper_map:
            task = loop.run_in_executor(executor, scraper_map[source])
            tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_documents = []
    for result in results:
        if isinstance(result, list):
            all_documents.extend(result)
        elif isinstance(result, Exception):
            print(f"Scraper error: {result}")
    
    if all_documents:
        try:
            documents_dict = [doc.dict() for doc in all_documents]
            await collection.insert_many(documents_dict)
            print(f"✓ Inserted {len(all_documents)} documents into MongoDB")
        except Exception as e:
            print(f"✗ Error inserting into MongoDB: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to save to database: {str(e)}")
    
    return ScrapeResponse(
        documents=all_documents,
        total=len(all_documents),
        sources=request.sources
    )

@app.get("/documents")
async def get_documents(limit: int = 50, skip: int = 0):
    """Get documents from MongoDB"""
    if collection is None:
        raise HTTPException(status_code=500, detail="MongoDB is not connected")
    
    try:
        cursor = collection.find().skip(skip).limit(limit).sort("scraped_at", -1)
        documents = await cursor.to_list(length=limit)
        
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        
        total = await collection.count_documents({})
        
        return {
            "documents": documents,
            "total": total,
            "limit": limit,
            "skip": skip
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch documents: {str(e)}")

@app.delete("/documents")
async def clear_documents():
    """Clear all documents from MongoDB"""
    if collection is None:
        raise HTTPException(status_code=500, detail="MongoDB is not connected")
    
    try:
        result = await collection.delete_many({})
        return {"message": f"Deleted {result.deleted_count} documents"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear documents: {str(e)}")

@app.post("/documents/filter")
async def get_filtered_documents(request: FilterRequest, limit: int = 1000, skip: int = 0):
    """Get documents from MongoDB filtered by sources and keywords"""
    if collection is None:
        raise HTTPException(status_code=500, detail="MongoDB is not connected")
    
    if not request.sources:
        raise HTTPException(status_code=400, detail="At least one source must be selected")
    
    try:
        source_map = {
            "RBI": "rbi.org.in",
            "Income Tax": "incometaxindia.gov.in",
            "GST Council": "gstcouncil.gov.in"
        }
        
        website_filters = [source_map[source] for source in request.sources if source in source_map]
        query = {"website": {"$in": website_filters}}
        
        if request.keywords and len(request.keywords) > 0:
            valid_keywords = [k.strip() for k in request.keywords if k.strip()]
            
            if valid_keywords:
                keyword_conditions = []
                for keyword in valid_keywords:
                    escaped_keyword = re.escape(keyword)
                    keyword_regex = {"$regex": f"\\b{escaped_keyword}\\b", "$options": "i"}
                    
                    keyword_conditions.append({
                        "$or": [
                            {"title": keyword_regex},
                            {"description": keyword_regex},
                            {"content": keyword_regex},
                            {"date": keyword_regex},
                            {"csv_data": keyword_regex}
                        ]
                    })
                
                query = {
                    "$and": [
                        {"website": {"$in": website_filters}},
                        {"$or": keyword_conditions}
                    ]
                }
        
        cursor = collection.find(query).skip(skip).limit(limit).sort("scraped_at", -1)
        documents = await cursor.to_list(length=limit)
        
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        
        total = await collection.count_documents(query)
        
        return {
            "documents": documents,
            "total": total,
            "limit": limit,
            "skip": skip,
            "sources": request.sources,
            "keywords": request.keywords
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch filtered documents: {str(e)}")

def clean_specials(text):
    """Remove markdown symbols and clean special characters"""
    # Remove markdown formatting
    text = re.sub(r'\*\*', '', text)  # Remove bold
    text = re.sub(r'\*', '', text)    # Remove italic
    text = re.sub(r'```.*?```', '', text, flags=re.DOTALL)  # Remove code blocks
    text = re.sub(r'`', '', text)     # Remove inline code
    text = re.sub(r'#+ ', '', text)   # Remove headers
    return text.strip()

async def ask_gptoss(prompt: str) -> str:
    """
    Use Hugging Face GPT-OSS model to process document content
    """
    try:
        print(" Asking GPT-OSS 120B (Groq reasoning)...")
        
        # Run the blocking API call in a thread pool
        loop = asyncio.get_event_loop()
        completion = await loop.run_in_executor(
            None,
            lambda: hf_client.chat.completions.create(
                model="openai/gpt-oss-120b:groq",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a professional document analyzer. "
                            "Extract key information and create structured summaries. "
                            "Always respond with valid JSON only, no markdown or additional text. "
                            "Be concise and factual."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=4096
            )
        )
        
        reply = completion.choices[0].message.content.strip()
        reply = clean_specials(reply)
        print("💬 GPT-OSS Replied (first 200 chars):", reply[:200])
        return reply
        
    except Exception as e:
        print("⚠ GPT-OSS reasoning failed:", e)
        import traceback
        traceback.print_exc()
        return None

async def process_document_with_gptoss(doc: dict) -> ProcessedDocument:
    """Process a single document using Hugging Face GPT-OSS with enhanced prompt"""
    try:
        description = doc.get('description', '') or ''
        content = doc.get('content', '') or ''
        csv_data = doc.get('csv_data', None)
        
        # Process MORE content for better coverage
        full_content = content if content else description
        
        # For very long documents, use strategic sampling
        max_content_length = 12000  # Reduced to be safer with API limits
        original_length = len(full_content)
        
        if len(full_content) > max_content_length:
            # Take strategic sections: beginning (50%), middle sample (20%), end (30%)
            first_part = full_content[:int(max_content_length * 0.5)]
            middle_start = int(original_length * 0.4)
            middle_part = full_content[middle_start:middle_start + int(max_content_length * 0.2)]
            last_part = full_content[-int(max_content_length * 0.3):]
            
            full_content = (
                first_part + 
                "\n\n[... section omitted for brevity ...]\n\n" + 
                middle_part +
                "\n\n[... section omitted for brevity ...]\n\n" + 
                last_part
            )
            print(f"  ⚠ Content sampled: {original_length} chars → {len(full_content)} chars")
            print(f"     (50% start + 20% middle + 30% end)")
        else:
            print(f"  ✓ Processing complete document: {len(full_content)} characters")
        
        # Limit CSV data if present (but keep more for tables)
        if csv_data and len(csv_data) > 3000:  # Increased from 2000
            csv_data = csv_data[:3000] + "\n[Table data truncated...]"
        
        content_length = len(full_content)
        
        # Build the comprehensive prompt focused on simplicity and completeness
        prompt = f"""You are a document translator for the common person. Your job is to take complex government/financial documents and break them into SIMPLE, EASY-TO-UNDERSTAND tiles that anyone can read.

Document Information:
- Title: {doc.get('title', 'N/A')[:200]}
- Website: {doc.get('website', 'N/A')}
- Date: {doc.get('date', 'N/A')}
- Content Length: {content_length} characters

FULL DOCUMENT CONTENT TO ANALYZE:
{full_content}

{f"TABLE/CSV DATA:\n{csv_data}\n" if csv_data else ""}

═══════════════════════════════════════════════════════════════════════
YOUR MISSION: MAKE THIS DOCUMENT UNDERSTANDABLE TO EVERYONE
═══════════════════════════════════════════════════════════════════════

CRITICAL RULES (FOLLOW STRICTLY):

1. **READ EVERY SINGLE WORD** of the document above. Do not skip any section.

2. **CREATE ONE TILE FOR EACH MEANINGFUL PIECE OF INFORMATION**:
   - Each policy point = 1 tile
   - Each guideline = 1 tile  
   - Each statistic or data point = 1 tile
   - Each recommendation = 1 tile
   - Each rule or regulation = 1 tile
   - Each important finding = 1 tile
   - Each table = 1 tile
   
3. **USE EXTREMELY SIMPLE LANGUAGE** as if explaining to someone with no financial/legal background:
   ❌ BAD: "The monetary policy framework necessitates recalibration"
   ✅ GOOD: "The rules about how much money banks can lend need to be updated"
   
   ❌ BAD: "Fiscal consolidation measures"
   ✅ GOOD: "Steps to reduce government spending"
   
   ❌ BAD: "Regulatory compliance mandate"
   ✅ GOOD: "Rules that must be followed"

4. **TRANSLATE ALL FINANCIAL/LEGAL JARGON**:
   - "Liquidity" → "available cash/money"
   - "Regulatory framework" → "set of rules"
   - "Compliance" → "following the rules"
   - "Statutory provisions" → "legal requirements"
   - "Fiscal deficit" → "when government spends more than it earns"
   - "Non-performing assets" → "loans that are not being repaid"
   - "Capital adequacy" → "having enough money reserves"
   - "Disburse" → "give out/pay"
   - "Remit" → "send money"
   - "Levy" → "charge/tax"

5. **BREAK DOWN COMPLEX SENTENCES** into simple, short ones:
   ❌ "The committee, after careful deliberation and extensive consultation with stakeholders, has recommended the implementation of..."
   ✅ "The committee talked to many people. They now recommend that we should..."

6. **FIX ALL FORMATTING ERRORS**:
   - "tr aders" → "traders"
   - "multi-sector al" → "multi-sectoral"
   - Remove page numbers, headers, footers
   - Fix broken words from OCR
   - Merge sentences split across lines

7. **CREATE CLEAR, DESCRIPTIVE HEADINGS** (4-8 words):
   ✅ "New Rules for Bank Loans"
   ✅ "How Much Tax You Need to Pay"
   ✅ "Changes in Interest Rates"
   ✅ "Data: Monthly Sales Report"
   
   ❌ "Subsection 2.3.4"
   ❌ "Recommendations Relati"
   ❌ "Policy Framework"

8. **FOR TABLES/DATA**:
   - Create a tile with heading "Data Table: [What the table shows]"
   - In description: First show the table in clean text format
   - Then explain what the numbers mean in simple words
   - Keep ALL numbers exactly as they are

9. **PRESERVE ALL IMPORTANT DETAILS**:
   - Keep all numbers, dates, amounts EXACTLY as written
   - Keep names of people, organizations, places
   - Keep percentages, statistics, figures
   - Keep deadlines and timelines

10. **DO NOT CREATE TILES FOR**:
    - Page numbers
    - Copyright notices
    - "Page 5 of 50" type text
    - Repeated headers/footers
    - The document title itself (already in Document Information)
    - Simple date stamps (already in Document Information)

11. **COVERAGE REQUIREMENTS**:
    - You MUST create tiles covering 100% of the meaningful content shown above
    - Aim for 15-25 comprehensive tiles (quality over extreme quantity)
    - Each tile should be substantial and informative
    - DO NOT SUMMARIZE multiple points into one tile - split them out!
    - If content is sampled, ensure tiles cover all three sections (start, middle, end)

12. **SIMPLICITY TEST** - Before creating each tile, ask yourself:
    - "Would my grandmother understand this?"
    - "Did I use any complex words?"
    - "Is this shorter and clearer than the original?"
    - If NO to any question, rewrite simpler!

═══════════════════════════════════════════════════════════════════════
OUTPUT FORMAT - CRITICAL
═══════════════════════════════════════════════════════════════════════

**IMPORTANT**: Return ONLY a valid, complete JSON array. 

Rules:
- Start with [ and end with ]
- Each tile must be a complete JSON object
- No markdown, no backticks, no explanation
- No truncation - send the COMPLETE array
- If you have many tiles, send ALL of them

JSON format:
[
  {{
    "heading": "Simple 4-8 Word Title",
    "description": "Easy-to-understand explanation with all numbers preserved exactly. Use short sentences. Explain like talking to a friend.",
    "csv_insights": null
  }},
  {{
    "heading": "Data Table: What This Shows",
    "description": "Column1 | Column2 | Column3\\nValue1 | Value2 | Value3\\n\\nExplanation: This table shows [explain in simple words what the data means and why it matters].",
    "csv_insights": null
  }}
]

**Double-check**: Your response must end with ] to be valid JSON!

EXAMPLE TRANSFORMATION:

ORIGINAL COMPLEX TEXT:
"The Reserve Bank mandates enhanced provisioning norms for non-performing assets, stipulating 15% coverage for substandard assets within 90 days of classification."

YOUR SIMPLIFIED TILE:
{{
  "heading": "New Rules for Bad Loans",
  "description": "When a bank has loans that people are not repaying (called bad loans), the bank must now set aside 15% of that loan amount as backup money. This must be done within 90 days of marking the loan as bad. This rule helps protect the bank and customers if people don't repay.",
  "csv_insights": null
}}

Now analyze the COMPLETE document above and create ALL necessary tiles in simple language:"""
        
        # Add retry logic with longer delays for complex processing
        max_retries = 3
        retry_delay = 8  # Increased from 5 seconds
        
        response_text = None
        for attempt in range(max_retries):
            try:
                print(f"  Attempt {attempt + 1}/{max_retries} - Sending to GPT-OSS API...")
                
                response_text = await ask_gptoss(prompt)
                
                if response_text:
                    print(f"  ✓ Received response from GPT-OSS API")
                    break
                else:
                    if attempt < max_retries - 1:
                        print(f"  ⚠ No response, retrying in {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    
            except Exception as e:
                error_str = str(e)
                if 'timeout' in error_str.lower() or '504' in error_str or '502' in error_str:
                    if attempt < max_retries - 1:
                        print(f"  ⚠ Timeout on attempt {attempt + 1}, retrying in {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"  ✗ All retry attempts failed")
                        raise e
                else:
                    raise e
        
        if not response_text:
            raise Exception("No response received from GPT-OSS after retries")
        
        # Enhanced response cleaning
        response_text = response_text.strip()
        
        # Remove markdown code blocks more aggressively
        response_text = re.sub(r'^```json\s*', '', response_text, flags=re.MULTILINE)
        response_text = re.sub(r'^```\s*', '', response_text, flags=re.MULTILINE)
        response_text = re.sub(r'\s*```$', '', response_text, flags=re.MULTILINE)
        response_text = re.sub(r'```', '', response_text)
        
        # Remove any leading/trailing whitespace again
        response_text = response_text.strip()
        
        # Try to extract JSON array if there's surrounding text
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)
        
        # Log what we're trying to parse
        print(f"  Attempting to parse JSON (length: {len(response_text)} chars)")
        print(f"  First 200 chars: {response_text[:200]}...")
        print(f"  Last 200 chars: ...{response_text[-200:]}")
        
        try:
            tiles_data = json.loads(response_text)
            print(f"  ✓ Successfully parsed JSON with {len(tiles_data)} tiles")
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON parsing error: {e}")
            print(f"  Error at position: {e.pos}")
            
            # Advanced JSON repair for truncated responses
            print(f"  Attempting advanced JSON repair...")
            
            repaired_text = response_text
            
            # Fix trailing commas
            repaired_text = re.sub(r',(\s*[}\]])', r'\1', repaired_text)
            
            # If response was truncated, try to close it properly
            if not repaired_text.rstrip().endswith(']'):
                print(f"  ⚠ JSON appears truncated - attempting to close properly")
                
                # Find the last complete tile
                last_complete = repaired_text.rfind('},')
                if last_complete > 0:
                    # Truncate to last complete tile
                    repaired_text = repaired_text[:last_complete + 1]
                    print(f"  Truncated to last complete tile at position {last_complete}")
                
                # Close any open strings
                if repaired_text.count('"') % 2 != 0:
                    repaired_text += '"'
                
                # Close any unclosed objects
                while repaired_text.count('{') > repaired_text.count('}'):
                    repaired_text += '}'
                
                # Close the array
                if not repaired_text.rstrip().endswith(']'):
                    repaired_text += '\n]'
                
                print(f"  Repaired JSON length: {len(repaired_text)} chars")
            
            # Try parsing the repaired JSON
            try:
                tiles_data = json.loads(repaired_text)
                print(f"  ✓ Successfully repaired and parsed JSON with {len(tiles_data)} tiles")
            except json.JSONDecodeError as e2:
                print(f"  ✗ Repair failed: {e2}")
                print(f"  Using fallback tile creation")
                tiles_data = create_fallback_tiles(doc, description, content, csv_data)
        
        # Validate tiles_data is a list
        if not isinstance(tiles_data, list):
            print(f"  ⚠ Response is not a list, using fallback")
            tiles_data = create_fallback_tiles(doc, description, content, csv_data)
        
        # Ensure we have at least 1 tile
        if len(tiles_data) == 0:
            print(f"  ⚠ No tiles generated, using fallback")
            tiles_data = create_fallback_tiles(doc, description, content, csv_data)
        
        # Create Tile objects with validation
        tiles = []
        for idx, tile_data in enumerate(tiles_data):
            if not isinstance(tile_data, dict):
                print(f"  ⚠ Skipping invalid tile {idx}: not a dict")
                continue
            
            heading = tile_data.get('heading', f'Section {idx + 1}')
            description_text = tile_data.get('description', 'No description available')
            csv_insights = tile_data.get('csv_insights')
            
            # Ensure heading is not too long
            if len(heading) > 200:
                heading = heading[:197] + "..."
            
            # Ensure description is not too long
            if len(description_text) > 5000:
                description_text = description_text[:4997] + "..."
            
            tiles.append(Tile(
                heading=heading,
                description=description_text,
                csv_insights=csv_insights
            ))
        
        print(f"  ✓ Created {len(tiles)} valid tiles")
        
        return ProcessedDocument(
            id=doc.get('id', str(uuid.uuid4())),
            website=doc.get('website', ''),
            link=doc.get('link', ''),
            date=doc.get('date'),
            tiles=tiles,
            original_title=doc.get('title', '')
        )
        
    except Exception as e:
        print(f"  ✗ Error processing document with GPT-OSS: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback response
        description = doc.get('description', '') or doc.get('content', '')[:500]
        csv_data = doc.get('csv_data', None)
        
        return ProcessedDocument(
            id=doc.get('id', str(uuid.uuid4())),
            website=doc.get('website', ''),
            link=doc.get('link', ''),
            date=doc.get('date'),
            tiles=create_fallback_tiles(doc, description, doc.get('content', ''), csv_data),
            original_title=doc.get('title', '')
        )


def create_fallback_tiles(doc: dict, description: str, content: str, csv_data: str = None) -> List[Tile]:
    """Create basic fallback tiles when AI processing fails"""
    tiles = []
    
    # Main content tile
    main_desc = description or content[:1000] if content else "No content available"
    tiles.append(Tile(
        heading=doc.get('title', 'Document Summary')[:100],
        description=main_desc,
        csv_insights=None
    ))
    
    # Table tile if CSV data exists
    if csv_data:
        tiles.append(Tile(
            heading="Data Table",
            description=f"This document contains tabular data:\n\n{csv_data[:1000]}",
            csv_insights=None
        ))
    
    # Additional content tile if content is long
    if content and len(content) > 1000:
        tiles.append(Tile(
            heading="Additional Details",
            description=content[1000:2000],
            csv_insights=None
        ))
    
    return tiles
def safe_parse_date(date_str):
    """Safely parse various date formats"""
    if not date_str or date_str == 'N/A':
        return datetime.min
    
    try:
        # Try ISO format first
        return datetime.fromisoformat(date_str)
    except:
        try:
            # Try parsing other formats like '04-03-2017', '28 October 2025', etc.
            return date_parser.parse(date_str)
        except:
            # If all parsing fails, return minimum datetime
            return datetime.min

@app.post("/documents/process")
async def process_documents(request: FilterRequest, limit: int = 1000, skip: int = 0):
    """Get filtered documents - process ONLY first chronological GST Council doc with AI, display others normally"""
    if collection is None:
        raise HTTPException(status_code=500, detail="MongoDB is not connected")
    
    if not request.sources:
        raise HTTPException(status_code=400, detail="At least one source must be selected")
    
    try:
        source_map = {
            "RBI": "rbi.org.in",
            "Income Tax": "incometaxindia.gov.in",
            "GST Council": "gstcouncil.gov.in"
        }
        
        website_filters = [source_map[source] for source in request.sources if source in source_map]
        query = {"website": {"$in": website_filters}}
        
        if request.keywords and len(request.keywords) > 0:
            valid_keywords = [k.strip() for k in request.keywords if k.strip()]
            
            if valid_keywords:
                keyword_conditions = []
                for keyword in valid_keywords:
                    escaped_keyword = re.escape(keyword)
                    keyword_regex = {"$regex": f"\\b{escaped_keyword}\\b", "$options": "i"}
                    
                    keyword_conditions.append({
                        "$or": [
                            {"title": keyword_regex},
                            {"description": keyword_regex},
                            {"content": keyword_regex},
                            {"date": keyword_regex},
                            {"csv_data": keyword_regex}
                        ]
                    })
                
                query = {
                    "$and": [
                        {"website": {"$in": website_filters}},
                        {"$or": keyword_conditions}
                    ]
                }
        
        print(f"Fetching documents from MongoDB...")
        cursor = collection.find(query).sort("scraped_at", -1)
        all_documents = await cursor.to_list(length=None)
        
        for doc in all_documents:
            doc['_id'] = str(doc['_id'])
        
        print(f"✓ Found {len(all_documents)} total documents")
        
        # Separate GST Council documents from others
        gst_documents = [doc for doc in all_documents if doc.get('website') == 'gstcouncil.gov.in']
        other_documents = [doc for doc in all_documents if doc.get('website') != 'gstcouncil.gov.in']
        
        print(f"  - GST Council documents: {len(gst_documents)}")
        print(f"  - Other documents: {len(other_documents)}")
        
        # Find the first chronological GST Council document (oldest date)
        gst_doc_to_process = None
        if gst_documents:
            # Sort GST documents by date (oldest first)
            sorted_gst_docs = sorted(
                gst_documents,
                key=lambda x: safe_parse_date(x.get('date')),
                reverse=True
            )
            gst_doc_to_process = sorted_gst_docs[0]  # Take the first (oldest) document
            print(f"✓ Selected first chronological GST Council document:")
            print(f"  Title: {gst_doc_to_process.get('title', '')[:80]}...")
            print(f"  Date: {gst_doc_to_process.get('date', 'N/A')}")
            print(f"  Content length: {len(gst_doc_to_process.get('content', ''))} chars")
        
        # Process only the selected GST Council document with AI
        processed_documents = []
        if gst_doc_to_process:
            try:
                print(f"\n{'='*60}")
                print(f"Processing GST Council document with GPT-OSS AI...")
                print(f"{'='*60}")
                
                result = await process_document_with_gptoss(gst_doc_to_process)
                processed_documents.append(result)
                
                print(f"{'='*60}")
                print(f"✓ Successfully processed GST Council document")
                print(f"✓ Generated {len(result.tiles)} tiles")
                print(f"{'='*60}\n")
                
            except Exception as e:
                print(f"{'='*60}")
                print(f"✗ Error processing GST Council document: {e}")
                print(f"{'='*60}\n")
                import traceback
                traceback.print_exc()
        
        # Prepare regular documents (all except the processed GST one)
        regular_documents = other_documents.copy()
        if gst_doc_to_process:
            # Add all other GST documents (not the processed one)
            regular_documents.extend([doc for doc in gst_documents if doc.get('id') != gst_doc_to_process.get('id')])
        
        # Sort regular documents by date (most recent first)
        regular_documents.sort(
            key=lambda x: safe_parse_date(x.get('date')),
            reverse=True
        )
        
        # Count total tiles from processed documents
        total_tiles = sum(len(doc.tiles) for doc in processed_documents)
        
        print(f"{'='*60}")
        print(f"SUMMARY:")
        print(f"  ✓ Processed documents with AI: {len(processed_documents)}")
        print(f"  ✓ Regular documents: {len(regular_documents)}")
        print(f"  ✓ Total tiles generated: {total_tiles}")
        print(f"{'='*60}\n")
        
        return {
            "processed_documents": [doc.dict() for doc in processed_documents],
            "regular_documents": regular_documents,
            "total_processed": len(processed_documents),
            "total_regular": len(regular_documents),
            "total_tiles": total_tiles,
            "sources": request.sources,
            "keywords": request.keywords,
            "note": f"Displaying first chronological GST Council document with AI analysis ({total_tiles} tiles) and {len(regular_documents)} other documents"
        }
        
    except Exception as e:
        print(f"✗ Error in process_documents_with_llm: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to process documents: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    session.close()
    executor.shutdown(wait=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
