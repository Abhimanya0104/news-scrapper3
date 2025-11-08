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
import PyPDF2
import io
from urllib.parse import urljoin, urlparse
import uuid
import csv
import pandas as pd

app = FastAPI(title="Government News Scraper API")

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
        
        # Find all rows in the table
        for row in soup.find_all('tr'):
            # Check if this row is a date header
            date_header = row.find('td', class_='tableheader')
            if date_header:
                h2 = date_header.find('h2', class_='dop_header')
                if h2:
                    current_date = h2.get_text(strip=True)
                    continue
            
            # Find press release link
            link_td = row.find('a', class_='link2')
            if link_td and 'href' in link_td.attrs:
                title = link_td.get_text(strip=True)
                relative_url = link_td['href']
                full_url = urljoin(base_url, relative_url)
                
                # Scrape detail page for tables
                content = ""
                description = ""
                try:
                    print(f"  Fetching detail page: {title[:60]}...")
                    detail_response = session.get(full_url, timeout=15)
                    detail_soup = BeautifulSoup(detail_response.content, 'html.parser')
                    
                    # Extract tables
                    tables = detail_soup.find_all('table')
                    
                    if tables:
                        # Process each table separately
                        for table_idx, table in enumerate(tables):
                            table_csv = extract_table_data(table)
                            
                            if table_csv:
                                # Create separate document for each table
                                table_title = f"{title} - Table {table_idx + 1}" if len(tables) > 1 else title
                                
                                # Extract text content for description
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
                        # No tables, store as regular document
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
                    
                    time.sleep(0.5)  # Rate limiting
                    
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
        
        # Get total pages
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
        
        # Process all pages
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
                    
                    # Extract PDF URL from onclick attribute
                    onclick = title_link.get('onclick', '')
                    url_match = re.search(r"'(https://[^']+\.pdf)", onclick)
                    
                    if url_match:
                        pdf_url = url_match.group(1)
                        pdf_counter += 1
                        
                        print(f"    [{pdf_counter}] Downloading: {title[:60]}...")
                        
                        # Extract PDF content
                        content = title
                        description = title[:200]
                        
                        try:
                            # Download PDF with retries
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
                                # Extract text from PDF
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
                            
                            time.sleep(1)  # Rate limiting
                            
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
            
            # Navigate to next page (if not the last page)
            if page_num < total_pages:
                try:
                    print(f"  Navigating to page {page_num + 1}...")
                    next_button = driver.find_element(By.CSS_SELECTOR, "input[id*='imgbtnNext']")
                    
                    if next_button.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        time.sleep(1)
                        next_button.click()
                        time.sleep(3)
                        
                        # Wait for news rows to be present
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
        
        # Determine total pages by checking pagination or use a safe maximum
        total_pages = 9  # Based on the website structure, adjust if needed
        
        print(f"  Will scrape {total_pages} pages")
        
        # Scrape all pages
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
                        
                        # Get date
                        date_cell = row.find('td', class_='views-field-field-date-of-uploading')
                        date = date_cell.get_text(strip=True) if date_cell else None
                        
                        # Handle relative URLs
                        if href.startswith('/'):
                            parsed = urlparse(base_url)
                            full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
                        else:
                            full_url = href
                        
                        # Extract PDF content
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
                            
                            time.sleep(1)  # Rate limiting after each PDF
                            
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
                
                # Small delay between pages
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
    
    # Map sources to their scraper functions
    scraper_map = {
        "RBI": scrape_rbi,
        "Income Tax": scrape_income_tax,
        "GST Council": scrape_gst_council
    }
    
    # Run all scrapers in parallel
    loop = asyncio.get_event_loop()
    tasks = []
    
    for source in request.sources:
        if source in scraper_map:
            task = loop.run_in_executor(executor, scraper_map[source])
            tasks.append(task)
    
    # Wait for all scrapers to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Flatten results and insert into MongoDB
    all_documents = []
    for result in results:
        if isinstance(result, list):
            all_documents.extend(result)
        elif isinstance(result, Exception):
            print(f"Scraper error: {result}")
    
    # Insert documents into MongoDB
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
        
        # Convert ObjectId to string
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
        # Map source names to website domains
        source_map = {
            "RBI": "rbi.org.in",
            "Income Tax": "incometaxindia.gov.in",
            "GST Council": "gstcouncil.gov.in"
        }
        
        # Create list of website domains to filter by
        website_filters = [source_map[source] for source in request.sources if source in source_map]
        
        # Build MongoDB query
        query = {"website": {"$in": website_filters}}
        
        # Add keyword filtering if keywords are provided
        if request.keywords and len(request.keywords) > 0:
            # Filter out empty keywords
            valid_keywords = [k.strip() for k in request.keywords if k.strip()]
            
            if valid_keywords:
                # Create regex patterns for case-insensitive partial matching
                keyword_conditions = []
                for keyword in valid_keywords:
                    # Escape special regex characters
                    escaped_keyword = re.escape(keyword)
                    keyword_regex = {"$regex": escaped_keyword, "$options": "i"}
                    
                    # Search in title, description, and content
                    keyword_conditions.append({
                        "$or": [
                            {"title": keyword_regex},
                            {"description": keyword_regex},
                            {"content": keyword_regex}
                        ]
                    })
                
                # Combine with source filter using AND logic
                query = {
                    "$and": [
                        {"website": {"$in": website_filters}},
                        {"$or": keyword_conditions}
                    ]
                }
        
        # Fetch filtered documents
        cursor = collection.find(query).skip(skip).limit(limit).sort("scraped_at", -1)
        documents = await cursor.to_list(length=limit)
        
        # Convert ObjectId to string
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        
        # Get total count for filtered results
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

@app.on_event("shutdown")
async def shutdown_event():
    session.close()
    executor.shutdown(wait=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
