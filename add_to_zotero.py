import pandas as pd
from pyzotero import zotero
import getpass
import sys
import numpy as np
import time
from habanero import Crossref # Import habanero
import os
import logging
import re

INPUT_CSV = 'merged_papers.csv'
ORIG_WOS_XLS = 'WebOfScience.xls'
ORIG_PSYC_XLS = 'PsycInfo.xls'
LOG_FILE = 'zotero_import_log_v4.txt' # New log file for this version

# === Configuration and Constants === #
rate_limit_delay = 15  # Seconds to wait if hit by rate limit

# === Setup Logging === #
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
                   format='%(asctime)s - %(message)s', 
                   datefmt='%Y-%m-%d %H:%M:%S')

def log_message(message):
    """Log a message to both the log file and console"""
    print(message)
    logging.info(message)

# --- Helper: Standardization functions (needed to match merged keys to original data) ---
def standardize_text(text):
    if isinstance(text, pd.Series):
        return text.astype(str).str.lower().str.strip().replace('nan', np.nan)
    elif pd.isna(text) or text is None:
        return ''
    else:
        return str(text).lower().strip()

def standardize_year(year):
    if pd.isna(year) or year is None:
        return ''
    # Extract first 4-digit number from the string
    match = re.search(r'\d{4}', str(year))
    if match:
        return match.group(0)
    return str(year).strip()

def standardize_year_wos(series):
    return pd.to_numeric(series, errors='coerce').astype('Int64')

def standardize_year_psyc(series):
    return pd.to_numeric(series.astype(str).str[:4], errors='coerce').astype('Int64')

# --- Helper: Find Original Record --- #
def find_original_record(doi_std, title_std, author_std, year_std, wos_orig_lookup, psyc_orig_lookup):
    # Prioritize DOI match if available
    if pd.notna(doi_std):
        if doi_std in wos_orig_lookup['DOI_std'].values:
            return wos_orig_lookup[wos_orig_lookup['DOI_std'] == doi_std].iloc[0], 'WOS'
        if doi_std in psyc_orig_lookup['DOI_std'].values:
            return psyc_orig_lookup[psyc_orig_lookup['DOI_std'] == doi_std].iloc[0], 'PsycInfo'

    # Fallback to Title/Author/Year match
    secondary_key = f"{title_std or ''}|{author_std or ''}|{year_std or ''}"
    if pd.notna(title_std) and pd.notna(author_std) and pd.notna(year_std):
        if secondary_key in wos_orig_lookup['Secondary_Key'].values:
            return wos_orig_lookup[wos_orig_lookup['Secondary_Key'] == secondary_key].iloc[0], 'WOS'
        if secondary_key in psyc_orig_lookup['Secondary_Key'].values:
            return psyc_orig_lookup[psyc_orig_lookup['Secondary_Key'] == secondary_key].iloc[0], 'PsycInfo'

    log_message(f"  WARNING: Could not find original record for DOI: {doi_std} / Title: {title_std:.30}")
    return None, None

# --- Helper: Create Zotero Template from Crossref Data --- #
def create_template_from_crossref(cr_data, collection_id=None):
    template = {
        'itemType': 'journalArticle',
        'title': cr_data.get('title', [''])[0] if isinstance(cr_data.get('title', []), list) else cr_data.get('title', ''),
        'creators': []
    }
    
    # Process creators
    if 'author' in cr_data:
        for author in cr_data['author']:
            creator = {'creatorType': 'author'}
            if 'given' in author and 'family' in author:
                creator['firstName'] = author['given']
                creator['lastName'] = author['family']
            elif 'name' in author:
                name_parts = author['name'].split(' ', 1)
                if len(name_parts) > 1:
                    creator['lastName'] = name_parts[-1]
                    creator['firstName'] = name_parts[0]
                else:
                    creator['lastName'] = author['name']
                    creator['firstName'] = ''
            template['creators'].append(creator)
    
    # Extract other metadata
    if 'container-title' in cr_data:
        pub_title = cr_data['container-title'][0] if isinstance(cr_data['container-title'], list) else cr_data['container-title']
        template['publicationTitle'] = pub_title
    
    if 'issued' in cr_data and 'date-parts' in cr_data['issued']:
        date_parts = cr_data['issued']['date-parts'][0]
        if len(date_parts) > 0:
            template['date'] = str(date_parts[0])  # Year
    
    if 'volume' in cr_data:
        template['volume'] = cr_data['volume']
    
    if 'issue' in cr_data:
        template['issue'] = cr_data['issue']
    
    if 'page' in cr_data:
        template['pages'] = cr_data['page']
    
    if 'DOI' in cr_data:
        template['DOI'] = cr_data['DOI']
    
    if cr_data.get('abstract'):
        abstract_text = re.sub('<[^<]+?>', '', cr_data['abstract'])
        template['abstractNote'] = abstract_text.strip()

    # Add collection if provided
    if collection_id:
        template['collections'] = [collection_id]
    else:
        template['collections'] = []

    return template

# --- Helper: Create Zotero Template from Original Data --- #
def create_template_from_original(original_row, source_db, collection_id=None):
    template = {
        'itemType': 'journalArticle',
        'title': standardize_text(original_row.get('Title', '')),
        'creators': []
    }
    
    # Process authors based on source database format
    authors_field = 'Authors' if 'Authors' in original_row else 'Author' if 'Author' in original_row else None
    
    if authors_field and not pd.isna(original_row.get(authors_field)):
        authors_text = original_row[authors_field]
        
        # Different parsing for different source databases
        if source_db == 'WebOfScience':
            # WoS format: "LastName, FirstName; LastName, FirstName"
            authors = authors_text.split(';')
            for author in authors:
                author = author.strip()
                if ',' in author:
                    last_name, first_name = author.split(',', 1)
                    template['creators'].append({
                        'creatorType': 'author',
                        'lastName': last_name.strip(),
                        'firstName': first_name.strip()
                    })
                elif author:  # Just in case there's no comma
                    template['creators'].append({
                        'creatorType': 'author',
                        'lastName': author,
                        'firstName': ''
                    })
        elif source_db == 'PsycInfo':
            # PsycInfo format varies, might be "LastName, FirstName; LastName, FirstName" or other
            authors = authors_text.split(';')
            for author in authors:
                author = author.strip()
                if ',' in author:
                    last_name, first_name = author.split(',', 1)
                    template['creators'].append({
                        'creatorType': 'author',
                        'lastName': last_name.strip(),
                        'firstName': first_name.strip()
                    })
                elif ' ' in author:  # FirstName LastName format
                    parts = author.rsplit(' ', 1)
                    template['creators'].append({
                        'creatorType': 'author',
                        'firstName': parts[0].strip(),
                        'lastName': parts[1].strip()
                    })
                elif author:  # Just in case there's no space
                    template['creators'].append({
                        'creatorType': 'author',
                        'lastName': author,
                        'firstName': ''
                    })
    
    # Map other fields
    journal_field = 'Journal' if 'Journal' in original_row else 'Source Title' if 'Source Title' in original_row else None
    if journal_field and not pd.isna(original_row.get(journal_field)):
        template['publicationTitle'] = standardize_text(original_row[journal_field])
    
    if 'Year' in original_row and not pd.isna(original_row['Year']):
        template['date'] = standardize_year(original_row['Year'])
    
    if 'Volume' in original_row and not pd.isna(original_row['Volume']):
        template['volume'] = standardize_text(original_row['Volume'])
    
    if 'Issue' in original_row and not pd.isna(original_row['Issue']):
        template['issue'] = standardize_text(original_row['Issue'])
    
    if 'Pages' in original_row and not pd.isna(original_row['Pages']):
        template['pages'] = standardize_text(original_row['Pages'])
    
    if 'DOI' in original_row and not pd.isna(original_row['DOI']):
        template['DOI'] = standardize_text(original_row['DOI'])
    
    if 'Abstract' in original_row and not pd.isna(original_row['Abstract']):
        template['abstractNote'] = standardize_text(original_row['Abstract'])
    
    # Ensure required fields have values
    for key in ['title', 'publicationTitle', 'date', 'DOI']:
        if not template.get(key):
             template[key] = ''

    # Add collection if provided
    if collection_id:
        template['collections'] = [collection_id]
    else:
        template['collections'] = []

    return template

# --- Helper: Add Existing Item to Collection --- #
def add_existing_item_to_collection(zot_client, item_key, collection_id, collection_name):
    if not collection_id or not item_key:
        log_message(f"  Skipping add to collection: Missing item_key ({item_key}) or collection_id ({collection_id})")
        return

    log_message(f"  Checking/adding existing Item Key {item_key} to Collection ID {collection_id} ('{collection_name}').")
    try:
        item_data = zot_client.item(item_key) # Fetch current item data
        if not item_data:
            log_message(f"    ERROR: Could not fetch data for existing item key {item_key}.")
            return

        current_collections = item_data['data'].get('collections', [])
        if collection_id not in current_collections:
            log_message(f"    Item not in collection, attempting to add.")
            updated_collections = current_collections + [collection_id]
            update_payload = {'collections': updated_collections}
            # Get current version for update
            item_version = item_data['data']['version']
            zot_client.update_item(item_key, update_payload, version=item_version)
            log_message(f"    Successfully added existing item {item_key} to collection '{collection_name}'.")
        else:
            log_message(f"    Item {item_key} already in collection '{collection_name}'.")

    except Exception as e:
         log_message(f"    ERROR: Exception adding existing item {item_key} to collection {collection_id}. Error: {e}")
         import traceback
         log_message(traceback.format_exc())
         if "Rate limit" in str(e):
             log_message(f"    WARNING: Hit Zotero API rate limit checking/adding existing item to collection. Waiting {rate_limit_delay}s.")
             time.sleep(rate_limit_delay)

# === Main Script Logic ===

# Clear previous log file
with open(LOG_FILE, 'w', encoding='utf-8') as f:
    f.write("--- Zotero Import Log V4 (CrossRef) ---\n")

# --- Get Credentials & Initialize Zotero --- #
print("\n===== Zotero Import Tool =====")
print("Please enter your Zotero credentials.")
print("Find your User ID and create an API Key here: https://www.zotero.org/settings/keys")

# Get Zotero user ID (should be numeric)
library_id = ""
while not library_id.isdigit():
    library_id = input("Enter your Zotero User ID (numbers only): ").strip()
    if not library_id.isdigit():
        print("Error: User ID should be numeric. Please check your Zotero settings.")

# Get Zotero API key (should be a string of letters and numbers)
api_key = getpass.getpass("Enter your Zotero API Key (input will be hidden): ")
if not api_key or len(api_key) < 10:  # Basic validation
    print("Error: API Key appears invalid. Please check your Zotero settings.")
    sys.exit(1)

library_type = 'user'
log_message(f"Connecting to Zotero library ID {library_id}...")

try:
    zot = zotero.Zotero(library_id, library_type, api_key)
    # Test the connection with a simple call
    zot.collections(limit=1)
    log_message("Successfully connected to Zotero.")
except Exception as e:
    log_message(f"Error connecting to Zotero: {e}")
    sys.exit(1)

# --- Get or Create Collection ---
collection_name = input(f"Enter the name for the Zotero collection (e.g., Leadership Identity Meta-Analysis): ").strip()
if not collection_name:
    collection_name = "Meta-Analysis Import"
    log_message(f"No collection name entered, using default: '{collection_name}'")
collections = zot.collections()
collection_id = None
for coll in collections:
    if coll['data']['name'] == collection_name:
        collection_id = coll['key']
        log_message(f"Found existing collection: '{collection_name}' (ID: {collection_id})")
        break
if collection_id is None:
    log_message(f"Creating new collection: '{collection_name}'...")
    try:
        resp = zot.create_collections([{'name': collection_name}])
        if resp['successful']:
            collection_id = list(resp['successful'].keys())[0]
            log_message(f"Successfully created collection (ID: {collection_id})")
        else:
            log_message(f"Error creating collection: {resp}")
            collection_id = None
    except Exception as e:
        log_message(f"Exception creating collection: {e}")
        collection_id = None
if collection_id is None:
    log_message("Could not find or create the target collection. Items will be added to the main library.")

# --- Load Data --- #
log_message(f"Loading deduplicated list from {INPUT_CSV}...")
try:
    dedup_df = pd.read_csv(INPUT_CSV)
    log_message(f"Loaded {len(dedup_df)} unique records to process.")
except FileNotFoundError:
    log_message(f"Error: Deduplicated file '{INPUT_CSV}' not found.")
    sys.exit(1)

log_message(f"Loading original data from {ORIG_WOS_XLS} and {ORIG_PSYC_XLS}...")
try:
    wos_orig_df = pd.read_excel(ORIG_WOS_XLS)
    psyc_orig_df = pd.read_excel(ORIG_PSYC_XLS)
    log_message("Original data loaded successfully.")
except FileNotFoundError:
    log_message(f"Error: Original XLS file(s) not found.")
    sys.exit(1)

# --- Pre-process Original Data for Lookup --- #
log_message("Preprocessing original data for lookups...")
# WOS
wos_orig_df['DOI_std'] = standardize_text(wos_orig_df['DOI'])
wos_orig_df['Title_std'] = standardize_text(wos_orig_df['Article Title'])
wos_orig_df['Authors_std'] = standardize_text(wos_orig_df['Authors'])
wos_orig_df['Year_std'] = standardize_year_wos(wos_orig_df['Publication Year'])
wos_orig_df['Secondary_Key'] = wos_orig_df['Title_std'].fillna('') + '|' + wos_orig_df['Authors_std'].fillna('') + '|' + wos_orig_df['Year_std'].astype(str).fillna('')

# PsycInfo
psyc_orig_df['DOI_std'] = standardize_text(psyc_orig_df['doi'])
psyc_orig_df['Title_std'] = standardize_text(psyc_orig_df['title'])
psyc_orig_df['Authors_std'] = standardize_text(psyc_orig_df['Authors'])
psyc_orig_df['Year_std'] = standardize_year_psyc(psyc_orig_df['publicationDate'])
psyc_orig_df['Secondary_Key'] = psyc_orig_df['Title_std'].fillna('') + '|' + psyc_orig_df['Authors_std'].fillna('') + '|' + psyc_orig_df['Year_std'].astype(str).fillna('')

# --- Initialize Crossref Client --- #
cr = Crossref()
log_message("Initialized Crossref client.")

# --- Process Each Paper --- #
log_message("\n--- Starting Zotero Item Processing --- (CrossRef -> Zotero ID -> Manual) - v5 ---")
added_count = 0
failed_count = 0
processed_count = 0

for index, row in dedup_df.iterrows():
    processed_count += 1
    paper_id = row.get('paper_id')
    doi_std = row.get('DOI')
    title_std = row.get('Article Title')
    authors_std = row.get('Authors')
    year_std = row.get('Publication Year')
    log_message(f"\nProcessing Paper ID: {paper_id} ({processed_count}/{len(dedup_df)}), DOI: {doi_std if pd.notna(doi_std) else 'N/A'}, Title: {title_std:.30}...")

    item_key = None
    item_created_now = False # Track if *this run* created the item
    template_source = None
    zotero_template = None

    # --- Strategy 1: Query CrossRef using DOI --- #
    if pd.notna(doi_std) and doi_std:
        log_message(f"  1. Attempting CrossRef query for DOI: {doi_std}")
        try:
            cr_result = cr.works(ids=doi_std)
            if cr_result and 'message' in cr_result:
                cr_data = cr_result['message']
                log_message("    CrossRef query successful.")
                # Pass collection_id when creating template
                zotero_template = create_template_from_crossref(cr_data, collection_id)
                if zotero_template:
                    template_source = "CrossRef"
                    log_message("    Created Zotero template (incl. collection) from CrossRef data.")
                else:
                     log_message("    WARNING: Could not create template from CrossRef data.")
            else:
                log_message("    WARNING: CrossRef query returned no message data.")
        except Exception as e:
            log_message(f"    ERROR: Exception during CrossRef query: {e}")

    # --- Create Item in Zotero (if template from CrossRef created) --- #
    if zotero_template:
        log_message(f"  Attempting Zotero item creation using template from: {template_source}")
        try:
            resp = zot.create_items([zotero_template])
            log_message(f"    Response from create_items: {resp}")
            if resp['successful'] and '0' in resp['successful'] and 'key' in resp['successful']['0']:
                item_key = resp['successful']['0']['key']
                item_created_now = True
                log_message(f"    SUCCESS: Item created in Zotero using {template_source} data. Item Key: {item_key}. Added to collection during creation.")
            elif resp['failed']:
                 # Handle potential duplicate error if item already exists but wasn't found by Crossref?
                 if '0' in resp['failed'] and resp['failed']['0']['code'] == 412: # Precondition failed (likely duplicate)
                     log_message("    INFO: Item creation failed (Code 412), likely already exists. Will try Zotero ID lookup.")
                 else:
                     log_message(f"    ERROR: Failed Zotero create_items call. Failure: {resp['failed']}")
            else:
                log_message("    ERROR: Unexpected response from Zotero create_items.")
            zotero_template = None # Reset template
        except Exception as e:
             log_message(f"    ERROR: Exception during Zotero create_items: {e}")
             import traceback
             log_message(traceback.format_exc())
             if "Rate limit" in str(e):
                 log_message(f"  WARNING: Hit Zotero API rate limit during creation. Waiting {rate_limit_delay}s. Item may fail.")
                 time.sleep(rate_limit_delay)

    # --- Strategy 2: Zotero Identifier Lookup (if CrossRef failed/skipped and DOI exists) --- #
    if item_key is None and pd.notna(doi_std) and doi_std:
        log_message(f"  2. Attempting Zotero identifier lookup for DOI: {doi_std}")
        try:
            resp = zot.add_items_by_identifier([doi_std])
            log_message(f"    Response from add_items_by_identifier: {resp}")
            if resp and isinstance(resp, dict) and resp.get('success'):
                item_key = resp['success'][0]
                item_created_now = True # Zotero created it now
                log_message(f"    SUCCESS: Item created via Zotero ID lookup. Item Key: {item_key}. Need to add collection separately if needed.")
                # Add to collection since it was just created
                add_existing_item_to_collection(zot, item_key, collection_id, collection_name)
            elif resp and isinstance(resp, dict) and resp.get('unchanged'):
                 item_key = resp['unchanged'][0]
                 item_created_now = False # Already existed
                 log_message(f"    INFO: Item already exists in library (unchanged). Item Key: {item_key}. Checking/adding collection.")
                 # Check if it needs adding to collection
                 add_existing_item_to_collection(zot, item_key, collection_id, collection_name)
            elif resp and isinstance(resp, dict) and resp.get('failed'):
                 log_message(f"    WARNING: Zotero ID lookup failed. Reason: {resp['failed']}")
            else:
                 log_message("    WARNING: Unexpected response from Zotero ID lookup.")
        except Exception as e:
             log_message(f"    ERROR: Exception during Zotero ID lookup: {e}")
             import traceback
             log_message(traceback.format_exc())
             if "Rate limit" in str(e):
                 log_message(f"  WARNING: Hit Zotero API rate limit during ID lookup. Waiting {rate_limit_delay}s. Item may fail.")
                 time.sleep(rate_limit_delay)

    # --- Strategy 3: Manual Creation from Original XLS (if others failed) --- #
    if item_key is None:
        log_message("  3. Attempting manual creation using original XLS data...")
        original_row, source_db = find_original_record(doi_std, title_std, authors_std, year_std, wos_orig_df, psyc_orig_df)
        if original_row is not None and source_db is not None:
            log_message(f"    Found original record in: {source_db}")
            # Pass collection_id when creating template
            zotero_template = create_template_from_original(original_row, source_db, collection_id)
            if zotero_template:
                template_source = f"Original {source_db} XLS"
                log_message(f"    Created Zotero template (incl. collection) from {template_source} data.")
                log_message(f"    Attempting Zotero item creation using template from: {template_source}")
                try:
                    resp = zot.create_items([zotero_template])
                    log_message(f"      Response from create_items: {resp}")
                    if resp['successful'] and '0' in resp['successful'] and 'key' in resp['successful']['0']:
                        item_key = resp['successful']['0']['key']
                        item_created_now = True
                        log_message(f"      SUCCESS: Item created in Zotero using {template_source} data. Item Key: {item_key}. Added to collection during creation.")
                    elif resp['failed']:
                         if '0' in resp['failed'] and resp['failed']['0']['code'] == 412: # Precondition failed (likely duplicate)
                            log_message("    INFO: Item creation failed (Code 412), likely already exists.")
                            # We could try to FIND the item here, but it's complex. Log failure.
                         else:
                             log_message(f"      ERROR: Failed Zotero create_items call. Failure: {resp['failed']}")
                    else:
                        log_message("      ERROR: Unexpected response from Zotero create_items.")
                except Exception as e:
                     log_message(f"      ERROR: Exception during Zotero create_items: {e}")
                     import traceback
                     log_message(traceback.format_exc())
                     if "Rate limit" in str(e):
                         log_message(f"  WARNING: Hit Zotero API rate limit during manual creation. Waiting {rate_limit_delay}s. Item may fail.")
                         time.sleep(rate_limit_delay)
            else:
                log_message("    ERROR: Could not create template from original row.")
        else:
            log_message("    ERROR: Failed to find original record for manual creation fallback.")

    # --- Update Counts --- #
    if item_key:
        added_count += 1
    else:
        failed_count += 1
        log_message(f"  FAILURE: Could not process/create/find paper ID {paper_id} via any method.")

    # Optional short delay to be nice to APIs
    time.sleep(0.6) # Slightly increased delay

# --- Final Summary --- #
log_message(f"\n--- Processing Finished ---")
log_message(f"Total unique records processed: {processed_count}")
log_message(f"Successfully processed (created or found): {added_count}")
log_message(f"Failed to process/create: {failed_count}")
log_message(f"Detailed log saved to: {LOG_FILE}") 