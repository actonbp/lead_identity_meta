import pandas as pd
import numpy as np

print("Loading data...")
try:
    wos_df = pd.read_excel('WebOfScience.xls')
    psyc_df = pd.read_excel('PsycInfo.xls')
    print("Data loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}. Make sure 'WebOfScience.xls' and 'PsycInfo.xls' are present.")
    exit()
except Exception as e:
    print(f"An error occurred loading Excel files: {e}")
    print("Ensure pandas, openpyxl, and xlrd are installed in the venv.")
    exit()

# --- Preprocessing ---
print("Preprocessing data...")

# Add source database identifiers
wos_df['Source DB'] = 'WOS'
psyc_df['Source DB'] = 'PsycInfo'

# Rename PsycInfo columns to match Web of Science where possible
psyc_df = psyc_df.rename(columns={
    'title': 'Article Title',
    'publicationDate': 'Publication Date Raw', # Keep raw date temporarily
    'source': 'Source Title',
    'doi': 'DOI'
})

# Extract Publication Year from PsycInfo's 'Publication Date Raw'
# Convert to string, take first 4 chars, convert to numeric (Int64 for NA support)
# Errors='coerce' will turn unparseable dates into NaT/NaN
psyc_df['Publication Year'] = pd.to_numeric(
    psyc_df['Publication Date Raw'].astype(str).str[:4],
    errors='coerce'
).astype('Int64') # Use Int64 to allow for Pandas NA values


# Define columns to keep from each dataframe
wos_cols_to_keep = ['Authors', 'Article Title', 'Source Title', 'Publication Year', 'DOI', 'Source DB']
psyc_cols_to_keep = ['Authors', 'Article Title', 'Source Title', 'Publication Year', 'DOI', 'Source DB'] # Matched names now

# Select only the relevant columns
wos_df = wos_df[wos_cols_to_keep]
psyc_df = psyc_df[psyc_cols_to_keep]

# Combine the dataframes
print("Combining datasets...")
combined_df = pd.concat([wos_df, psyc_df], ignore_index=True)
print(f"Total records before deduplication: {len(combined_df)}")

# --- Standardization for Deduplication ---
print("Standardizing data for deduplication...")

# Convert key columns to lowercase strings, handling potential NaN values
for col in ['DOI', 'Article Title', 'Authors']:
    combined_df[col] = combined_df[col].astype(str).str.lower().str.strip()
    # Replace 'nan' strings resulting from conversion with actual None or np.nan
    combined_df[col] = combined_df[col].replace('nan', np.nan)

# Convert Publication Year to a consistent numeric type (float for NaN handling)
# combined_df['Publication Year'] = pd.to_numeric(combined_df['Publication Year'], errors='coerce') # Already Int64, handles NA


# --- Deduplication ---
print("Identifying and removing duplicates...")

# Strategy:
# 1. Prioritize matching on DOI if available.
# 2. If DOI is missing, match on lowercase Title, lowercase Authors, and Year.

# Create a normalized DOI column (replace None/NaN with a placeholder that doesn't match anything)
combined_df['DOI_norm'] = combined_df['DOI'].fillna('__missing_doi__')

# Create a combined key for secondary matching (Title + Authors + Year)
# Convert year to string to handle potential NaN/NA consistently during concatenation
combined_df['Secondary_Key'] = combined_df['Article Title'].fillna('') + \
                              '|' + combined_df['Authors'].fillna('') + \
                              '|' + combined_df['Publication Year'].astype(str).fillna('')


# Identify duplicates: Keep the 'first' occurrence
# First sort by DOI (non-missing first), then by the secondary key to have a stable sort order
# This ensures that if a record has a DOI, it's prioritized over one without, if they are otherwise identical
combined_df = combined_df.sort_values(by=['DOI_norm', 'Secondary_Key'], na_position='last')


# Mark duplicates based on DOI first
duplicates_doi = combined_df.duplicated(subset=['DOI'], keep='first') & combined_df['DOI'].notna()


# Mark duplicates based on Secondary Key for those without a DOI
duplicates_secondary = combined_df.duplicated(subset=['Secondary_Key'], keep='first') & combined_df['DOI'].isna()


# Combine the boolean masks - an entry is a duplicate if it's marked by either DOI or secondary key logic
is_duplicate = duplicates_doi | duplicates_secondary


# Separate unique records and duplicates
deduplicated_df = combined_df[~is_duplicate].copy()
duplicates_df = combined_df[is_duplicate].copy()


print(f"Total records after deduplication: {len(deduplicated_df)}")
print(f"Total duplicate records removed: {len(duplicates_df)}")


# --- Final Steps ---
print("Assigning unique IDs and saving...")

# --- Process Unique Records ---
# Drop temporary helper columns from unique records
deduplicated_df = deduplicated_df.drop(columns=['DOI_norm', 'Secondary_Key'])

# Reset index after deduplication
deduplicated_df = deduplicated_df.reset_index(drop=True)

# Add unique paper ID (starting from 1)
deduplicated_df['paper_id'] = deduplicated_df.index + 1

# Reorder columns for clarity (put paper_id first)
final_cols = ['paper_id'] + [col for col in deduplicated_df.columns if col != 'paper_id']
final_df = deduplicated_df[final_cols]

# Save the final unique dataframe
output_file_unique = 'merged_papers.csv'
try:
    final_df.to_csv(output_file_unique, index=False, encoding='utf-8')
    print(f"Successfully saved deduplicated data with IDs to '{output_file_unique}'")
except Exception as e:
    print(f"An error occurred while saving the unique file: {e}")


# --- Process and Save Duplicate Records ---
# Drop temporary helper columns from duplicates
duplicates_df = duplicates_df.drop(columns=['DOI_norm', 'Secondary_Key'])

# Save the duplicate dataframe
output_file_duplicates = 'duplicates_removed.csv'
try:
    duplicates_df.to_csv(output_file_duplicates, index=False, encoding='utf-8')
    print(f"Successfully saved duplicate records to '{output_file_duplicates}'")
except Exception as e:
    print(f"An error occurred while saving the duplicates file: {e}")


print("Script finished.") 