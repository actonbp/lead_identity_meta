import pandas as pd
import numpy as np

print("--- Verification Script Started ---")

# --- Configuration ---
WOS_FILE = 'WebOfScience.xls'
PSYC_FILE = 'PsycInfo.xls'
MERGED_FILE = 'merged_papers.csv'
DUPLICATES_FILE = 'duplicates_removed.csv'
EXPECTED_FINAL_COUNT = 262 # From the merge script output

# --- Helper Function for Standardization ---
def standardize_text(series):
    # Convert to string, lowercase, strip whitespace, replace 'nan' string with actual NaN
    return series.astype(str).str.lower().str.strip().replace('nan', np.nan)

def standardize_year(series):
    # Handles potential raw dates (PsycInfo) or just years (WOS)
    # Convert to string, extract first 4 chars, coerce errors to NaN, convert to Int64
    return pd.to_numeric(series.astype(str).str[:4], errors='coerce').astype('Int64')


# --- 1. Load Data ---
print(f"Loading original files: {WOS_FILE}, {PSYC_FILE}")
print(f"Loading merged file: {MERGED_FILE}")
try:
    wos_df_orig = pd.read_excel(WOS_FILE)
    psyc_df_orig = pd.read_excel(PSYC_FILE)
    merged_df = pd.read_csv(MERGED_FILE)
    duplicates_df = pd.read_csv(DUPLICATES_FILE)
    print("Files loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}. Make sure all files exist (including {DUPLICATES_FILE}).")
    exit()
except Exception as e:
    print(f"An error occurred loading files: {e}")
    exit()

# --- 2. Count Verification ---
print("\n--- Check 1: Final Record Count ---")
actual_final_count = len(merged_df)
if actual_final_count == EXPECTED_FINAL_COUNT:
    print(f"PASS: Final count matches expected count ({actual_final_count}).")
else:
    print(f"FAIL: Final count ({actual_final_count}) does not match expected count ({EXPECTED_FINAL_COUNT}).")

# --- 3. DOI Overlap Analysis ---
print("\n--- Check 2: DOI Overlap and Duplication ---")

# Standardize DOIs in original files
wos_doi_standardized = standardize_text(wos_df_orig['DOI'])
psyc_doi_standardized = standardize_text(psyc_df_orig['doi']) # Original column name

# Get sets of non-null DOIs
wos_dois = set(wos_doi_standardized.dropna())
psyc_dois = set(psyc_doi_standardized.dropna())

# Find DOIs present in both sets
overlapping_dois = wos_dois.intersection(psyc_dois)
print(f"Found {len(overlapping_dois)} DOIs present in both WOS and PsycInfo files.")

# Standardize DOIs in the merged file
merged_dois_standardized = standardize_text(merged_df['DOI'])

# Check if overlapping DOIs appear exactly once in the final file
doi_counts_in_merged = merged_dois_standardized[merged_dois_standardized.isin(overlapping_dois)].value_counts()

# Find DOIs that appear more than once
duplicated_overlapping_dois = doi_counts_in_merged[doi_counts_in_merged > 1]

if not duplicated_overlapping_dois.empty:
    print(f"FAIL: {len(duplicated_overlapping_dois)} DOIs that were in both original files appear more than once in the final merged file:")
    print(duplicated_overlapping_dois)
else:
    print("PASS: All DOIs found in both original files appear exactly once in the final merged file.")

# --- 4. Unique ID Check ---
print("\n--- Check 3: Unique Paper ID ---")
ids = merged_df['paper_id']
is_unique = ids.is_unique
is_sequential = ids.equals(pd.Series(range(1, actual_final_count + 1), name='paper_id'))

if is_unique and is_sequential:
    print(f"PASS: 'paper_id' column is unique and sequential from 1 to {actual_final_count}.")
elif not is_unique:
    print("FAIL: 'paper_id' column contains duplicate values.")
    print(ids[ids.duplicated()])
else: # Not sequential
    print("FAIL: 'paper_id' column is unique but not sequential from 1.")
    # Could add more detail here, e.g., find gaps or wrong start

# --- 5. Source DB Representation ---
print("\n--- Check 4: Source DB Representation ---")
source_counts = merged_df['Source DB'].value_counts()
has_wos = 'WOS' in source_counts
has_psycinfo = 'PsycInfo' in source_counts

if has_wos and has_psycinfo:
    print("PASS: Final data contains records originating from both 'WOS' and 'PsycInfo'.")
    print(source_counts)
elif not has_wos:
    print("FAIL: Final data is missing records originating from 'WOS'.")
elif not has_psycinfo:
    print("FAIL: Final data is missing records originating from 'PsycInfo'.")

# --- 6. Total Count Consistency Check ---
print("\n--- Check 5: Total Count Consistency ---")
wos_count = len(wos_df_orig)
psyc_count = len(psyc_df_orig)
merged_count = len(merged_df)
duplicates_count = len(duplicates_df)

initial_total = wos_count + psyc_count
final_total = merged_count + duplicates_count

print(f"Initial total (WOS + PsycInfo): {wos_count} + {psyc_count} = {initial_total}")
print(f"Final total (Merged + Duplicates): {merged_count} + {duplicates_count} = {final_total}")

if initial_total == final_total:
    print("PASS: Sum of merged and duplicate counts equals the sum of original counts.")
else:
    print("FAIL: Sum of merged and duplicate counts does NOT equal the sum of original counts.")

print("\n--- Verification Script Finished ---") 