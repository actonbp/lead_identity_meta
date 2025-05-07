import pandas as pd
import math

INPUT_CSV = 'merged_papers.csv'
OUTPUT_RIS = 'zotero_import.ris'

def format_ris_entry(row):
    ris_entry = []
    ris_entry.append("TY  - JOUR") # Assuming all are journal articles

    # Handle Authors (split by semicolon, one AU per author)
    authors = row.get('Authors', '')
    if isinstance(authors, str):
        author_list = [a.strip() for a in authors.split(';') if a.strip()]
        for author in author_list:
            ris_entry.append(f"AU  - {author}")

    # Handle Title
    title = row.get('Article Title', '')
    if pd.notna(title) and title:
        ris_entry.append(f"TI  - {title}")

    # Handle Journal Name (Source Title)
    journal = row.get('Source Title', '')
    if pd.notna(journal) and journal:
        ris_entry.append(f"T2  - {journal}") # T2 or JO - T2 is often better for secondary title (journal)

    # Handle Publication Year
    year = row.get('Publication Year')
    # Check if year is not NaN/NaT and can be converted to int
    if pd.notna(year):
        try:
            # Convert potential float/Int64 to int, then string
            year_int = int(year)
            ris_entry.append(f"PY  - {year_int}")
        except (ValueError, TypeError):
            pass # Skip if year cannot be converted to int

    # Handle DOI
    doi = row.get('DOI', '')
    if pd.notna(doi) and doi:
        ris_entry.append(f"DO  - {doi}")

    # End record marker
    ris_entry.append("ER  - ")

    return "\n".join(ris_entry)


print(f"Loading data from {INPUT_CSV}...")
try:
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} records.")
except FileNotFoundError:
    print(f"Error: Input file '{INPUT_CSV}' not found.")
    exit()
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

print(f"Generating RIS data for {OUTPUT_RIS}...")
ris_output = []
for index, row in df.iterrows():
    ris_output.append(format_ris_entry(row))

# Join all entries with an extra newline between records
full_ris_content = "\n\n".join(ris_output)

print(f"Saving RIS file to {OUTPUT_RIS}...")
try:
    with open(OUTPUT_RIS, 'w', encoding='utf-8') as f:
        f.write(full_ris_content)
    print("Successfully saved RIS file.")
except Exception as e:
    print(f"Error writing RIS file: {e}")

print("Script finished.") 