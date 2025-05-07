import pandas as pd

try:
    # Load the datasets
    wos_df = pd.read_excel('WebOfScience.xls')
    psyc_df = pd.read_excel('PsycInfo.xls')

    # Print header and first 5 rows for Web of Science
    print("--- WebOfScience.xls ---")
    print("Columns:", wos_df.columns.tolist())
    print("\nHead:")
    print(wos_df.head().to_string()) # Use to_string to prevent truncation

    print("\n\n--- PsycInfo.xls ---")
    print("Columns:", psyc_df.columns.tolist())
    print("\nHead:")
    print(psyc_df.head().to_string()) # Use to_string to prevent truncation

except FileNotFoundError as e:
    print(f"Error: {e}. Make sure the files 'WebOfScience.xls' and 'PsycInfo.xls' are in the same directory.")
except Exception as e:
    print(f"An error occurred: {e}")
    print("Please ensure you have pandas and openpyxl (or xlrd for older .xls) installed: pip install pandas openpyxl") 