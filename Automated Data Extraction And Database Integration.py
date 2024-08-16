import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import numpy as np
import sqlite3

# Function to log progress
def log_progress(message):
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{time_stamp} : {message}\n"
    
    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)

# Function to extract data from webpage and return as Pandas DataFrame
def extract(url):
    try:
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table under the heading "By market capitalization"
        heading = soup.find('span', id='By_market_capitalization')
        if heading:
            table = heading.find_next('table')
            
            # Extract table data into a list of lists
            rows = table.find_all('tr')
            data = []
            for row in rows:
                cols = row.find_all(['th', 'td'])
                cols = [col.text.strip() for col in cols]
                data.append(cols)
            
            # Convert to Pandas DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Check for expected columns
            expected_columns = ['Rank', 'Bank name', 'Market cap(US$ billion)']
            if not all(col in df.columns for col in expected_columns):
                raise ValueError("Expected columns not found in extracted data.")
            
            # Rename columns to standardize names
            df.rename(columns={
                'Rank': 'Rank',
                'Bank name': 'Name',
                'Market cap(US$ billion)': 'Market Cap'
            }, inplace=True)
            
            # Clean 'Market Cap' column
            df['Market Cap'] = df['Market Cap'].str.replace(',', '').astype(float)
            
            return df
        else:
            raise ValueError("Could not find heading 'By market capitalization' on the page.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching webpage: {e}")
        log_progress(f"Error fetching webpage: {e}")
    except Exception as e:
        print(f"Error extracting data: {e}")
        log_progress(f"Error extracting data: {e}")

# Function to perform transformation on DataFrame
def transform(df, exchange_rate_file):
    try:
        # Read exchange rate data from CSV into a DataFrame
        exchange_rate_df = pd.read_csv(exchange_rate_file, header=None, names=['Currency', 'Rate'])
        
        # Convert DataFrame to dictionary
        exchange_rate = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
        
        # Add new columns to DataFrame
        df['MC_USD_Billion'] = df['Market Cap'] / 1e9
        df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('GBP', 1), 2)
        df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('EUR', 1), 2)
        df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('INR', 1), 2)
        
        log_progress("Transformation process complete.")
        
        return df
    
    except Exception as e:
        print(f"Error during transformation: {e}")
        log_progress(f"Error during transformation: {e}")

# Function to save DataFrame to CSV
def load_to_csv(df, output_file):
    try:
        df.to_csv(output_file, index=False)
        log_progress(f"Data saved to CSV file: {output_file}")
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
        log_progress(f"Error saving DataFrame to CSV: {e}")

# Function to load DataFrame to SQLite database
def load_to_db(conn, table_name, df):
    try:
        # Ensure all column names are converted to valid SQLite column names
        df.columns = df.columns.str.replace(' ', '_')  # Replace spaces with underscores
        
        # Load DataFrame to SQLite
        df.to_sql(table_name, conn, index=False, if_exists='replace', dtype={
            'Rank': 'INTEGER',
            'Name': 'TEXT',
            'Market_Cap': 'REAL',
            'MC_USD_Billion': 'REAL',
            'MC_GBP_Billion': 'REAL',
            'MC_EUR_Billion': 'REAL',
            'MC_INR_Billion': 'REAL'
        })
        
        log_progress(f"Data loaded to Database as table '{table_name}', Executing queries.")
    
    except Exception as e:
        print(f"Error loading DataFrame to DB: {e}")
        log_progress(f"Error loading DataFrame to DB: {e}")

# Function to run SQL queries
def run_queries(conn, queries):
    try:
        cursor = conn.cursor()
        
        for query in queries:
            print(f"Executing query: {query}")
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            print()
        
        log_progress("Executed all queries successfully.")
    
    except Exception as e:
        print(f"Error executing query: {e}")
        log_progress(f"Error executing query: {e}")

# Main function for script execution
def main():
    # URL of the webpage to extract data from
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    
    # Exchange rate CSV file
    exchange_rate_file = 'exchange_rates.csv'  # Replace with your exchange rate CSV file
    
    # SQLite database connection
    conn = sqlite3.connect('Banks.db')
    
    # Table name in the SQLite database
    table_name = 'Largest_banks'
    
    # Queries to execute
    queries = [
        "SELECT * FROM Largest_banks",
        "SELECT AVG(MC_USD_Billion) FROM Largest_banks",
        "SELECT Name FROM Largest_banks LIMIT 5"
    ]
    
    try:
        # Extract data from webpage
        df = extract(url)
        
        if df is not None:
            # Save extracted data to CSV
            load_to_csv(df, "bank_data.csv")
            
            # Perform transformation on extracted data
            transformed_df = transform(df, exchange_rate_file)
            
            if transformed_df is not None:
                # Print or further process transformed DataFrame
                print(transformed_df.head())
                
                # Save transformed data to CSV using load_to_csv function
                load_to_csv(transformed_df, "transformed_bank_data.csv")
                
                # Load transformed data to SQLite database using load_to_db function
                load_to_db(conn, table_name, transformed_df)
                
                # Run SQL queries
                run_queries(conn, queries)
                
            else:
                print("Transformation failed.")
        
        else:
            print("Data extraction failed.")
    
    except Exception as e:
        print(f"Error: {e}")
        log_progress(f"Error: {e}")
    
    finally:
        # Close database connection
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
