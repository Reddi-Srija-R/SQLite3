## Bank Data ETL Pipeline

This project extracts data from a webpage listing the largest banks by market capitalization, transforms the data, and loads it into a SQLite database. It also provides functionality to run SQL queries against the database.

`Features`
- Data Extraction: Fetches and parses data from a Wikipedia page listing the largest banks.
- Data Transformation: Converts market capitalization values to multiple currencies using provided exchange rates.
- Data Loading: Saves transformed data to both CSV files and an SQLite database.
- SQL Query Execution: Runs predefined SQL queries against the SQLite database.

`Working`
- Extract data from the specified URL.
- Save the extracted data to bank_data.csv.
- Transform the data using exchange rates and save it to transformed_bank_data.csv.
- Load the transformed data into an SQLite database named Banks.db.
- Execute predefined SQL queries and print the results.
- Logging: Progress and error messages are logged to code_log.txt.

  #
  Thank you !
