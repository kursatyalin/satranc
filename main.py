# scrape a webpage and write it to database

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pandas as pd

def scrape_webpage(url):
  """
  Scrapes a webpage and returns its content as a BeautifulSoup object.

  Args:
      url: The URL of the webpage to scrape.

  Returns:
      A BeautifulSoup object representing the webpage's content, or None if an error occurs.
  """
  try:
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

    soup = BeautifulSoup(response.content, "html.parser")
    return soup

  except requests.exceptions.RequestException as e:
    print(f"Error fetching the URL: {e}")
    return None

  except Exception as e:
    print(f"An error occurred: {e}")
    return None

# Call the function
url_to_scrape = "https://secure.tsf.org.tr/2025tyg/kayitliste"
soup = scrape_webpage(url_to_scrape)


# Write the table to a postgres database
# Database connection details
db_host = "XXXXX"
db_name = "XXXXX"
db_user = "XXXXX"
db_password = "XXXXX"

# Construct the connection string
db_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"

# Create a SQLAlchemy engine
engine = create_engine(db_string)

if soup:
    tables = soup.find_all('table')
    if len(tables) > 1:
        second_table = tables[1]
        try:
            df = pd.read_html(str(second_table))[0]
          
            # Deduplicate the DataFrame by keeping the first occurance of each duplicate row
            deduplicated_df = df.drop_duplicates()

            # Write the DataFrame to the PostgreSQL table
            deduplicated_df.to_sql("katilimci", engine, if_exists="replace", index=False) # replace your_table_name
            print("DataFrame successfully written to PostgreSQL")

        except ValueError as e:
            print(f"Error converting the second table to DataFrame: {e}")
    else:
        print("Less than two tables found on the page.")