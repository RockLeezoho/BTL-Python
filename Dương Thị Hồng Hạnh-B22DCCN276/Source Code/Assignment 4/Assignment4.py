# https://www.footballtransfers.com/en/leagues-cups/national/uk/premier-league/transfers/2023-2024
# This web using JavaScrip => scrape dynamic site using selenium
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager  
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_data_table(url, driver):
   try:
      driver.get(url)
      #wait for the element to appear before performing the operation
      WebDriverWait(driver, 60).until(
         EC.presence_of_element_located((By.ID, 'player-table-body'))
      )

      #get the table content
      table_body = driver.find_element(By.ID, 'player-table-body')
      rows = table_body.find_elements(By.TAG_NAME, 'tr')

      data = []
      for row in rows:
         cols = row.find_elements(By.TAG_NAME, 'td')
         if cols:
            data.append([col.text for col in cols])
      
      data_table = pd.DataFrame(data, columns=['Player', 'From / To', 'Date', 'Price'])
      driver.quit()
      return data_table
   except Exception as e:
      print(f"Error fetching data from {url}: {e}")
      return pd.DataFrame()

if __name__ == '__main__':
   options = webdriver.ChromeOptions()
   options.add_argument('--headless')#no browser interface displayed
   options.add_argument('--no-sandbox')#avoid access issues
   options.add_argument('--disable-dev-shm-usage')#avoid memory errors when running automated tasks
   #start browser with Chrome loaded automatically
   driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
   url = 'https://www.footballtransfers.com/en/leagues-cups/national/uk/premier-league/transfers/2023-2024'

   data_tables = []
   for page in range(1, 19):
      page_url = f'{url}/{page}'
      table = get_data_table(page_url, driver)
      data_tables.append(table)
      time.sleep(2 * (page + 1))

   driver.quit()
   table_combined = pd.concat(data_tables, ignore_index=True)
   table_combined.to_csv('football_transfers20232024.csv', index=True, encoding='utf_8_sig')
