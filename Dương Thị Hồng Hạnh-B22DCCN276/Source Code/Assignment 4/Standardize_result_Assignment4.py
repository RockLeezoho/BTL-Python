import pandas as pd

def convert_price(price):
   if price == 'Free': return 0
   price = price.replace('€', '').strip()
   if 'M' in price:
      price = price.replace('M', '').strip()
      return float(price) * 1000000
   price = price.replace('K', '').strip()
   return float(price) * 1000

if __name__ == '__main__':
   file_path = 'football_transfers20232024.csv'
   data = pd.read_csv(file_path)
   data['From / To'] = data['From / To'].apply(lambda x: x.replace('\n', ' / '))
   data['Player'] = data['Player'].apply(lambda x: x.strip().split('\n')[0])
   data['Price'] = data['Price'].apply(convert_price)
   data = data.drop(columns=['Unnamed: 0'])
   data.to_csv('players_transfers2324.csv', index=False, encoding='utf_8_sig')
