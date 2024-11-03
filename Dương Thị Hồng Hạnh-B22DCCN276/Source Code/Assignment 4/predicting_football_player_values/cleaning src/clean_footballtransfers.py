import pandas as pd

if __name__ == '__main__':
   file_path = 'players_transfers2324.csv'
   data = pd.read_csv(file_path)
   filtered_data = data[['Player', 'Price']]
   filtered_data = filtered_data[filtered_data['Price'] != 0]
   filtered_data.to_csv('clean_players_footballtransfers.csv', index=False, encoding='utf_8_sig')
