import pandas as pd

file_path = 'clean_players_footballtransfers.csv'
data_player_value = pd.read_csv(file_path)
data_player_value['Player'] = data_player_value['Player'].str.strip().str.lower()
players_list = data_player_value['Player'].tolist()

file_path = 'clean_players_fbref.csv'
data_table = pd.read_csv(file_path)
data_table['Player'] = data_table['Player'].str.strip().str.lower()
data_filtered = data_table[data_table['Player'].isin(players_list)]

#merge 
merged_df = data_player_value.merge(data_filtered, on=['Player'], how='outer') #outer: lay ca hang khong co o mot trong hai
merged_df = merged_df.dropna()
merged_df.to_csv('train_test_data.csv', index=False, encoding='utf_8_sig')
