import pandas as pd
file_path = 'total_players.csv'
table_data = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])
table_data.drop(['Nation', 'Pos'], axis=1, inplace=True)
table_data.drop([col for col in table_data.columns if 'Player Goalkeeping' in col], axis=1, inplace=True)
table_data.fillna(round(table_data.mean(numeric_only=True), 2), inplace=True)
table_data.to_csv('clean_players_fbref.csv', index=False, encoding='utf_8_sig')
