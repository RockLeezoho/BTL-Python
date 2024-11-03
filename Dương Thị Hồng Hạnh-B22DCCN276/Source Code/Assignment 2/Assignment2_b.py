import pandas as pd

file_path = 'results.csv'
data_table = pd.read_csv(file_path)

# filter out all columns with numeric data type
attributes = data_table.select_dtypes(include=['float', 'int']).columns.tolist()

results = []
# Calculate the median, mean, and standard deviation of each metric for players across the league
all_players_row = ['All']
for att in attributes:
   all_players_row.append(round(data_table[att].median(), 2))
   all_players_row.append(round(data_table[att].mean(), 2))
   all_players_row.append(round(data_table[att].std(), 2))
results.append(all_players_row)

#Calculate the median, mean, and standard deviation of each team
all_teams = data_table['Squad'].unique()
for team in all_teams:
   each_team_row = [team]
   team_data = data_table[data_table['Squad'] == team]

   for att in attributes:
      each_team_row.append(round(team_data[att].median(), 2))
      each_team_row.append(round(team_data[att].mean(), 2))
      each_team_row.append(round(team_data[att].std(), 2))
   
   results.append(each_team_row)

header_table =['Teams']
header_table.extend([f'{stat} of {att}' for att in attributes for stat in ['Median', 'Mean', 'Std']])

#create result DataFrame
results = pd.DataFrame(results, columns= header_table)
results.rename(columns={'Teams': ''}, inplace=True)

# write results to csv file
print('The median, mean, and standard deviation of each metric for players across the league and for each team are in results2.csv file.')
results.to_csv('results2.csv', index=True, encoding= 'utf_8_sig')
