# Draw the distribution histogram of each player's index in the whole tournament and each team.
import pandas as pd
from matplotlib import pyplot as plt

file_path = 'results.csv'
data_table = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])

# filter out all columns with numeric data type
attributes = data_table.select_dtypes(include=['float', 'int']).columns.tolist()

plt.style.use('fivethirtyeight')

#plot the histogram of the distribution of each player's index throughout the tournament
for att in attributes:
   index = data_table[att].dropna()
   plt.hist(index, bins=15, color='green', edgecolor='black')

   att_name = ' '.join(att.split('.'))
   plt.title(f'{att_name} Distribution')
   plt.xlabel(att_name)
   plt.ylabel('Number of Players')
   plt.show()
   
   
#plot the histogram of the distribution of each team
all_teams = data_table['Squad'].unique()
for team in all_teams:
   team_data = data_table[data_table['Squad'] == team]
   for att in attributes:
      index = data_table[att].dropna()

      plt.hist(index, bins=20, color='orange', edgecolor='black')

      att_name = ' '.join(att.split('.'))
      plt.title(f'{att_name} Distribution of {team}')
      plt.xlabel(att_name)
      plt.ylabel(f'Number of Players on {team}')
      plt.show()
