import pandas as pd
import matplotlib.pyplot as plt
from soccerplots.radar_chart import Radar
import argparse

"""
list of player names in "player_names.csv" file
list of attribute names in "attribute_names.csv" file

test: python radarChartPlot.py --p1 'Aaron Cresswell' --p2 'Adam Smith' --Attribute 'Age,Playing Time.MP,Playing Time.Starts,Player Goalkeeping.Performance.GA,Player Goalkeeping.Performance.GA90,Player Goalkeeping.Performance.SoTA,Player Goalkeeping.Performance.Saves,Player Goalkeeping.Performance.Save%,Player Goalkeeping.Performance.W,Player Goalkeeping.Performance.D,Player Goalkeeping.Performance.L'
"""

def load_data(file_path):
   data_table = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])
   # data_table.fillna(data_table.mean(numeric_only=True), inplace=True)
   return data_table

#Calculate values for each attribute
def get_values(data_table, player_name, attributes):
   player_data = data_table[data_table['Player'] == player_name]
   if player_data.empty:
      raise ValueError(f"Player {player_name} not found in data.")
   values = player_data[attributes].values.flatten().tolist()
   return values

#Calculate ranges for each attribute for radar plot scaling
def get_ranges(data_table, attributes):
   ranges = []
   for att in attributes:
      min_val = data_table[att].min()
      max_val = data_table[att].max()
      range_min = min_val - (min_val*.25)
      range_max = max_val + (max_val*.25)
      ranges.append((range_min, range_max))
   return ranges

#get team name
def get_team(data_table, player_name):
   player_data = data_table[data_table['Player'] == player_name]
   if player_data.empty:
      raise ValueError(f'Player {player_name} not found in data.')
   return player_data['Squad'].values[0] # players can appear multiple times

# Plot radar chart comparing two players
def plot_radar(player1, player2, attributes, data_table):
   player1_values = get_values(data_table, player1, attributes)
   player2_values = get_values(data_table, player2, attributes)
   values = [player1_values, player2_values]
   squad1 = get_team(data_table, player1)
   squad2 = get_team(data_table, player2)
   ranges = get_ranges(data_table, attributes) 

   title = dict(
   title_name= player1,
   title_color= 'red',
   subtitle_name = squad1,
   subtitle_color = 'red',
   title_name_2= player2,
   title_color_2= 'blue',
   subtitle_name_2= squad2,
   subtitle_color_2= 'blue',
   title_fontsize= 14
   )
   endnote = '2023-2024 Premier League Stats'

   radar = Radar()
   fig,ax = radar.plot_radar(ranges=ranges, 
                           params=[att.split('.')[-1] for att in attributes], 
                           values=values, radar_color=['red', 'blue'], 
                           alphas=[.75, .6], title=title, 
                           endnote=endnote, compare=True)
   plt.show()

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Plot radar chart for two football players:')
   parser.add_argument('--p1', type=str, required=True, help='player1 name')
   parser.add_argument('--p2', type=str, required=True, help='player2 name')
   parser.add_argument('--Attribute', type=str, required=True, help='list of attributes to compare')
   args = parser.parse_args()

   file_path = 'results.csv'
   data_table = load_data(file_path)

   player1 = args.p1
   player2 = args.p2
   attributes = args.Attribute.strip().split(',')
   common_attributes = [att for att in attributes 
                        if not pd.isna(data_table.loc[data_table['Player'] == player1][att].values[0]) and
                           not pd.isna(data_table.loc[data_table['Player'] == player2][att].values[0])]
   if len(common_attributes) > 2:
      plot_radar(player1, player2, common_attributes, data_table)
   else:
      raise ValueError('Player 1 and Player 2 have no common attributes so cannot be compared, or the number of common attributes >= 3.')
