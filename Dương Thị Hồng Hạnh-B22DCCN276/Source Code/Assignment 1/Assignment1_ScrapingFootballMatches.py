#https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats
from bs4 import BeautifulSoup, Comment
import requests
import pandas as pd
import time
import re
from dask import dataframe as dd

standard_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
            'Playing Time.MP', 
            'Playing Time.Starts', 
            'Playing Time.Min',
            'Performance.G-PK', 
            'Performance.PK', 
            'Performance.Ast', 
            'Performance.CrdY', 
            'Performance.CrdR',
            'Expected.xG', 
            'Expected.npxG', 
            'Expected.xAG',
            'Progression.PrgC', 
            'Progression.PrgP', 
            'Progression.PrgR',
            'Per 90 Minutes.Gls', 
            'Per 90 Minutes.Ast', 
            'Per 90 Minutes.G+A', 
            'Per 90 Minutes.G-PK', 
            'Per 90 Minutes.G+A-PK', 
            'Per 90 Minutes.xG', 
            'Per 90 Minutes.xAG', 
            'Per 90 Minutes.xG+xAG', 
            'Per 90 Minutes.npxG', 
            'Per 90 Minutes.npxG+xAG']

goalkeeping_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                     'Player Goalkeeping.Performance.GA', 
                     'Player Goalkeeping.Performance.GA90', 
                     'Player Goalkeeping.Performance.SoTA', 
                     'Player Goalkeeping.Performance.Saves', 
                     'Player Goalkeeping.Performance.Save%', 
                     'Player Goalkeeping.Performance.W', 
                     'Player Goalkeeping.Performance.D', 
                     'Player Goalkeeping.Performance.L', 
                     'Player Goalkeeping.Performance.CS', 
                     'Player Goalkeeping.Performance.CS%',
                     'Player Goalkeeping.Penalty Kicks.PKatt', 
                     'Player Goalkeeping.Penalty Kicks.PKA', 
                     'Player Goalkeeping.Penalty Kicks.PKsv', 
                     'Player Goalkeeping.Penalty Kicks.PKm', 
                     'Player Goalkeeping.Penalty Kicks.Save%']

shooting_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                  'Player Shooting.Standard.Gls', 
                  'Player Shooting.Standard.Sh', 
                  'Player Shooting.Standard.SoT', 
                  'Player Shooting.Standard.SoT%', 
                  'Player Shooting.Standard.Sh/90', 
                  'Player Shooting.Standard.SoT/90', 
                  'Player Shooting.Standard.G/Sh', 
                  'Player Shooting.Standard.G/SoT', 
                  'Player Shooting.Standard.Dist', 
                  'Player Shooting.Standard.FK', 
                  'Player Shooting.Standard.PK',
                  'Player Shooting.Expected.xG', 
                  'Player Shooting.Expected.npxG', 
                  'Player Shooting.Expected.npxG/Sh', 
                  'Player Shooting.Expected.G-xG', 
                  'Player Shooting.Expected.np:G-xG']

passing_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                  'Player Passing.Total.Cmp', 
                  'Player Passing.Total.Att', 
                  'Player Passing.Total.Cmp%', 
                  'Player Passing.Total.TotDist', 
                  'Player Passing.Total.PrgDist',
                  'Player Passing.Short.Cmp', 
                  'Player Passing.Short.Att', 
                  'Player Passing.Short.Cmp%',
                  'Player Passing.Medium.Cmp', 
                  'Player Passing.Medium.Att', 
                  'Player Passing.Medium.Cmp%',
                  'Player Passing.Long.Cmp', 
                  'Player Passing.Long.Att', 
                  'Player Passing.Long.Cmp%',
                  'Player Passing.Expected.Ast', 
                  'Player Passing.Expected.xAG', 
                  'Player Passing.Expected.xA', 
                  'Player Passing.Expected.A-xAG', 
                  'Player Passing.Expected.KP', 
                  'Player Passing.Expected.1/3', 
                  'Player Passing.Expected.PPA', 
                  'Player Passing.Expected.CrsPA', 
                  'Player Passing.Expected.PrgP']

pass_types_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                     'Player Pass Types.Pass Types.Live', 
                     'Player Pass Types.Pass Types.Dead', 
                     'Player Pass Types.Pass Types.FK', 
                     'Player Pass Types.Pass Types.TB', 
                     'Player Pass Types.Pass Types.Sw', 
                     'Player Pass Types.Pass Types.Crs', 
                     'Player Pass Types.Pass Types.TI', 
                     'Player Pass Types.Pass Types.CK',
                     'Player Pass Types.Corner Kicks.In', 
                     'Player Pass Types.Corner Kicks.Out', 
                     'Player Pass Types.Corner Kicks.Str',
                     'Player Pass Types.Outcomes.Cmp', 
                     'Player Pass Types.Outcomes.Off', 
                     'Player Pass Types.Outcomes.Blocks']

goal_and_shot_creation_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                                 'Player Goal and Shot Creation.SCA.SCA', 
                                 'Player Goal and Shot Creation.SCA.SCA90',
                                 'Player Goal and Shot Creation.SCA Types.PassLive', 
                                 'Player Goal and Shot Creation.SCA Types.PassDead', 
                                 'Player Goal and Shot Creation.SCA Types.TO', 
                                 'Player Goal and Shot Creation.SCA Types.Sh', 
                                 'Player Goal and Shot Creation.SCA Types.Fld', 
                                 'Player Goal and Shot Creation.SCA Types.Def',
                                 'Player Goal and Shot Creation.GCA.GCA', 
                                 'Player Goal and Shot Creation.GCA.GCA90',
                                 'Player Goal and Shot Creation.GCA Types.PassLive', 
                                 'Player Goal and Shot Creation.GCA Types.PassDead', 
                                 'Player Goal and Shot Creation.GCA Types.TO', 
                                 'Player Goal and Shot Creation.GCA Types.Sh', 
                                 'Player Goal and Shot Creation.GCA Types.Fld', 
                                 'Player Goal and Shot Creation.GCA Types.Def']

defensive_actions_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                           'Player Defensive Actions.Tackles.Tkl', 
                           'Player Defensive Actions.Tackles.TklW', 
                           'Player Defensive Actions.Tackles.Def 3rd', 
                           'Player Defensive Actions.Tackles.Mid 3rd', 
                           'Player Defensive Actions.Tackles.Att 3rd',
                           'Player Defensive Actions.Challenges.Tkl', 
                           'Player Defensive Actions.Challenges.Att', 
                           'Player Defensive Actions.Challenges.Tkl%', 
                           'Player Defensive Actions.Challenges.Lost',
                           'Player Defensive Actions.Blocks.Blocks', 
                           'Player Defensive Actions.Blocks.Sh', 
                           'Player Defensive Actions.Blocks.Pass', 
                           'Player Defensive Actions.Blocks.Int', 
                           'Player Defensive Actions.Blocks.Tkl+Int', 
                           'Player Defensive Actions.Blocks.Clr', 
                           'Player Defensive Actions.Blocks.Err']

possession_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                     'Player Possession.Touches.Touches', 
                     'Player Possession.Touches.Def Pen', 
                     'Player Possession.Touches.Def 3rd', 
                     'Player Possession.Touches.Mid 3rd', 
                     'Player Possession.Touches.Att 3rd', 
                     'Player Possession.Touches.Att Pen', 
                     'Player Possession.Touches.Live',
                     'Player Possession.Take-Ons.Att', 
                     'Player Possession.Take-Ons.Succ', 
                     'Player Possession.Take-Ons.Succ%', 
                     'Player Possession.Take-Ons.Tkld', 
                     'Player Possession.Take-Ons.Tkld%',
                     'Player Possession.Carries.Carries', 
                     'Player Possession.Carries.TotDist', 
                     'Player Possession.Carries.PrgDist', 
                     'Player Possession.Carries.PrgC', 
                     'Player Possession.Carries.1/3', 
                     'Player Possession.Carries.CPA', 
                     'Player Possession.Carries.Mis', 
                     'Player Possession.Carries.Dis',
                     'Player Possession.Receiving.Rec', 
                     'Player Possession.Receiving.PrgR']

playing_time_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                     'Player Playing Time.Starts.Starts', 
                     'Player Playing Time.Starts.Mn/Start', 
                     'Player Playing Time.Starts.Compl',
                     'Player Playing Time.Subs.Subs', 
                     'Player Playing Time.Subs.Mn/Sub', 
                     'Player Playing Time.Subs.unSub',
                     'Player Playing Time.Team Success.PPM', 
                     'Player Playing Time.Team Success.onG', 
                     'Player Playing Time.Team Success.onGA',
                     'Player Playing Time.Team Success (xG).onxG', 
                     'Player Playing Time.Team Success (xG).onxGA']

miscellaneous_stats_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s',
                              'Player Miscellaneous Stats.Performance.Fls', 
                              'Player Miscellaneous Stats.Performance.Fld', 
                              'Player Miscellaneous Stats.Performance.Off', 
                              'Player Miscellaneous Stats.Performance.Crs', 
                              'Player Miscellaneous Stats.Performance.OG', 
                              'Player Miscellaneous Stats.Performance.Recov',
                              'Player Miscellaneous Stats.Aerial Duels.Won', 
                              'Player Miscellaneous Stats.Aerial Duels.Lost', 
                              'Player Miscellaneous Stats.Aerial Duels.Won%']

general_indexes = ['Player', 'Nation', 'Pos', 'Squad', 'Age', '90s']

all_indexes = [standard_indexes, goalkeeping_indexes, shooting_indexes, passing_indexes, pass_types_indexes, goal_and_shot_creation_indexes, defensive_actions_indexes, possession_indexes, playing_time_indexes, miscellaneous_stats_indexes]

field_name = ['Player Standard Stats', 'Player Goalkeeping',   
            'Player Shooting','Player Passing', 
            'Player Pass Types', 'Player Goal and Shot Creation',
            'Player Defensive Actions', 'Player Possession', 'Player Playing Time', 'Player Miscellaneous Stats']


def get_data(url, headers, retries=5):
   for i in range(retries):
      try:
         response = requests.get(url, headers=headers, timeout=10)# 10 seconds
         if response.status_code == 200:
               return response
         elif response.status_code == 429:
               retry_after = response.headers.get('Retry-After')
               if retry_after:
                  print(f"Waiting: {retry_after} seconds")
                  time.sleep(int(retry_after))
               else:
                  print(f"Too Many Requests: {response.status_code}")
      except requests.exceptions.RequestException as e:
         print(f"Attempt {i+1} failed: {e}.")
         time.sleep(2 * i)  # Wait for 2 seconds before retrying
   return None


def table_element(order, headers, url):
   data_field = get_data(url, headers)
   data_field = re.compile("<!--|-->").sub("", data_field.text)

   try:
      #Convert Html Table to Dataframe
      table = pd.read_html(data_field, match= field_name[order], header= [0, 1])[0]

      #Standardize column names
      if isinstance(table.columns, pd.MultiIndex):
         table.columns = [f"{field_name[order]}.{level[0]}.{level[1]}" for level in table.columns]
         table.columns = [col.split('.')[-1] if ('Unnamed' in col) or ('90s' in col) else col for col in table.columns]
      else:
         table.columns = [f"{field_name[order]}.{col}" for col in table.columns]
      
      #Statistics of all players who have played more than 90 minutes in the football tournament
      if field_name[order] == 'Player Standard Stats':
         table.columns = [col.split('.', 1)[-1] if 'Player Standard Stats' in col else col for col in table.columns]

   #Fix broken column names of Passing and Defensive Actions
      if field_name[order] == 'Player Passing':
         rename_columns = {'Ast': 'Player Passing.Expected.Ast', 
                           'xAG':'Player Passing.Expected.xAG', 
                           'KP': 'Player Passing.Expected.KP', 
                           '1/3':'Player Passing.Expected.1/3', 
                           'PPA': 'Player Passing.Expected.PPA', 
                           'CrsPA': 'Player Passing.Expected.CrsPA', 
                           'PrgP': 'Player Passing.Expected.PrgP'}
         table.rename(columns=rename_columns, inplace=True)
      elif field_name[order] == 'Player Defensive Actions':
         rename_columns = {'Int': 'Player Defensive Actions.Blocks.Int', 
                           'Tkl+Int': 'Player Defensive Actions.Blocks.Tkl+Int',
                           'Clr': 'Player Defensive Actions.Blocks.Clr',
                           'Err': 'Player Defensive Actions.Blocks.Err'}
         table.rename(columns=rename_columns, inplace=True)

      #Get the required columns
      table = table[all_indexes[order]]
      
      return table
   except Exception as e:
      print(f'Table Error {field_name[order]}.')
   return None


def merge_tables_list(tables_list, conditions_join):
   #left join data tables
   tables_list = [dd.from_pandas(table, npartitions=10) for table in tables_list]

   merged_tables = tables_list[0]
   for i in range(1, len(tables_list)):
      merged_tables = dd.merge(merged_tables, tables_list[i], on=conditions_join, how='left')
      #drop duplicate rows
      merged_tables = merged_tables.drop_duplicates()

   #fill NaN values with 'N/a'
   merged_tables = merged_tables.fillna('N/a')
   merged_tables = merged_tables.compute()
   return merged_tables

def filter_players(table, item):
   table[item] = pd.to_numeric(table[item], errors='coerce')
   players_over_90s_table = table[table[item] > 1.0]
   return players_over_90s_table


def sort_table(table):
   if 'Player' not in table:
      raise ValueError('The table must contain a Player column.')
   try:
      #split First Name column
      table['First Name'] = table['Player'].str.split(' ', n=1, expand=True)[0]

      sorted_table = table.sort_values(by=['First Name', 'Age'], ascending=[True, False])
      sorted_table = sorted_table.drop(columns=['First Name'])
      return sorted_table
   except ValueError as e:
      print('Table conversion error.')
   return None


if __name__ == '__main__':
   headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'
   }
   standing_url = 'https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats'

   #Making a Get requests
   print('Getting the html content of 2023-2024 Premier League Stats web page.')
   data = get_data(standing_url, headers)

   time.sleep(60)
   soup = BeautifulSoup(data.text)
   standing_tags = soup.select('li.full.hasmore')[0]
   links = standing_tags.find_all('a')
   links.pop(2)
   links = [l.get('href') for l in links]
   player_urls = [f'http://fbref.com{l}' for l in links]

   print('Please wait, data is scraping and parsing!')
   #for loop
   data_tables = []
   for i, url in enumerate(player_urls):
      create_table = table_element(i, headers, url)
      data_tables.append(create_table)
      time.sleep(2 ** (i + 1))


   conditions_join = general_indexes
   if len(data_tables) > 0:
      result_tables = merge_tables_list(data_tables, conditions_join)
      total_players_table = result_tables

      result_tables = filter_players(result_tables, '90s')
      result_tables.drop('90s', axis=1, inplace=True)
      total_players_table.drop('90s', axis=1, inplace=True)
      result_tables = sort_table(result_tables)

      #remove multiple headers
      mask = total_players_table.iloc[:, 0] == 'Player'
      total_players_table = total_players_table[~mask]

      print('Data table of all players with more than 90 minutes of playing time in the 2023-2024 Premier League season, arranged in order of first name, if the names are the same, then arranged in order of age from oldest to youngest. This table is in results.csv file.')
      result_tables.to_csv('results.csv', index=False, encoding='utf_8_sig')
      total_players_table.to_csv('total_players.csv', index=False, encoding='utf_8_sig')
   else:
      print('No data table were found.')
   
