import pandas as pd
import re

file_path = 'results.csv'
table_data = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])

# filter out all columns with numeric data type
numeric_columns = table_data.select_dtypes(include=['float', 'int']).columns.tolist()  

#for loop
for col in numeric_columns:
   # find the top 3 players with the highest scores for each index
   largest_top3 = table_data[['Player', 'Nation', 'Pos', 'Squad', col]].dropna().nlargest(3, col)
   largest_top3['Type'] = f'highest'

   # Find the top 3 players with the lowest scores for each index
   smallest_top3 = table_data[['Player', 'Nation', 'Pos', 'Squad', col]].dropna().nsmallest(3, col)
   smallest_top3['Type'] = f'lowest'

   # concatenate results
   results = pd.concat([largest_top3, smallest_top3])

   #Replace non-alphanumeric characters with underscore '_'
   replace_col_name = re.sub(r'[^\w]', '_', col)

   print('The top 3 players with the highest and lowest scores for each index are in <col name>_top3.csv file.')
   results.to_csv(f'{replace_col_name}_top3.csv', index=False, encoding='utf_8_sig')
