import pandas as pd

file_path = 'results.csv'
data_table = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])

# find the team with the highest score in each index
# filter out all columns with numeric data type
attributes = data_table.select_dtypes(include=['float', 'int']).columns.tolist()
attributes.remove('Age')
results = []
for att in attributes:
   results.append(data_table.loc[data_table[att].idxmax(skipna=True)]['Squad'])

results = pd.DataFrame([results], columns=attributes)
print('The team with the highest score in each index is in results2_d.csv file.')
results.to_csv('result2_d.csv', index=False, encoding='utf_8_sig')

#Which team is in the best form in the Premier League 2023-2024 season?
# A team's form is assessed based on important indicators such as goals, expected goals, non-penalty goals, pass accuracy, lost possession, pass success rate, tackled success rate, etc. 
key_indicators = ['Performance', 'Expected', 'Shooting', 'Passing', 'Goal and Shot Creation', 'Defensive Actions', 'Possession']
best_form = []
for index in key_indicators:
   teams_in_index = [results[col].iloc[0] for col in results.columns if index in col]
   best_form.append(max(set(teams_in_index), key=teams_in_index.count))
print(f'Team is in the best from in the Premier League 2023-2024 season: {max(set(best_form), key=best_form.count)}.')
