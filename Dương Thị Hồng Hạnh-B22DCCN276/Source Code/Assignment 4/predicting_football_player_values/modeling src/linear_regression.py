import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

file_path = 'train_test_data.csv'
data_table = pd.read_csv(file_path)
data_table = data_table.select_dtypes(include=[np.number]).dropna(axis=1)

top_features = list((abs(data_table.corr()['Price'])).sort_values(ascending=False)[1:25].keys())

X = data_table[top_features]
y = data_table['Price']

#Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.2, random_state=42)

#LinearRegression
lr = LinearRegression()
lr.fit(X_train, y_train)

y_train_pred_lr = lr.predict(X_train)
y_test_pred_lr = lr.predict(X_test)

r2_train_lr = r2_score(y_train, y_train_pred_lr)
r2_test_lr = r2_score(y_test, y_test_pred_lr)

print(f'Linear Regression R² Train: {r2_train_lr:.2f}')
print(f'Linear Regression R² Test: {r2_test_lr:.2f}')

#Plot
print('Chart comparing actual value to predicted value of players:')
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_test_pred_lr, color='green', alpha=0.5)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], color='red', lw=2)
plt.title('Compare Actual Values With Predicted Values (Linear Regression)')
plt.xlabel('Actual Values')
plt.ylabel('Predictioin Values')
plt.axis('equal')
plt.grid(True)
plt.show()
