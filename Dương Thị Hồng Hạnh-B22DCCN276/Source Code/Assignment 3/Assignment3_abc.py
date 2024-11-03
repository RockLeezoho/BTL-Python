import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import pandas as pd
from sklearn.decomposition import PCA
from IPython.display import clear_output
from sklearn.preprocessing import StandardScaler

def scale_data(data):
   scaler = StandardScaler()
   data_scalered = scaler.fit_transform(data)
   return data_scalered

def kmeans_algorithm(data, k):
   kmeans = KMeans(n_clusters=k, random_state=42)
   labels = kmeans.fit_predict(data)
   centroids = kmeans.cluster_centers_
   return labels, centroids

def PCA_plot_clusters(data, labels, centroids):
   pca = PCA(n_components=2)
   data_2d = pca.fit_transform(data) 
   centroids_2d = pca.transform(centroids)
   clear_output(wait=True)
   plt.figure(figsize=(10, 6))
   plt.scatter(x=data_2d[:, 0], y=data_2d[:, 1], c=labels, cmap='viridis', marker='*', s=100)
   plt.title('K-means Clustering of Players')
   plt.colorbar(label='Cluster')
   plt.scatter(x=centroids_2d[:,0], y=centroids_2d[:,1], c='red', s=100, label='Centroids')
   plt.show()

def elbow_find_best_k(data):
   K = range(1, 10)
   sum_of_squared_distances = []
   for k in K:
      kmeans = KMeans(n_clusters=k)
      kmeans = kmeans.fit(data)
      sum_of_squared_distances.append(kmeans.inertia_)
   plt.plot(K, sum_of_squared_distances, 'bx-')
   plt.xlabel('Values of K')
   plt.ylabel('Sum of Squared Distances')
   plt.title('Elbow method for optimal K')
   plt.show()


if __name__ == '__main__':
   file_path = 'results.csv'
   table_data = pd.read_csv(file_path, na_values=['N/a', 'N/A', 'NA', 'n/a', 'n/A'])
   numeric_columns = table_data.select_dtypes(include=['float', 'int']).columns.tolist()  
   data = table_data[numeric_columns]

   # handle missing values
   data = data.fillna(data.mean())
   data_scaled = scale_data(data)

   # find K centroids using Elbow method
   print('The graph determines the appropriate number of clusters for the input data:')
   elbow_find_best_k(data)

   # Create K-Means with clusters = 3
   labels, centroids = kmeans_algorithm(data_scaled, 3)

   print('Clustering chart of data points on 2D dimension:')
   PCA_plot_clusters(data_scaled, labels, centroids)


# Count number of players in each position for each cluster
   table_data['Cluster'] = labels
   print(table_data.groupby(["Cluster", "Pos"])['Player'].count())
