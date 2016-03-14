# K-Means-Clustering
Hadoop Map Reduce was used to perform k-means clustering, which aims to partition n observations into k clusters in which each observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster. The problem is computationally difficult (NP-hard).

# Author
Gaurav Keswani (gk368@cornell.edu)

# Description
The k-means clustering algorithm is implemented based on below idea: 
  1. Pick k points to serve as the initial cluster centroids
  2. For every point Pk, find the closes centroid Ci (using Euclidean distance) and associate it with Ci 
  3. Update the Ci's by taking all points associated with each Ci in the previous step and setting the new Ci's to the mean of the points associated with it 
  4. Repeat for a specific number of iterations or until the centroids stop changing, whichever comes first

