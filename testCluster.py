import ClusterCreator
import math

c = ClusterCreator.ClusterCreator(35484770, 100, 30, 30, 20, math.floor(131072 / 132))

leaders, clusters, l, c_id = c.create_clusters()

print(len(leaders))
print(len(clusters))
print("---")
print(clusters[0].count)
n = 0
for i in clusters:
    n += i.count
print(n)