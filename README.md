# veri

Probabilistically scaling vector spaces

Veri as a cluster can hold a Vector Space with fixed dimension and allows easy querying of k nearest neighbour search queries and also querying a sample space to be used in a machine learning algorithm.

*Veri means data in Turkish.*

Veri is not a regular database, instead it is purely designed to be used in machine learning. It does not give any guarantee of responding with the same result every time.

In machine learning, data scientist usually convert data into a feature label vector space, when a space is ready it is almost always about writing and optimising the algorithm.

I have worked in different roles as a Data Engineer, Data scientist and a Software Developer. In many projects, I wanted a scalable approach to vector space search which is not available. I wanted to optimise the data ingestion and data querying into one tool.

Veri is meant to be scale. Each Veri instance tries to synchronise its data with other peers and keep a statistically identical subset of the general vector space.

## What does statistically identical mean?

Veri keeps the average (Center) and a histogram of distribution of data to the distance to the center.
Every instance continue, exchanging data as long as their average and histogram are not close enough.

## Knn querying

Veri internally has a kd-tree, put it also query its neighbours and merge the result. It is very similar to map-reduce process done on the fly without planning.

When a knn query is stated, veri creates a unique id,
Starts a timer,
Then do a local kd-tree search,
Then calls its peers to do the same with a smaller timeout,
Merges results into a map,
Waits for timeout and then do a refine process on the result map,
and return.

if a search comes with the same id received, query is rejected to avoid infinite recursions. This behaviour will be replaced with cached results and checking timeout.

Every knn query has a timeout and timeout defines the precision of the result. User can trade the precision for time. In production users usually want a predictable response time. Since every Veri instance keeps a statistically identical in most classification case you will get the same result.
