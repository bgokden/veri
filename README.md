# veri

Probabilistically Scaling Vector Spaces

Veri as a cluster can hold a Vector Space with fixed dimension and allows easy querying of k nearest neighbour search queries and also querying a sample space to be used in a machine learning algorithm.

Veri is currently in Alpha Stage

*Veri means data in Turkish.*

Veri is not a regular database, instead it is purely designed to be used in machine learning. It does not give any guarantee of responding with the same result every time.

In machine learning, data scientist usually convert data into a feature label vector space, when a space is ready it is almost always about writing and optimising the algorithm.

I have worked in different roles as a Data Engineer, Data scientist and a Software Developer. In many projects, I wanted a scalable approach to vector space search which is not available. I wanted to optimise the data ingestion and data querying into one tool.

Veri is meant to be scale. Each Veri instance tries to synchronise its data with other peers and keep a statistically identical subset of the general vector space.

## What does statistically identical mean?

Veri keeps the average (Center) and a histogram of distribution of data with the distance to the center (Euclidean Distance).
Every instance continue, exchanging data as long as their average and histogram are not close enough.

## Knn querying

Veri internally has a kd-tree, but it also query its neighbours and merge the result. It is very similar to map-reduce process done on the fly without planning.

When a knn query is stated, veri creates a unique id,
Starts a timer,
Then do a local kd-tree search,
Then calls its peers to do the same with a smaller timeout,
Merges results into a map,
Waits for timeout and then do a refine process on the result map,
and return.

if a search comes with the same id received, query is rejected to avoid infinite recursions. This behaviour will be replaced with cached results and checking timeout.

Every knn query has a timeout and timeout defines the precision of the result. User can trade the precision for time. In production users usually want a predictable response time. Since every Veri instance keeps a statistically identical in most classification case you will get the same result.

## High Availability

Veri has a different way of approaching high availability.
Veri as a cluster try to use all the memory it is allowed to use.
If there is enough memory, all the data is replicated to every instance.
If there is not enough memory, data is split within instances using histogram balancing.
If memory is nearly full, Veri will reject insertion requests.
So if you want more high availability, use more instances.
Currently, it is recommended to use another database for long term storage. Usually vector spaces, change over time and only the original data is kept. So I didn't implement a direct backend into it. Instead, you can regularly insert new data and evict old data. So you will keep your vector space up to date. Veri can respond queries while data being inserted or deleted, unlike most knn search systems.

TODO:
- Add Dump data function to allow machine learning algorithms to get a Sample Space.
- Add Query Caching and Return Cached Result instead of rejecting result.
- Add Internal classification endpoint.
- Authentication.
- Documentation.

Contact me for any questions: berkgokden@gmail.com
