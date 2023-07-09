# Social Networks
Social network analysis (SNA) is the process of investigating social structures through the use of networks and graph theory

In this problem, we are looking at the spending cycle of users on a P2P mobile payment app. Based on this, we then calculate the multiple social network metrics for the 2 million users between 2012-16.

Social Network Metrics calculated:
i) Number of friends and number of friends of friends: First, we construct a data frame that represents all friend relationships by unioning transactions from both 
perspectives (user1 to user2 and user2 to user1), and then remove duplicates. Now, that we have a data frame of unique friend relationships. To get the friends of friends, we joined the DataFrame with itself on the "friend" column and then filtered out any relationships where the friend of a friend is the original user.

ii) Clustering coefficient of a user's network: The clustering coefficient measures the degree to which a user's friends are connected to each other. To calculate the clustering coefficient for each user, we would need to define a function that computes the clustering coefficient based on the user's friends and friends of friends. The formula for the clustering coefficient involves counting the number of triangles in the network.

iii) Page rank of each user: To calculate the page rank of each user, instead of calculating the page rank for each user at each lifetime point, we can calculate the page rank once for the entire network using the GraphFrames API.
