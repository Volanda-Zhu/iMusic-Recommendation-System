# iMusic-Recommendation-System
Recommendation System has been widely used to personalize user experience on
their own musical journey. With the advent of digital content distribution and cloud
systems, we can capture user preference on an unprecedented scale. This report
focuses on the two most ubiquitous types of approaches: Collaborative filtering
and Content-based, to provide song recommendations for users based on past
listening history and song characteristics. The collaborative-filtering hypotheses
users with similar preferences now will hold similar opinions in the future. We use
the Alternating Least Square (ALS) algorithm to recommend 10 songs for the top
3 active users by learning the user’s listening habits. The content-based approach is
implemented through K-Means clustering techniques that identify the resemblance
among songs. We utilized both the structure and unstructured features to find
similarity. We retrieved the users’ top 3 songs and searched for the 4 most similar
songs within their same clusters and use Manhattan Distance as a measurement.
