# Sparkify's Data Lake Deployment
## Advancing a Music Streaming Startup's Machine Learning Capabilities

<img src="https://github.com/Morgan-Sell/data-lake-music-streaming-app/blob/main/img/robot_keyboard.jpg" width="800" height="250">

As more people were home and hosting virtual dance parties during the COVID-19 pandemic, Sparkify, a fake streaming music app, experienced exponential growth. With the ever-increasing quantity of data and desire to perform more sophisticated analysis, Sparkify's management decided to adopt a data lake as its database management system.

The startup will leverage the capabilities of Spark and Hadoop to enable distributing computing via AWS. Spark creates a cluster manager that oversees the operations of all the worker nodes. This project's cluster mode is **standalone**.

One of the main justifications for transitioning to a data lake is that Sparkify is heavily investing in its recommender system and other ML capabilities. Management wants to develop the industry's best recommender system. By using Spark, the recommender system can be trained at a substantially faster rate.

# Project Summary
I initially created a data warehouse for Sparkify to organize its music library and played songs depository. Sparkify's team and I defined clear rules that enabled analytics and monitoring of metrics. However, as Sparkify increased its music library and expanded its users, these rules became cumbersome by creating obstacles for data scientists and ML engineers to perform their work.

The data is now "semi-schematized". The data is schematized as it is being processed by the DS/ML team. This approach is referred to as **"schema-on-read"**.

The workflow below is a high-level synopsis of how we migrating from a data warehouse to a data lake.

<p align="center">
    <img src="https://github.com/Morgan-Sell/data-lake-music-streaming-app/blob/main/img/etl_flow_chart.png" width="750" height="220">
</p>

# Data Lake Design
The primary change to Sparkify's database management system is that data is distributed across various **worker nodes**, i.e., vCPUs provided by AWS. When designing, training, and deploying ML models, the time required is based on the last node completing its task. Consequently, it is important to allocate or partition the data in an equalized manner. The data allocation should be not skewed.

Consequently, we partitioned the following tables by the corresponding attributes:

- **songs_table** - year and artist_id
- **time_table** - year and month
- **songsplay_table** - year and month

# Conclusion
With Sparkify's data lake, the company is now able to focus on developing the most personalized music/media streaming experience available.Users' recommended playlists will incorporate new music as released by artists and Top 100 hits will no longer plug the playlist gaps. Sparkify's recommender system will introduce users to the music they never knew existed and from places of the world that they only know from books and movies.

# Packages
- Configparser
- Numpy
- PySpark




