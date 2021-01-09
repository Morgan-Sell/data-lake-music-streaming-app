# Sparkify's Data Lake Deployment
## Advancing a Music Streaming Startups Data Capabilities

<img src="https://github.com/Morgan-Sell/data-lake-music-streaming-app/blob/main/img/robot_keyboard.jpg" width="800" height="250">

# Objective
As more people were home and hosting virtual dance parties during the COVID-19 pandemic, Sparkify, a fake streaming music app, experienced exponential growth. With the ever-increasing quantity of data and desire to perform more sosphiticated analysis, Sparkify's management decided to store its data in a data lake.

The startup will leverage the capabilities of Spark and Hadoop to enable distributing computing via AWS. Spark creates a cluster manager that oversees the operations of all nodes. This project's cluster mode is **standalone**.

One of the main justifications for transitioning to a data lake is that Sparkify is heavily investing in its recommender system and other ML capabilities. Management wants to develop the industry's best recommender system. By using Spark, the recommender system can be trained at subsantially faster rate.

# Project Description
I initially created a data warehouse for Sparkify to organize its music library and played songs depository. Sparkify's  team and I defined cleare rules which were enable analytics and monitoring of metrics. However, as Sparkify increased its music library and expanded its users, these rules became cumbersome by creating obstacles for data scientists and ML engineers to perform their work.

The data is now "semi-schematized". The data is schematized as it is being processed by the DS/ML team. This approach is refered to as **"schema-on-read"**.

The workflow below is a high-level sypnosis of how we migrating from a data warehouse to a data lake.

<p align="center">
    <img src="https://github.com/Morgan-Sell/data-lake-music-streaming-app/blob/main/img/etl_flow_chart.png" width="600" height="150">
</p>