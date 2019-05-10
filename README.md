# Bret Toplyn Purchase Recommender

This set of files will load purchases data from the *Online Retail.xlsx*
sheet into Kafka where they are then loaded to Elasticsearch for referencing.
This process then creates a function to recommend purchase items that are 
commonly purchased with other items.

##  Files

* Online Retail.xlsx - all retail transaction information
* Shopping_produce.py - python script to 
    * Load the retail data
    * Transform the data for later use
    * Load the data into a Kafka Producer

* Bret Toplyn Final.ipynb
    * Create a Kafka Consumer to consume messages
    * Create functiones to load consumed messages to Elasticsearch
    * Use queries to build reverse index item recommender

**Steps**
* Run docker-compose up -d for elasticsearch and expose hostnames.
* Run the shopping_produce.py script from terminal
    * enter: python shopping_produce.py
    * This code will run for roughly 30 seconds and will starting notifying you as messages are produced
* Run Bret Toplyn Final.ipynb until the *Run Until Here section
    * This will also take a while as it consumes messages and feeds them to Elasticsearch
    * Each message will include a number.  There are roughly 20k records
* Run Elasticsearch queries as necessary to recommend items
    * These are individual queries and transformations to create a recommender
    * It will also test the recommender with a score for accuracy


