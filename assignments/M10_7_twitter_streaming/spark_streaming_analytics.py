##################################################################################################################
# PURPOSE:      Computes the frequency of Twitter hashtags in each batch of data
# INSTRUCTIONS: Run this code on CLIENT terminal to consume Twitter tweets from SERVER and return streaming analytics
# FILENAME:     spark_streaming_analytics.py
# LAST UPDATE:  4/16/21
##################################################################################################################
## CLIENT SECTION 1 START (COPY/PASTE THIS ENTIRE SECTION INTO TERMINAL)

## LIBRARIES
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext given cores and batch interval (this is an integer)
batch_interval = None

sc = SparkContext(None)

# create Streaming Context from spark context w interval size 5 seconds
ssc = None

lines = ssc.socketTextStream(None, None)

# set checkpoint for RDD recovery
ssc.checkpoint(None)

#####
# Split each line into words
words = lines.flatMap(None)

# Retain hashtags (hint:#) and map each hashtag in each batch to (hashtag,1)
hashtags = None

# Count the hashtags in the batch
wordCounts = None

# Print hashtags, counts for each batch
[None]

# Start the computation
ssc.start()  

## CLIENT SECTION 1 END
##################################################################################################################
