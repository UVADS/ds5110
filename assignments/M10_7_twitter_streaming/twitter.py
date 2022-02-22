##############################################################################################################
# PURPOSE: Run this code on SERVER terminal to call Twitter API and push streaming tweets  to Spark Streaming
# FILENAME: twitter.py
# LAST UPDATE: 11/9/2019
##############################################################################################################
## SERVER SECTION 1 START (COPY/PASTE/RUN THIS ENTIRE SECTION IN TERMINAL)

## LIBRARIES
import sys

# run if you needed to install packages, updating python version as needed.
# sys.path.append('/usr/local/lib/python3.6/site-packages/')

import socket
import requests
import requests_oauthlib
import json
##############################################################################################################
# SET UP TWITTER CREDENTIALS

ACCESS_TOKEN    = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
ACCESS_SECRET   = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
CONSUMER_KEY    = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
CONSUMER_SECRET = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

##############################################################################################################
## FUNCTIONS

# call Twitter API and return response for stream
def get_twitter_tweets(my_auth):
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response

# 1) take response and extract tweet text from JSON
# 2) send tweets to Spark Streaming
def send_tweets_to_spark_streaming(http_resp, tcp_conn):
	for line in http_resp.iter_lines():
		try:
			tweet_full = json.loads(line.decode('utf-8')) # decode bytes and cast to json
			tweet_text = tweet_full['text']
			tweet_data = bytes(tweet_text + "\n", 'utf-8') # encode to bytes for TCP
			tcp_conn.send(tweet_data)
			print(tweet_data)
		except:
			e = sys.exc_info()
			print("=== error: {}".format(e))
##############################################################################################################
## BUILD THE SOCKET AND MAKE THE API CONNECTION

# socket parameters
TCP_IP = "localhost"
TCP_PORT = 9009

conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
print("=== Waiting for TCP connection...")
conn, addr = s.accept()


## IMPORTANT NOTE: at this point, need to run CLIENT program
# when CLIENT connection made, continue with below, which will feed CLIENT

## SERVER SECTION 1 END
##################################
## SERVER SECTION 2 START (COPY/PASTE/RUN THIS ENTIRE SECTION IN TERMINAL)

print("=== Connection complete.")
print("=== Tweet collection starting...")
resp = get_twitter_tweets(my_auth)
send_tweets_to_spark_streaming(resp, conn)

## SERVER SECTION 2 END
##################################