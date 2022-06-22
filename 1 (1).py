#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Sentiment analysis on streaming Twitter data using Spark Structured Streaming  Python


# In[7]:


import tweepy


# In[8]:


from tweepy import Stream


# In[9]:


#from tweepy.streaming import StreamListener


# In[10]:


from tweepy import OAuthHandler


# In[11]:


import socket


# In[12]:


import json


# In[13]:


consumer_key='1996HkDaF4k3quAZZrVO07JbN'
consumer_secret='5jr7PKg8w6yrb1lyZuKyVzmrLFSLhvuMtfm7vlPt3YHThzzChy'
access_token ='763942819450859520-ZlULWToD2o8pQJhSz8t175PF1Cwszk5'
access_secret='TnYobrZGSzPG1hDJckDMd4BqNGNBqkiugDNZ66fWFpsZR'


# In[17]:


class TweetsListener(tweepy.Stream):
  # tweet object listens for the tweets
  def __init__(self, csocket):
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      print("new message")
      # if tweet is longer than 140 characters
      if "extended_tweet" in msg:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['extended_tweet']['full_text']+"t_end")\
            .encode('utf-8'))         
        print(msg['extended_tweet']['full_text'])
      else:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['text']+"t_end")\
            .encode('utf-8'))
        print(msg['text'])
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True
#print(data)


# In[18]:


def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  # authentication based on the credentials
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  # start sending data from the Streaming API 
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track = keyword, languages=["en"])


# In[ ]:


if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    host = "127.0.0.1"     # Get local machine name
    port = 6666                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port
    
    print("Listening on port: %s" % str(port))
    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
    print("Received request from: " + str(addr))
    print(c)
    sendData(c)


# In[ ]:


s.listen(5)
c,addr = s.accept()
print("ok" + str(addr) )


# In[ ]:


sendData(c)


# In[ ]:


sendData( c )


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:






# In[ ]:





# In[ ]:




