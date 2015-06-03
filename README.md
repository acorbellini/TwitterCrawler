# TwitterCrawler

A configurable twitter crawler. Download the last release and run crawl.sh with a config file having this config:

oauthdir=a list of directories containing Twitter4J oauth files separated by , .  
input=a text file containing a set of user ids  
output=outputDir  
threads=number of threads  
// Optional: crawl=FOLLOWEES,FOLLOWERS,FAVORITES,TWEETS (this is the default config)
// Optional: forceRecrawl=false(default)/true  
// "lang" is a language detected from analyzing tweets using textcat.
// Optional: lang=(it can be: english(by default), any, other language, you can try crawl a user and see the language code)
  
# Oauth Files
An oauth file must contain the following information ( you must have an account on https://apps.twitter.com/ ):  
  
debug=false  
oauth.consumerKey=consumerKey provided by "Twitter for Devs"  
oauth.consumerSecret=consumerSecret provided by "Twitter for devs"  
oauth.accessToken=access token provided by "Twitter for devs"  
oauth.accessTokenSecret=access token secret provided by "Twitter for devs"  
restBaseURL=https\://api.twitter.com/1.1/  

# Shell
While crawling, a web-shell is opened on localhost:8080. You can use the following:  
  
store.getTweets(UID, TweetType.TWEETS/FAVORITES)  
store.getAdjacency(UID, ListType.FOLLOWEES/FOLLOWEERS)  
store.getUserInfo(UID)  
store.getUserStatus(UID)  
store.getUserList()  

# Source

It's a maven project. In eclipse, import->existing maven projects. To build: Run As->maven build-> goal: package. It creates an uber-jar in the dist folder.


