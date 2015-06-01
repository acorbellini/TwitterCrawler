# TwitterCrawler

A configurable twitter crawler. Download the last release and run crawl.sh with a config file having this config:

oauthdir=a list of directories containing Twitter4J oauth files separated by , .
input=a text file containing a set of user ids
output=outputDir
threads=number of threads
// Optional: crawl=FOLLOWEES,FOLLOWERS,FAVORITES,TWEETS
// Optional: forceRecrawl=true

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

