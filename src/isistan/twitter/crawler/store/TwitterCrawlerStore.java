package isistan.twitter.crawler.store;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.tweet.TweetType;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;

public interface TwitterCrawlerStore {
	public void addAdjacency(long u, ListType type, long[] list)
			throws Exception;

	public void finishedAdjacency(long user, ListType type) throws Exception;

	public void finishedTweets(long user, TweetType type) throws Exception;

	public Long getLatestCrawled() throws Exception;

	public UserStatus getUserStatus(long u) throws Exception;

	public void updateLatestCrawled(final long user) throws Exception;

	public void writeInfo(User status) throws Exception;

	public void writeTweets(long user, TweetType type,
			ResponseList<Status> status) throws Exception;

	public void writeTweetsHeader(long user, TweetType type, String header)
			throws Exception;
}
