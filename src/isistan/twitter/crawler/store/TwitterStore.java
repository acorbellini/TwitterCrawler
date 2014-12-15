package isistan.twitter.crawler.store;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.tweet.Tweet;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.util.ListReader;
import isistan.twitter.crawler.util.TweetReader;

import java.util.List;
import java.util.Map;

public interface TwitterStore {

	public abstract void close() throws Exception;

	// public abstract void saveAdjacency(long u, ListType type, long[] list)
	// throws Exception;

	public abstract void commit() throws Exception;

	public abstract void compactTweets(long user, TweetType tweets)
			throws Exception;

	public abstract long[] getAdjacency(Long user, ListType type)
			throws Exception;

	public abstract Long getLatestCrawled() throws Exception;

	public abstract List<Tweet> getTweets(long user, TweetType type)
			throws Exception;

	public abstract UserInfo getUserInfo(Long user) throws Exception;

	public abstract Map<String, String> getUserStatus(Long user)
			throws Exception;

	public abstract String getUserStatus(String k, long user) throws Exception;

	public abstract boolean hasAdjacency(Long user, ListType followees)
			throws Exception;

	public abstract boolean hasInfo(Long user) throws Exception;

	public abstract boolean hasStatus(Long user) throws Exception;

	public abstract boolean hasTweets(Long user, TweetType tweets)
			throws Exception;

	public abstract void saveAdjacency(Long user, ListType followers,
			ListReader listReader, boolean skipCheck) throws Exception;

	public abstract void saveCompactedTweets(Long user, TweetType tweets,
			TweetReader tweetReader, boolean skipCheck) throws Exception;

	public abstract void saveLatestCrawled(long user) throws Exception;

	public abstract void saveTweets(long user, TweetType type,
			List<Tweet> tweets) throws Exception;

	public abstract void saveUserInfo(UserInfo info, boolean skipCheck)
			throws Exception;

	public abstract void saveUserStatus(long user, Map<String, String> stats,
			boolean skipCheck) throws Exception;

	public abstract void setUserStatus(String k, String v, long user)
			throws Exception;

}