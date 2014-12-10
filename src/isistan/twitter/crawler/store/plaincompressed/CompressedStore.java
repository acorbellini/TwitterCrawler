package isistan.twitter.crawler.store.plaincompressed;

import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;

public class CompressedStore implements TwitterCrawlerStore {

	@Override
	public void writeTweets(long user, TweetType type,
			ResponseList<Status> status) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeInfo(User status) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void addAdjacency(long u, ListType type, long[] list)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeTweetsHeader(long user, TweetType type, String header)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void finishedTweets(long user, TweetType type) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void finishedAdjacency(long user, ListType type) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLatestCrawled(long user) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Long getLatestCrawled() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserStatus getUserStatus(long u) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
