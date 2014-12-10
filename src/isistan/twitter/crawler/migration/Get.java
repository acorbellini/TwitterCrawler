package isistan.twitter.crawler.migration;

import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.store.h2.H2Mode;
import isistan.twitter.crawler.store.h2.H2Store;
import isistan.twitter.crawler.store.h2.Tweet;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class Get {
	private H2Store store;

	public Get(String db) throws Exception {
		this.store = new H2Store(new File(db));
	}

	public static void main(String[] args) throws NumberFormatException,
			Exception {

		new Get(args[0]).info(Long.valueOf(args[1]));
		
	}

	private void info(Long u) throws Exception {
		System.out.println(this.store.getUserInfo(u));
	}

	private void get(Long u, Long t, String type) throws Exception {

		TweetType tt;
		if (type.equals("tweets"))
			tt = TweetType.TWEETS;
		else
			tt = TweetType.FAVORITES;
		HashMap<Long, Tweet> tweets = new HashMap<>();
		List<Tweet> tweetList = store.getTweets(u, tt);
		for (Tweet tweet : tweetList)
			tweets.put(tweet.tweetid, tweet);

		System.out.println(tweets.get(t));
	}
}
