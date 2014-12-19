package isistan.twitter.crawler.store;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.BigTextStore;
import isistan.twitter.crawler.tweet.Tweet;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.util.CrawlerUtil;
import isistan.twitter.crawler.util.StoreUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;

public class CrawlerStore {

	BigTextStore bt;
	private Properties ltProp;

	private String outputdir;

	public CrawlerStore(BigTextStore bt, String outputdir)
			throws FileNotFoundException, IOException {
		this.bt = bt;
		this.outputdir = outputdir;
		this.ltProp = CrawlerUtil.openProperties(outputdir
				+ "/LatestCrawled.prop");
	}

	public void addAdjacency(long u, ListType type, long[] list)
			throws Exception {
		for (long l : list)
			bt.saveAdjacent(u, type, false, l);
	}

	public void finishedAdjacency(long user, ListType type) throws Exception {
	}

	public void finishedTweets(long user, TweetType type) throws Exception {
	}

	public Long getLatestCrawled() {
		String lt = ltProp.getProperty("LatestCrawled");
		if (lt != null)
			return Long.valueOf(lt);
		return null;
	}

	public UserStatus getUserStatus(long u) throws Exception {
		return new StoredUserStatus(u, this);
	}

	public void updateLatestCrawled(long user) throws Exception {
		CrawlerUtil.updateProperty("LatestCrawled", user + "", ltProp,
				outputdir + "/LatestCrawled.prop");
	}

	public void writeInfo(User user) throws Exception {
		bt.saveUserInfo(
				new UserInfo(user.getId(), user.getScreenName(),
						user.getName(), user.getDescription(), user.getLang(),
						new java.sql.Date(user.getCreatedAt().getTime()), user
								.getLocation(), user.getTimeZone(), user
								.getUtcOffset(), user.getFriendsCount(), user
								.getFollowersCount(),
						user.getFavouritesCount(), user.getListedCount(), user
								.getStatusesCount(), user.isVerified(), user
								.isProtected()), false);
	}

	public void writeTweets(long user, TweetType type,
			ResponseList<Status> stats) throws Exception {
		for (Status t : stats) {
			Status retweetedStatus = t.getRetweetedStatus();
			Tweet tweet = new Tweet(user, t.getId(), t.getCreatedAt(),
					StoreUtil.removeTags(t.getSource()), Tweet.formatHashtags(t
							.getHashtagEntities()),
					Tweet.formatMentionEntities(t.getUserMentionEntities()),
					t.getText(), t.getRetweetCount(),
					retweetedStatus != null ? retweetedStatus.getId() : -1,
					t.getFavoriteCount(), t.isFavorited(),
					t.isPossiblySensitive());
			tweet.setReply(t.getInReplyToScreenName(), t.getInReplyToUserId(),
					t.getInReplyToStatusId());
			tweet.setContributors(Arrays.toString(t.getContributors()));
			tweet.setMedia(Tweet.formatMedia(t.getMediaEntities()));
			if (t.getPlace() != null) {
				Place place = t.getPlace();
				tweet.setPlace(place.getCountry(), place.getFullName(),
						place.getName());
			} else
				tweet.setPlace("", "", "");

			bt.saveTweet(tweet.user, type, false, tweet);
		}
	}

	public void writeTweetsHeader(long user, TweetType type, String header)
			throws Exception {
	}

	public void close() throws Exception {
		bt.close();
	}
}
