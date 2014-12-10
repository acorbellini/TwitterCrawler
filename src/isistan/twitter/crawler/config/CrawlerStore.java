package isistan.twitter.crawler.config;

import isistan.twitter.crawler.CrawlerUtil;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import isistan.twitter.crawler.store.bigtext.BigTextStore;
import isistan.twitter.crawler.store.h2.Tweet;
import isistan.twitter.crawler.store.h2.UserInfo;
import isistan.twitter.crawler.store.plain.PlainTextStore;
import isistan.twitterapi.util.StoreUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;

public class CrawlerStore implements TwitterCrawlerStore {

	private BigTextStore bt;
	private Properties ltProp;
	private String outputdir;

	public CrawlerStore(BigTextStore bt, String outputdir)
			throws FileNotFoundException, IOException {
		this.bt = bt;
		this.outputdir = outputdir;
		this.ltProp = CrawlerUtil.openProperties(outputdir
				+ "/LatestCrawled.prop");
	}

	@Override
	public void writeTweets(long user, TweetType type,
			ResponseList<Status> stats) throws Exception {
		for (Status t : stats) {
			Status retweetedStatus = t.getRetweetedStatus();
			Tweet tweet = new Tweet(user, t.getId(), t.getCreatedAt(),
					StoreUtil.removeTags(t.getSource()),
					PlainTextStore.formatHashtags(t.getHashtagEntities()),
					PlainTextStore.formatMentionEntities(t
							.getUserMentionEntities()), t.getText(),
					t.getRetweetCount(),
					retweetedStatus != null ? retweetedStatus.getId() : -1,
					t.getFavoriteCount(), t.isFavorited(),
					t.isPossiblySensitive());
			tweet.setReply(t.getInReplyToScreenName(), t.getInReplyToUserId(),
					t.getInReplyToStatusId());
			tweet.setContributors(Arrays.toString(t.getContributors()));
			tweet.setMedia(PlainTextStore.formatMedia(t.getMediaEntities()));
			if (t.getPlace() != null) {
				Place place = t.getPlace();
				tweet.setPlace(place.getCountry(), place.getFullName(),
						place.getName());
			} else
				tweet.setPlace("", "", "");

			bt.saveTweet(tweet.user, type, false, tweet);
		}
	}

	@Override
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

	@Override
	public void addAdjacency(long u, ListType type, long[] list)
			throws Exception {
		for (long l : list)
			bt.saveAdjacent(u, type, false, l);
	}

	@Override
	public void writeTweetsHeader(long user, TweetType type, String header)
			throws Exception {
	}

	@Override
	public void finishedTweets(long user, TweetType type) throws Exception {
	}

	@Override
	public void finishedAdjacency(long user, ListType type) throws Exception {
	}

	public Long getLatestCrawled() {
		String lt = ltProp.getProperty("LatestCrawled");
		if (lt != null)
			return Long.valueOf(lt);
		return null;
	}

	@Override
	public void updateLatestCrawled(long user) throws Exception {
		CrawlerUtil.updateProperty("LatestCrawled", user + "", ltProp,
				outputdir + "/LatestCrawled.prop");
	}

	private static class StoredUserStatus extends UserStatus {

		private CrawlerStore db;

		public StoredUserStatus(long u, CrawlerStore dbCrawlerStore) {
			super(u);
			this.db = dbCrawlerStore;
		}

		@Override
		public void set(String k, String v) throws Exception {
			Map<String, String> hash = db.bt.getUserStatus(getUser());
			hash.put(k, v);
			db.bt.saveUserStatus(getUser(), hash, true);
		}

		@Override
		public String get(String k) throws Exception {
			Map<String, String> hash = db.bt.getUserStatus(getUser());
			return hash.get(k);
		}

		@Override
		public void setComplete() throws Exception {
		}

	}

	@Override
	public UserStatus getUserStatus(long u) throws Exception {
		return new StoredUserStatus(u, this);
	}

}
