package isistan.twitter.crawler.store.h2;

import gnu.trove.list.array.TLongArrayList;
import isistan.twitter.crawler.CrawlerUtil;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import isistan.twitter.crawler.store.plain.PlainTextStore;
import isistan.twitterapi.util.StoreUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;

public class DBCrawlerStore implements TwitterCrawlerStore {

	private TwitterStore store;
	private Properties ltProp;
	private String outputdir;
	private HashMap<String, DB> tempDBs = new HashMap<>();

	public DBCrawlerStore(TwitterStore store, String outputdir)
			throws FileNotFoundException, IOException {
		this.store = store;
		this.outputdir = outputdir;
		this.ltProp = CrawlerUtil.openProperties(outputdir
				+ "/LatestCrawled.prop");
	}

	private DB getDB(String name) {
		DB db = tempDBs.get(name);
		if (db == null) {
			synchronized (this) {
				db = tempDBs.get(name);
				if (db == null) {
					File file = new File(CrawlerConfiguration.getCurrent()
							.getOutputdir() + "/temp/");
					file.mkdirs();
					db = DBMaker
							.newFileDB(new File(file.getPath() + "/" + name))
							.closeOnJvmShutdown().make();
					tempDBs.put(name, db);
				}
			}
		}
		return db;
	}

	@Override
	public void writeInfo(User user) throws Exception {
		if (store.hasInfo(user.getId()))
			return;
		store.saveUserInfo(
				new UserInfo(user.getId(), user.getScreenName(),
						user.getName(), user.getDescription(), user.getLang(),
						new java.sql.Date(user.getCreatedAt().getTime()), user
								.getLocation(), user.getTimeZone(), user
								.getUtcOffset(), user.getFriendsCount(), user
								.getFollowersCount(),
						user.getFavouritesCount(), user.getListedCount(), user
								.getStatusesCount(), user.isVerified(), user
								.isProtected()), false);
		store.commit();
	}

	@Override
	public void addAdjacency(long user, ListType type, long[] list)
			throws Exception {
		if (store.hasAdjacency(user, type))
			return;
		HTreeMap<Long, Long> adjHash = getAdjHash(user, type);
		for (long l : list)
			adjHash.put(user, l);
		String k = type + "-" + user;
		getDB(k).commit();
	}

	private HTreeMap<Long, Long> getAdjHash(long user, ListType type) {

		String k = type + "-" + user;
		DB db = getDB(k);
		HTreeMap<Long, Long> adjHash = db.getHashMap(k);
		if (adjHash == null) {
			synchronized (this) {
				adjHash = db.getHashMap(k);
				if (adjHash == null)
					adjHash = db.createHashMap(type.toString())
							.<Long, Long> make();
			}
		}
		return adjHash;
	}

	@Override
	public void writeTweets(long user, TweetType type,
			ResponseList<Status> stats) throws Exception {
		if (store.hasTweets(user, type))
			return;
		HTreeMap<Long, Tweet> tweetHash = getTweetHash(user, type);
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

			tweetHash.put(tweet.tweetid, tweet);
		}
		String k = type + "-" + user;
		getDB(k).commit();
	}

	private HTreeMap<Long, Tweet> getTweetHash(long user, TweetType type) {
		String k = type + "-" + user;
		DB db = getDB(k);
		HTreeMap<Long, Tweet> tweetHash = db.getHashMap(k);
		if (tweetHash == null) {
			synchronized (this) {
				tweetHash = db.getHashMap(k);
				if (tweetHash == null)
					tweetHash = db.createHashMap(type.toString())
							.<Long, Tweet> make();
			}
		}
		return tweetHash;
	}

	@Override
	public void writeTweetsHeader(long user, TweetType type, String header) {
		// Do nothing...
	}

	@Override
	public void finishedTweets(long user, TweetType type) throws Exception {
		String k = type + "-" + user;
		if (!store.hasTweets(user, type)) {
			HTreeMap<Long, Tweet> hm = getTweetHash(user, type);
			Set<Tweet> tweets = new HashSet<>(hm.values());
			// store.saveCompactedTweets(user, type, tweets);
			store.commit();
		}
		deleteMapDB(k);
	}

	private void deleteMapDB(String k) {
		getDB(k).close();
		try {
			new File(CrawlerConfiguration.getCurrent().getOutputdir()
					+ "/temp/" + k).delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			new File(CrawlerConfiguration.getCurrent().getOutputdir()
					+ "/temp/" + k + ".t").delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			new File(CrawlerConfiguration.getCurrent().getOutputdir()
					+ "/temp/" + k + ".p").delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void finishedAdjacency(long user, ListType type) throws Exception {
		String k = type + "-" + user;
		if (!store.hasAdjacency(user, type)) {
			HTreeMap<Long, Long> hm = getAdjHash(user, type);
			TLongArrayList toadd = new TLongArrayList();
			for (Long l : hm.values())
				toadd.add(l);
			// store.saveAdjacency(user, type, toadd.toArray());
			store.commit();
		}
		deleteMapDB(k);
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

	private static class DatabaseUserStatus extends UserStatus {

		private DBCrawlerStore db;

		public DatabaseUserStatus(long u, DBCrawlerStore dbCrawlerStore) {
			super(u);
			this.db = dbCrawlerStore;
		}

		@Override
		public void set(String k, String v) throws Exception {
			HTreeMap<String, String> hash = db.getStatusHash(getUser());
			hash.put(k, v);
			String dbkey = user + "-status";
			db.getDB(dbkey).commit();
		}

		@Override
		public String get(String k) throws Exception {
			HTreeMap<String, String> hash = db.getStatusHash(getUser());
			return hash.get(k);
		}

		@Override
		public void setComplete() throws Exception {
			super.setComplete();
			db.store.saveUserStatus(getUser(), db.getStatusHash(getUser()),
					false);
			db.store.commit();
			String k = user + "-status";
			db.deleteMapDB(k);
		}

	}

	@Override
	public UserStatus getUserStatus(long u) throws Exception {
		return new DatabaseUserStatus(u, this);
	}

	public HTreeMap<String, String> getStatusHash(long user) throws Exception {
		String k = user + "-status";
		DB db = getDB(k);
		HTreeMap<String, String> statusHash = db.getHashMap(k);
		if (statusHash == null) {
			synchronized (this) {
				statusHash = db.getHashMap(k);
				if (statusHash == null) {
					statusHash = db.createHashMap(k).<String, String> make();
					statusHash.putAll(store.getUserStatus(user));
				}
			}
		}
		return statusHash;
	}

}
