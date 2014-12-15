package isistan.twitter.crawler.store.bigtext;

import gnu.trove.list.array.TLongArrayList;
import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.store.TwitterStore;
import isistan.twitter.crawler.tweet.Tweet;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.util.ListReader;
import isistan.twitter.crawler.util.StoreUtil;
import isistan.twitter.crawler.util.TweetReader;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.Record;
import edu.bigtextformat.record.RecordFormat;
import edu.bigtextformat.util.Pair;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;

public class BigTextStore {

	// private boolean loadMode = true;

	private SortedLevelFile tweets_file;
	private RecordFormat tweet_key_format;

	private SortedLevelFile favs_file;
	private RecordFormat favs_key_format;

	private SortedLevelFile uinfo_file;
	private RecordFormat uinfo_key_format;

	private SortedLevelFile followees_file;
	private RecordFormat followees_key_format;

	private SortedLevelFile followers_file;
	private RecordFormat followers_key_format;

	private SortedLevelFile status_file;
	private RecordFormat status_key_format;

	public BigTextStore(File file) throws Exception {
		tweet_key_format = tweetFormat();
		tweets_file = createSorted(file, "tweets", tweet_key_format,
				8 * 1024 * 1024);

		favs_key_format = tweetFormat();
		favs_file = createSorted(file, "favs", favs_key_format, 8 * 1024 * 1024);

		uinfo_key_format = infoFormat();
		uinfo_file = createSorted(file, "uinfo", uinfo_key_format,
				4 * 1024 * 1024);

		followees_key_format = adjFormat();
		followees_file = createSorted(file, "followees", followees_key_format,
				4 * 1024 * 1024);

		followers_key_format = adjFormat();
		followers_file = createSorted(file, "followers", followers_key_format,
				4 * 1024 * 1024);

		status_key_format = statusFormat();
		status_file = createSorted(file, "status", status_key_format,
				4 * 1024 * 1024);
	}

	private RecordFormat adjFormat() {
		return RecordFormat.create(new String[] { "uid", "adj" },
				new FormatType<?>[] { FormatTypes.LONG.getType(),
						FormatTypes.LONG.getType() }, new String[] { "uid",
						"adj" });
	}

	public void close() throws Exception {
		tweets_file.compact();
		tweets_file.close();
		favs_file.compact();
		favs_file.close();
		uinfo_file.compact();
		uinfo_file.close();
		followees_file.compact();
		followees_file.close();
		followers_file.compact();
		followers_file.close();
		status_file.compact();
		status_file.close();
	}

	public void commit() throws Exception {
		tweets_file.compact();

		favs_file.compact();

		uinfo_file.compact();

		followees_file.compact();

		followers_file.compact();

		status_file.compact();
	}

	public void compactTweets(long user, TweetType tweets) throws Exception {
	}

	private SortedLevelFile createSorted(File file, String name,
			RecordFormat tweet_key_format2, int mem) throws Exception {
		return SortedLevelFile.open(
				file.getPath() + "/" + name,
				new LevelOptions().setFormat(tweet_key_format2)
						.setMaxMemTablesWriting(8).setMemTableSize(mem)
						.setBaseSize(20 * 1024 * 1024).setMaxLevel0Files(4)
						.setCompactLevel0Threshold(4).setMaxLevelFiles(10)
						.setMaxBlockSize(512 * 1024)
						.setCompressed(CompressionType.BZIP.getComp()));
	}

	public long[] getAdjacency(Long user, ListType type) throws Exception {
		Iterator<Pair<byte[], byte[]>> it = null;
		if (type.equals(ListType.FOLLOWEES)) {
			it = followees_file.rangeIterator(
					followees_key_format.newRecord().set("uid", user)
							.set("adj", 0l).toByteArray(),
					followees_key_format.newRecord().set("uid", user)
							.set("adj", Long.MAX_VALUE).toByteArray());
		} else if (type.equals(ListType.FOLLOWERS)) {
			it = followers_file.rangeIterator(
					followers_key_format.newRecord().set("uid", user)
							.set("adj", 0l).toByteArray(),
					followers_key_format.newRecord().set("uid", user)
							.set("adj", Long.MAX_VALUE).toByteArray());
		}
		TLongArrayList adj = new TLongArrayList();
		while (it.hasNext()) {
			Pair<byte[], byte[]> pair = (Pair<byte[], byte[]>) it.next();
			byte[] longasbytes = null;
			if (type.equals(ListType.FOLLOWEES)) {
				longasbytes = followees_key_format
						.getData("adj", pair.getKey());
			} else if (type.equals(ListType.FOLLOWERS)) {
				longasbytes = followers_key_format
						.getData("adj", pair.getKey());
			}
			adj.add(DataTypeUtils.byteArrayToLong(longasbytes));
		}
		return adj.toArray();
	}

	public Long getLatestCrawled() throws Exception {
		return null;
	}

	public List<Tweet> getTweets(long user, TweetType type) throws Exception {
		Iterator<Pair<byte[], byte[]>> it = null;
		if (type.equals(TweetType.TWEETS)) {
			it = tweets_file.rangeIterator(
					tweet_key_format.newRecord().set("uid", user)
							.set("tid", 0l).toByteArray(),
					tweet_key_format.newRecord().set("uid", user)
							.set("tid", Long.MAX_VALUE).toByteArray());
		} else if (type.equals(TweetType.FAVORITES)) {
			it = favs_file.rangeIterator(
					favs_key_format.newRecord().set("uid", user).set("tid", 0l)
							.toByteArray(),
					favs_key_format.newRecord().set("uid", user)
							.set("tid", Long.MAX_VALUE).toByteArray());
		}
		List<Tweet> tweets = new ArrayList<Tweet>();
		while (it.hasNext()) {
			Pair<byte[], byte[]> pair = (Pair<byte[], byte[]>) it.next();
			tweets.add(StoreUtil.byteArrayToTweet(pair.getValue()));
		}
		return tweets;
	}

	public UserInfo getUserInfo(Long user) throws Exception {
		Record rec = uinfo_key_format.newRecord().set("uid", user);
		byte[] val = uinfo_file.get(rec.toByteArray());
		if (val == null)
			return null;
		return StoreUtil.bytesToUserInfo(val);
	}

	public Map<String, String> getUserStatus(Long user) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		byte[] stat = status_file.get(rec.toByteArray());
		if (stat == null)
			return new HashMap<>();
		ByteBuffer def = new ByteBuffer(stat);
		return def.getMap();
	}

	public String getUserStatus(String k, long user) throws Exception {
		return null;
	}

	public boolean hasAdjacency(Long user, ListType t) throws Exception {
		if (t.equals(ListType.FOLLOWEES)) {
			Record rec = followees_key_format.newRecord().set("uid", user);
			return followees_file.contains(rec.toByteArray());
		} else if (t.equals(ListType.FOLLOWERS)) {
			Record rec = followers_key_format.newRecord().set("uid", user);
			return followers_file.contains(rec.toByteArray());
		}
		return false;
	}

	public boolean hasInfo(Long user) throws Exception {
		Record rec = uinfo_key_format.newRecord().set("uid", user);
		return uinfo_file.contains(rec.toByteArray());
	}

	public boolean hasStatus(Long user) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		return status_file.contains(rec.toByteArray());
	}

	public boolean hasTweets(Long user, TweetType type) throws Exception {
		Iterator<Pair<byte[], byte[]>> it = null;
		if (type.equals(TweetType.TWEETS)) {
			it = tweets_file.rangeIterator(
					tweet_key_format.newRecord().set("uid", user)
							.set("tid", 0l).toByteArray(),
					tweet_key_format.newRecord().set("uid", user)
							.set("tid", Long.MAX_VALUE).toByteArray());
		} else if (type.equals(TweetType.FAVORITES)) {
			it = favs_file.rangeIterator(
					favs_key_format.newRecord().set("uid", user).set("tid", 0l)
							.toByteArray(),
					favs_key_format.newRecord().set("uid", user)
							.set("tid", Long.MAX_VALUE).toByteArray());
		}
		return it.hasNext();
	}

	private RecordFormat infoFormat() {
		return RecordFormat.create(new String[] { "uid" },
				new FormatType<?>[] { FormatTypes.LONG.getType() },
				new String[] { "uid" });
	}

	public void saveAdjacency(Long user, ListType type, ListReader listReader,
			boolean skipCheck) throws Exception {
		while (listReader.hasNext()) {
			Long f = (Long) listReader.next();
			saveAdjacent(user, type, skipCheck, f);
		}
	}

	public void saveAdjacent(Long user, ListType type, boolean skipCheck, Long f)
			throws Exception {
		if (type.equals(ListType.FOLLOWEES)) {
			byte[] asBytes = followees_key_format.newRecord().set("uid", user)
					.set("adj", f).toByteArray();
			if (skipCheck || !followees_file.contains(asBytes))
				followees_file.put(asBytes, new byte[] {});
		} else if (type.equals(ListType.FOLLOWERS)) {
			byte[] asBytes = followers_key_format.newRecord().set("uid", user)
					.set("adj", f).toByteArray();
			if (skipCheck || !followers_file.contains(asBytes))
				followers_file.put(asBytes, new byte[] {});
		}
	}

	public void saveCompactedTweets(Long user, TweetType type,
			TweetReader tweetReader, boolean skipCheck) throws Exception {
		while (tweetReader.hasNext()) {
			Tweet tweet = (Tweet) tweetReader.next();
			saveTweet(user, type, skipCheck, tweet);
		}
	}

	public void saveLatestCrawled(long user) throws Exception {
	}

	public void saveTweet(Long user, TweetType type, boolean skipCheck,
			Tweet tweet) throws Exception {
		if (type.equals(TweetType.TWEETS)) {
			byte[] byteArray = tweet_key_format.newRecord().set("uid", user)
					.set("tid", tweet.tweetid).toByteArray();
			if (skipCheck || !tweets_file.contains(byteArray))
				tweets_file.put(byteArray, StoreUtil.tweetToByteArray(tweet));
		} else if (type.equals(TweetType.FAVORITES)) {
			byte[] byteArray = favs_key_format.newRecord().set("uid", user)
					.set("tid", tweet.tweetid).toByteArray();
			if (skipCheck || !favs_file.contains(byteArray))
				favs_file.put(byteArray, StoreUtil.tweetToByteArray(tweet));
		}
	}

	public void saveTweets(long user, TweetType type, List<Tweet> tweets)
			throws Exception {

	}

	public void saveUserInfo(UserInfo info, boolean skipCheck) throws Exception {
		byte[] rec = uinfo_key_format.newRecord().set("uid", info.uid)
				.toByteArray();
		if (skipCheck || !uinfo_file.contains(rec))
			uinfo_file.put(rec, StoreUtil.userInfoToBytes(info));
	}

	public void saveUserStatus(long user, Map<String, String> map,
			boolean skipCheck) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		ByteBuffer buffer = new ByteBuffer();
		buffer.putMap(map);
		byte[] byteArray = rec.toByteArray();
		if (skipCheck || !status_file.contains(byteArray))
			status_file.put(byteArray, buffer.build());
	}

	public void setUserStatus(String k, String v, long user) throws Exception {
	}

	private RecordFormat statusFormat() {
		return RecordFormat.create(new String[] { "uid" },
				new FormatType<?>[] { FormatTypes.LONG.getType() },
				new String[] { "uid" });
	}

	private RecordFormat tweetFormat() {
		return RecordFormat.create(new String[] { "uid", "tid" },
				new FormatType<?>[] { FormatTypes.LONG.getType(),
						FormatTypes.LONG.getType() }, new String[] { "uid",
						"tid" });
	}

}
