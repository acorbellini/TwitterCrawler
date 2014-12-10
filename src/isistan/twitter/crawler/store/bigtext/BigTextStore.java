package isistan.twitter.crawler.store.bigtext;

import gnu.trove.list.array.TLongArrayList;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.store.h2.Tweet;
import isistan.twitter.crawler.store.h2.TwitterStore;
import isistan.twitter.crawler.store.h2.UserInfo;
import isistan.twitterapi.util.ListReader;
import isistan.twitterapi.util.StoreUtil;
import isistan.twitterapi.util.TweetReader;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.Pair;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.Record;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;

public class BigTextStore implements TwitterStore {

	private boolean loadMode = false;

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
				32 * 1024 * 1024);

		favs_key_format = tweetFormat();
		favs_file = createSorted(file, "favs", favs_key_format,
				32 * 1024 * 1024);

		uinfo_key_format = infoFormat();
		uinfo_file = createSorted(file, "uinfo", uinfo_key_format,
				2 * 1024 * 1024);

		followees_key_format = adjFormat();
		followees_file = createSorted(file, "followees", followees_key_format,
				32 * 1024 * 1024);

		followers_key_format = adjFormat();
		followers_file = createSorted(file, "followers", followers_key_format,
				32 * 1024 * 1024);

		status_key_format = statusFormat();
		status_file = createSorted(file, "status", status_key_format,
				2 * 1024 * 1024);
	}

	private RecordFormat statusFormat() {
		return RecordFormat.create(new String[] { "uid" },
				new FormatType<?>[] { FormatTypes.LONG.getType() },
				new String[] { "uid" });
	}

	private RecordFormat adjFormat() {
		return RecordFormat.create(new String[] { "uid", "adj" },
				new FormatType<?>[] { FormatTypes.LONG.getType(),
						FormatTypes.LONG.getType() }, new String[] { "uid",
						"adj" });
	}

	private RecordFormat infoFormat() {
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

	private SortedLevelFile createSorted(File file, String name,
			RecordFormat tweet_key_format2, int mem) throws Exception {
		return SortedLevelFile.open(file.getPath() + "/" + name,
				new LevelOptions().setFormat(tweet_key_format2)
						.setMaxMemTablesWriting(8).setMemTableSize(mem)
						.setBaseSize(10 * 1024 * 1024).setMaxLevel0Files(4)
						.setCompactLevel0Threshold(15).setMaxLevelFiles(10)
						.setMaxBlockSize(512 * 1024).setMinMergeElements(1)
						// .setAppendOnly(loadMode)
						.setCompressed(CompressionType.BZIP.getComp()));
	}

	@Override
	public void saveUserInfo(UserInfo info, boolean skipCheck) throws Exception {
		byte[] rec = uinfo_key_format.newRecord().set("uid", info.uid)
				.toByteArray();
		if (skipCheck || !uinfo_file.contains(rec))
			uinfo_file.put(rec, StoreUtil.userInfoToBytes(info));
	}

	@Override
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

	@Override
	public void saveTweets(long user, TweetType type, List<Tweet> tweets)
			throws Exception {

	}

	@Override
	public void saveLatestCrawled(long user) throws Exception {
	}

	@Override
	public Long getLatestCrawled() throws Exception {
		return null;
	}

	@Override
	public String getUserStatus(String k, long user) throws Exception {
		return null;
	}

	@Override
	public void setUserStatus(String k, String v, long user) throws Exception {
	}

	@Override
	public void saveUserStatus(long user, Map<String, String> map,
			boolean skipCheck) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		ByteBuffer buffer = new ByteBuffer();
		buffer.putMap(map);
		byte[] byteArray = rec.toByteArray();
		if (skipCheck || !status_file.contains(byteArray))
			status_file.put(byteArray, buffer.build());
	}

	@Override
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

	@Override
	public void compactTweets(long user, TweetType tweets) throws Exception {
	}

	@Override
	public void saveCompactedTweets(Long user, TweetType type,
			TweetReader tweetReader, boolean skipCheck) throws Exception {
		while (tweetReader.hasNext()) {
			Tweet tweet = (Tweet) tweetReader.next();
			saveTweet(user, type, skipCheck, tweet);
		}
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

	@Override
	public void commit() throws Exception {
		tweets_file.compact();

		favs_file.compact();

		uinfo_file.compact();

		followees_file.compact();

		followers_file.compact();

		status_file.compact();
	}

	@Override
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

	@Override
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

	@Override
	public boolean hasInfo(Long user) throws Exception {
		Record rec = uinfo_key_format.newRecord().set("uid", user);
		return uinfo_file.contains(rec.toByteArray());
	}

	@Override
	public boolean hasStatus(Long user) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		return status_file.contains(rec.toByteArray());
	}

	@Override
	public Map<String, String> getUserStatus(Long user) throws Exception {
		Record rec = status_key_format.newRecord().set("uid", user);
		byte[] stat = status_file.get(rec.toByteArray());
		if (stat == null)
			return new HashMap<>();
		ByteBuffer def = new ByteBuffer(stat);
		return def.getMap();
	}

	@Override
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

	@Override
	public UserInfo getUserInfo(Long user) throws Exception {
		Record rec = uinfo_key_format.newRecord().set("uid", user);
		byte[] val = uinfo_file.get(rec.toByteArray());
		if (val == null)
			return null;
		return StoreUtil.bytesToUserInfo(val);
	}

	@Override
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

}
