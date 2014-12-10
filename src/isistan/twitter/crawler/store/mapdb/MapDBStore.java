package isistan.twitter.crawler.store.mapdb;

import isistan.def.utils.DEFByteBuffer;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.store.h2.Tweet;
import isistan.twitter.crawler.store.h2.TwitterStore;
import isistan.twitter.crawler.store.h2.UserInfo;
import isistan.twitterapi.util.ListReader;
import isistan.twitterapi.util.StoreUtil;
import isistan.twitterapi.util.TweetReader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import edu.jlime.util.compression.CompressionType;

public class MapDBStore implements TwitterStore {
	// private TweetDB<TweetKey> tDB = new TweetDB<>();
	// private TweetDB<TweetKey> fDB = new TweetDB<>();
	private DB adjDB;
	private DB infoDB;
	private DB tDBtemp;
	private DB fDBtemp;

	private BTreeMap<UserKey, byte[]> followees;
	private BTreeMap<UserKey, byte[]> followers;

	private BTreeMap<UserKey, byte[]> info;

	private BTreeMap<TweetKey, byte[]> tdata;
	private BTreeMap<UserKey, byte[]> tcompact;

	private BTreeMap<TweetKey, byte[]> fdata;
	private BTreeMap<UserKey, byte[]> fcompact;
	private DB fDBCompact;
	private DB tDBCompact;

	public MapDBStore(File dir) {
		tDBtemp = createDB(dir, "tweetstemp");
		tdata = tDBtemp.<TweetKey, byte[]> getTreeMap("tweetstemp");
		tDBCompact = createDB(dir, "tweets");
		tcompact = tDBCompact.<UserKey, byte[]> getTreeMap("tweets");

		fDBtemp = createDB(dir, "favoritestemp");
		fdata = fDBtemp.<TweetKey, byte[]> getTreeMap("favoritestemp");

		fDBCompact = createDB(dir, "favorites");
		fcompact = fDBCompact.<UserKey, byte[]> getTreeMap("/favorites");

		adjDB = createDB(dir, "adjacency");
		followees = adjDB.<UserKey, byte[]> getTreeMap("followees");
		followers = adjDB.<UserKey, byte[]> getTreeMap("followers");

		infoDB = createDB(dir, "userinfo");
		info = infoDB.<UserKey, byte[]> getTreeMap("info");

	}

	private DB createDB(File dir, String name) {
		return DBMaker.newFileDB(new File(dir + "/" + name))
				.mmapFileEnablePartial().transactionDisable()
				.closeOnJvmShutdown().make();
	}

	@Override
	public void saveUserInfo(UserInfo info, boolean skipChecking)
			throws Exception {
		if (info == null)
			return;

		UserKey uk = new UserKey(info.uid);
		if (this.info.containsKey(uk))
			return;

		byte[] asBytes = StoreUtil.userInfoToBytes(info);

		this.info.put(uk, CompressionType.BZIP.getComp().compress(asBytes));
		// infoDB.commit();
	}

	public void saveAdjacency(long u, ListType type, long[] list)
			throws Exception {
		if (list == null || list.length == 0)
			return;
		UserKey uk = new UserKey(u);
		BTreeMap<UserKey, byte[]> db = null;
		if (type.equals(ListType.FOLLOWEES))
			db = followees;
		else
			db = followers;
		if (db.containsKey(uk))
			return;
		DEFByteBuffer buff = new DEFByteBuffer();
		for (long l : list)
			buff.putLong(l);
		byte[] compress = CompressionType.BZIP.getComp().compress(buff.build());
		db.put(uk, compress);
		// adjDB.commit();
	}

	@Override
	public void saveTweets(long user, TweetType type, List<Tweet> tweets)
			throws Exception {
		for (Tweet tweet : tweets) {
			if (type.equals(TweetType.TWEETS))
				saveTweets(user, tweet, this.tdata);
			else
				saveTweets(user, tweet, this.fdata);
		}
		if (type.equals(TweetType.TWEETS)) {
			this.tDBtemp.commit();
		} else
			this.fDBtemp.commit();
	}

	private void saveTweets(long user, Tweet tweet,
			BTreeMap<TweetKey, byte[]> db) throws IOException {
		TweetKey tk = new TweetKey(user, tweet.tweetid);
		if (db.containsKey(tk))
			return;

		byte[] asBytes = StoreUtil.tweetToByteArray(tweet);

		synchronized (db) {
			db.put(tk, CompressionType.BZIP.getComp().compress(asBytes));
		}
	}

	@Override
	public void saveLatestCrawled(long user) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Long getLatestCrawled() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getUserStatus(String k, long user) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setUserStatus(String k, String v, long user) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		commit();

		compact();

		tDBtemp.close();
		fDBtemp.close();
		infoDB.close();
		adjDB.close();
		fDBCompact.close();
		tDBCompact.close();

	}

	private void compact() {
		tDBtemp.compact();
		fDBtemp.compact();
		infoDB.compact();
		adjDB.compact();
		fDBCompact.compact();
		tDBCompact.compact();
	}

	@Override
	public void compactTweets(long user, TweetType type) throws Exception {
		UserKey key = new UserKey(user);

		BTreeMap<TweetKey, byte[]> data = tdata;
		BTreeMap<UserKey, byte[]> compact = tcompact;

		if (type.equals(TweetType.FAVORITES)) {
			data = fdata;
			compact = fcompact;
		}
		if (compact.containsKey(key))
			return;
		DEFByteBuffer buff = new DEFByteBuffer();

		ConcurrentNavigableMap<TweetKey, byte[]> map = data.subMap(
				new TweetKey(user, 0), new TweetKey(user, Long.MAX_VALUE));
		if (map.isEmpty())
			return;

		for (Entry<TweetKey, byte[]> it : map.entrySet())
			buff.putByteArray(CompressionType.BZIP.getComp().uncompress(
					it.getValue()));

		synchronized (data) {
			map.clear();
			if (type.equals(TweetType.FAVORITES)) {
				fDBtemp.commit();
				// fDBtemp.compact();
			} else {
				tDBtemp.commit();
				// tDBtemp.compact();
			}
		}

		byte[] build = buff.build();
		insertCompact(type, user, build);

	}

	public void insertCompact(TweetType type, long u, byte[] build)
			throws IOException {
		UserKey key = new UserKey(u);
		BTreeMap<UserKey, byte[]> compact;
		if (type.equals(TweetType.FAVORITES))
			compact = fcompact;
		else
			compact = tcompact;
		if (build.length == 0)
			return;
		byte[] comp = CompressionType.BZIP.getComp().compress(build);
		if (compact.containsKey(key))
			return;

		compact.put(key, comp);

		// if (type.equals(TweetType.FAVORITES))
		// fDBCompact.commit();
		// else
		// tDBCompact.commit();
	}

	public void saveCompactedTweets(Long user, TweetType type, Set<Tweet> tweet)
			throws Exception {
		DEFByteBuffer buff = new DEFByteBuffer();
		for (Tweet t : tweet) {
			buff.putByteArray(StoreUtil.tweetToByteArray(t));
		}

		insertCompact(type, user, buff.build());

	}

	@Override
	public void commit() {
		tDBtemp.commit();
		fDBtemp.commit();
		infoDB.commit();
		adjDB.commit();
		fDBCompact.commit();
		tDBCompact.commit();

	}

	@Override
	public void saveUserStatus(long user, Map<String, String> stats,
			boolean skipChecking) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasTweets(Long user, TweetType tweets) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasAdjacency(Long user, ListType followees) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasInfo(Long user) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasStatus(Long user) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> getUserStatus(Long user) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getAdjacency(Long user, ListType type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserInfo getUserInfo(Long user) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Tweet> getTweets(long user, TweetType type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveCompactedTweets(Long user, TweetType tweets,
			TweetReader tweetReader, boolean skipChecking) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveAdjacency(Long user, ListType followers,
			ListReader listReader, boolean skipChecking) throws Exception {
		// TODO Auto-generated method stub

	}
}
