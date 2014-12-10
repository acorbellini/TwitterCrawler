package isistan.twitter.crawler.store.h2;

import edu.jlime.util.compression.CompressionType;
import gnu.trove.list.array.TLongArrayList;
import isistan.def.utils.DEFByteBuffer;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitterapi.util.ListReader;
import isistan.twitterapi.util.StoreUtil;
import isistan.twitterapi.util.TweetReader;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class H2Store implements TwitterStore {
	private Connection tweetConn;

	private Connection statusConn;

	private Connection infoConn;

	private Connection adjConn;

	private String mergeIntoUserInfo = "INSERT INTO USERINFO VALUES (?,?);";
	private String mergeIntoFollowers = "INSERT INTO FOLLOWERS VALUES (?,?);";
	private String mergeIntoFollowees = "INSERT INTO FOLLOWEES VALUES (?,?);";
	private String updateCrawlerStatus = "UPDATE CRAWLER_STATUS SET value=? WHERE key=?;";
	private String selectUserStatus = "SELECT data FROM USER_STATUS WHERE uid=?;";
	private String selectLastestCrawled = "SELECT value FROM CRAWLER_STATUS WHERE key=?;";
	private String mergeIntoTweetsCompact = "INSERT INTO TWEETS_COMPACT VALUES (?,?);";
	private String mergeIntoFavsCompact = "INSERT INTO FAVORITES_COMPACT VALUES (?,?);";
	private String mergeIntoTweets = "INSERT INTO TWEETS VALUES (?,?,?);";
	private String mergeIntoFavorites = "INSERT INTO FAVORITES VALUES (?,?,?);";
	private String mergeIntoUserStatus = "MERGE INTO USER_STATUS VALUES(?,?) ;";

	private String selectInfo = "SELECT data FROM USERINFO WHERE uid=?";
	private String selectFollowees = "SELECT list FROM FOLLOWEES WHERE uid=?";
	private String selectFollowers = "SELECT list FROM FOLLOWERS WHERE uid=?";
	private String selectTweetsCompact = "SELECT data FROM TWEETS_COMPACT WHERE uid=?";
	private String selectFavsCompact = "SELECT data FROM FAVORITES_COMPACT WHERE uid=?";

	String opts = "";

	public H2Store(File dbDir) throws Exception {
		this(dbDir, H2Mode.NORMAL);
	}

	public H2Store(File dbDir, H2Mode mode) throws Exception {
		switch (mode) {
		case BULK_INSERT:
			opts = ";CACHE_SIZE=1048576";
			break;
		case READ_ONLY:
			opts = ";ACCESS_MODE_DATA=r";
			break;
		default:
			opts = "";
			break;
		}

		try {
			Class.forName("org.h2.Driver");
			// Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
			// String connType = "jdbc:derby:";

			tweetConn = getDB(dbDir, "tweets");
			infoConn = getDB(dbDir, "userinfo");
			adjConn = getDB(dbDir, "adjacency");
			statusConn = getDB(dbDir, "status");

			createTables();

			PreparedStatement stmt = statusConn
					.prepareStatement("CREATE TABLE IF NOT EXISTS CRAWLER_STATUS ("
							+ "key VARCHAR NOT NULL PRIMARY KEY,                   "
							+ "value VARCHAR NOT NULL );");
			stmt.executeUpdate();
			stmt.close();

			stmt = statusConn
					.prepareStatement("CREATE TABLE IF NOT EXISTS USER_STATUS ("
							+ "uid BIGINT NOT NULL PRIMARY KEY,             "
							+ "data BINARY NOT NULL );");
			stmt.executeUpdate();
			stmt.close();
			statusConn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Connection getDB(File crawlDir, String name) throws SQLException {
		return DriverManager.getConnection("jdbc:h2:file:" + crawlDir + "/"
				+ name + opts
		//
		// +
		//
				, "sa", "");
	}

	private void createTables() throws SQLException {
		PreparedStatement stmt = tweetConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS TWEETS ("
						+ "uid BIGINT NOT NULL,        "
						+ "tweetid BIGINT NOT NULL,          "
						+ "data BINARY NOT NULL,"
						+ "PRIMARY KEY (uid, tweetid) );");
		stmt.executeUpdate();
		stmt.close();

		stmt = tweetConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS TWEETS_COMPACT ("
						+ "uid BIGINT NOT NULL PRIMARY KEY,        "
						+ "data BINARY NOT NULL                       " + ");");
		stmt.executeUpdate();
		stmt.close();

		stmt = tweetConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS FAVORITES ("
						+ "uid BIGINT NOT NULL,        "
						+ "tweetid BIGINT NOT NULL,          "
						+ "data BINARY NOT NULL                    " + ");");
		stmt.executeUpdate();
		stmt.close();

		stmt = tweetConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS FAVORITES_COMPACT ("
						+ "uid BIGINT NOT NULL PRIMARY KEY,        "
						+ "data BINARY NOT NULL                     " + ");");
		stmt.executeUpdate();
		stmt.close();

		stmt = infoConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS USERINFO ("
						+ "uid BIGINT NOT NULL PRIMARY KEY,        "
						+ "data BINARY NOT NULL" + ");");
		stmt.executeUpdate();
		stmt.close();

		stmt = adjConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS FOLLOWERS ("
						+ "uid BIGINT NOT NULL PRIMARY KEY,                   "
						+ "list BINARY NOT NULL " + ");");
		stmt.executeUpdate();
		stmt.close();

		stmt = adjConn
				.prepareStatement("CREATE TABLE IF NOT EXISTS FOLLOWEES ("
						+ "uid BIGINT NOT NULL PRIMARY KEY,                   "
						+ "list BINARY NOT NULL" + ");");
		stmt.executeUpdate();
		stmt.close();

		adjConn.commit();
		tweetConn.commit();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * isistan.twitter.crawler.store.h2.TwitterStore#saveUserInfo(isistan.twitter
	 * .crawler.store.h2.UserInfo)
	 */
	@Override
	public void saveUserInfo(UserInfo info, boolean skipChecking)
			throws SQLException {

		if (info == null)
			return;
		byte[] bzipcompress = CompressionType.BZIP.getComp().compress(
				StoreUtil.userInfoToBytes(info));
		synchronized (infoConn) {
			PreparedStatement stmt = infoConn
					.prepareStatement(mergeIntoUserInfo);
			stmt.setLong(1, info.uid);
			stmt.setBytes(2, bzipcompress);
			stmt.execute();
			stmt.close();
			infoConn.commit();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see isistan.twitter.crawler.store.h2.TwitterStore#saveAdjacency(long,
	 * isistan.twitter.crawler.ListType, long[])
	 */
	public void saveAdjacency(long u, ListType type, long[] list)
			throws SQLException {
		String sql = mergeIntoFollowers;
		if (type.equals(ListType.FOLLOWEES)) {
			sql = mergeIntoFollowees;
		}
		DEFByteBuffer buff = new DEFByteBuffer();
		for (long l : list)
			buff.putLong(l);
		byte[] compress = CompressionType.BZIP.getComp().compress(buff.build());
		synchronized (adjConn) {
			PreparedStatement stmt = adjConn.prepareStatement(sql);
			stmt.setLong(1, u);
			stmt.setBytes(2, compress);
			stmt.execute();
			stmt.close();
			adjConn.commit();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see isistan.twitter.crawler.store.h2.TwitterStore#saveTweets(long,
	 * isistan.twitter.crawler.TweetType, java.util.List)
	 */
	@Override
	public void saveTweets(long user, TweetType type, List<Tweet> tweets)
			throws Exception {
		for (Tweet tweet : tweets) {
			saveTweet(type, tweet);
		}
	}

	private void saveTweet(TweetType type, Tweet tweet) throws Exception {
		String sql = mergeIntoTweets;
		if (type.equals(TweetType.FAVORITES))
			sql = mergeIntoFavorites;
		PreparedStatement stmt = tweetConn.prepareStatement(sql);
		stmt.setLong(1, tweet.user);
		stmt.setLong(2, tweet.tweetid);
		stmt.setBytes(
				3,
				CompressionType.BZIP.getComp().compress(
						StoreUtil.tweetToByteArray(tweet)));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * isistan.twitter.crawler.store.h2.TwitterStore#saveLatestCrawled(long)
	 */
	@Override
	public void saveLatestCrawled(long user) throws Exception {
		PreparedStatement stmt = statusConn
				.prepareStatement(updateCrawlerStatus);
		stmt.setString(1, user + "");
		stmt.setString(2, "LatestCrawled");
		stmt.executeUpdate();
		stmt.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see isistan.twitter.crawler.store.h2.TwitterStore#getLatestCrawled()
	 */
	@Override
	public Long getLatestCrawled() throws Exception {

		try {
			PreparedStatement stmt = statusConn
					.prepareStatement(selectLastestCrawled);
			stmt.setString(1, "LatestCrawled");
			stmt.execute();
			ResultSet resultSet = stmt.getResultSet();
			if (!resultSet.first())
				return null;
			Long res = resultSet.getLong(1);
			stmt.close();
			statusConn.commit();
			return res;
		} catch (Exception e) {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * isistan.twitter.crawler.store.h2.TwitterStore#getUserStatus(java.lang
	 * .String, long)
	 */
	@Override
	public String getUserStatus(String k, long user) throws Exception {
		return getUserStatus(user).get(k);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * isistan.twitter.crawler.store.h2.TwitterStore#setUserStatus(java.lang
	 * .String, java.lang.String, long)
	 */
	@Override
	public synchronized void setUserStatus(String k, String v, long user)
			throws Exception {
		Map<String, String> map = getUserStatus(user);
		map.put(k, v);
		saveUserStatus(user, map, false);
	}

	@Override
	public Map<String, String> getUserStatus(Long user) throws Exception {
		try {
			PreparedStatement stmt = statusConn
					.prepareStatement(selectUserStatus);
			stmt.setLong(1, user);
			stmt.execute();
			Map<String, String> map = null;
			ResultSet rs = stmt.getResultSet();
			if (!rs.first())
				return new HashMap<>();
			byte[] mapAsBytes = rs.getBytes(1);
			if (mapAsBytes == null)
				map = new HashMap<String, String>();
			else {
				DEFByteBuffer buffer = new DEFByteBuffer(CompressionType.BZIP
						.getComp().uncompress(mapAsBytes));
				map = buffer.getMap();
			}
			stmt.close();
			return map;
		} catch (Exception e) {
			return new HashMap<>();
		}
	}

	public void close() throws Exception {
		synchronized (infoConn) {
			infoConn.close();
		}
		synchronized (tweetConn) {
			tweetConn.close();
		}
		synchronized (statusConn) {
			statusConn.close();
		}
		synchronized (adjConn) {
			adjConn.close();
		}
	}

	@Override
	public void compactTweets(long user, TweetType tweets) throws Exception {
	}

	public void saveCompactedTweets(Long user, TweetType type, Set<Tweet> tweet)
			throws Exception {

		DEFByteBuffer buff = new DEFByteBuffer();
		for (Tweet t : tweet) {
			buff.putByteArray(StoreUtil.tweetToByteArray(t));
		}
		String sql = mergeIntoTweetsCompact;
		if (type.equals(TweetType.FAVORITES))
			sql = mergeIntoFavsCompact;
		byte[] bzipcompress = CompressionType.BZIP.getComp().compress(
				buff.build());

		synchronized (tweetConn) {
			PreparedStatement stmt = tweetConn.prepareStatement(sql);
			stmt.setLong(1, user);
			stmt.setBytes(2, bzipcompress);
			stmt.execute();
			stmt.close();
			tweetConn.commit();
		}
	}

	@Override
	public void commit() throws Exception {
		// tweetConn.commit();
		// statusConn.commit();
		// adjConn.commit();
	}

	@Override
	public void saveUserStatus(long user, Map<String, String> map,
			boolean skipChecking) throws Exception {
		DEFByteBuffer buffer = new DEFByteBuffer();
		buffer.putMap(map);
		byte[] bzipcompress = CompressionType.BZIP.getComp().compress(
				buffer.build());
		synchronized (statusConn) {
			PreparedStatement stmt = statusConn
					.prepareStatement(mergeIntoUserStatus);
			stmt.setLong(1, user);
			stmt.setBytes(2, bzipcompress);
			stmt.execute();
			stmt.close();
			statusConn.commit();
		}
	}

	@Override
	public boolean hasTweets(Long user, TweetType t) throws Exception {
		String sql = "SELECT uid FROM TWEETS_COMPACT WHERE uid=?";
		if (t.equals(TweetType.FAVORITES))
			sql = "SELECT uid FROM FAVORITES_COMPACT WHERE uid=?";
		PreparedStatement stmt = tweetConn.prepareStatement(sql);
		stmt.setLong(1, user);
		return (stmt.executeQuery().first());
	}

	@Override
	public boolean hasAdjacency(Long user, ListType f) throws Exception {
		synchronized (adjConn) {
			String sql = "SELECT uid FROM FOLLOWEES WHERE uid=?";
			if (f.equals(ListType.FOLLOWERS))
				sql = "SELECT uid FROM FOLLOWERS WHERE uid=?";
			PreparedStatement stmt = adjConn.prepareStatement(sql);
			stmt.setLong(1, user);
			return (stmt.executeQuery().first());
		}
	}

	@Override
	public boolean hasInfo(Long user) throws Exception {
		synchronized (infoConn) {
			String sql = "SELECT uid FROM USERINFO WHERE uid=?";
			PreparedStatement stmt = infoConn.prepareStatement(sql);
			stmt.setLong(1, user);
			return (stmt.executeQuery().first());
		}
	}

	@Override
	public boolean hasStatus(Long user) throws Exception {
		synchronized (statusConn) {
			String sql = "SELECT uid FROM USER_STATUS WHERE uid=?";
			PreparedStatement stmt = statusConn.prepareStatement(sql);
			stmt.setLong(1, user);
			return (stmt.executeQuery().first());
		}
	}

	public List<Tweet> getTweets(long uid, TweetType type) throws SQLException {
		ArrayList<Tweet> ret = new ArrayList<>();

		String sql = selectTweetsCompact;
		if (type.equals(TweetType.FAVORITES))
			sql = selectFavsCompact;

		PreparedStatement stmt = tweetConn.prepareStatement(sql);
		stmt.setLong(1, uid);
		ResultSet rs = stmt.executeQuery();
		if (!rs.first())
			return ret;
		byte[] bytes = rs.getBytes(1);
		byte[] data = CompressionType.BZIP.getComp().uncompress(bytes);
		stmt.close();

		DEFByteBuffer buff = new DEFByteBuffer(data);
		while (buff.hasRemaining()) {
			ret.add(StoreUtil.byteArrayToTweet(buff.getByteArray()));
		}
		return ret;
	}

	public UserInfo getUserInfo(Long user) throws Exception {
		byte[] bytes = null;
		PreparedStatement stmt = infoConn.prepareStatement(selectInfo);
		stmt.setLong(1, user);
		ResultSet rs = stmt.executeQuery();
		if (!rs.first())
			return null;
		bytes = CompressionType.BZIP.getComp().uncompress(rs.getBytes(1));
		return StoreUtil.bytesToUserInfo(bytes);
	}

	public long[] getAdjacency(Long user, ListType type) throws SQLException {
		String sql = selectFollowees;
		if (type.equals(ListType.FOLLOWERS))
			sql = selectFollowers;
		PreparedStatement stmt = adjConn.prepareStatement(sql);
		stmt.setLong(1, user);
		TLongArrayList list = new TLongArrayList();
		ResultSet rs = stmt.executeQuery();
		if (!rs.first())
			return new long[] {};
		byte[] bytes = CompressionType.BZIP.getComp()
				.uncompress(rs.getBytes(1));
		DEFByteBuffer buff = new DEFByteBuffer(bytes);
		while (buff.hasRemaining()) {
			list.add(buff.getLong());
		}
		return list.toArray();
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
