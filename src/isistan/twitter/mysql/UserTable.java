package isistan.twitter.mysql;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class UserTable {

	String columns = "USERID, USERINFO, TWEETS, FAVORITES, FOLLOWEES, FOLLOWERS";
	String USER = "USER";

	String FOLLOWEES = "FOLLOWEES";
	String FOLLOWERS = "FOLLOWERS";
	String FAVORITES = "FAVORITES";
	String TWEETS = "TWEETS";

	private File userFolder;
	private Long userID;

	private String userStructure = "`USERID` BIGINT NOT NULL,"
			+ "`USERINFO` VARCHAR(45) NOT NULL,"
			+ "`TWEETS` VARCHAR(45)  NOT NULL,"
			+ "`FAVORITES` VARCHAR(45)  NOT NULL,"
			+ "`FOLLOWEES` VARCHAR(45)  NOT NULL,"
			+ "`FOLLOWERS` VARCHAR(45)  NOT NULL," + "PRIMARY KEY (`USERID`)";
	private Connection conn;

	private UserTable(Connection conn, File userFolder) throws Exception {
		PlainTextExporter.createIfNotExist(conn, USER, userStructure);
		this.userFolder = userFolder;
		this.userID = Long.valueOf(userFolder.getName());
		this.conn = conn;
		// if (PlainTextExporter.contains(USER, "ORIGINALUSERID", tweeterID))
		// this.userID = PlainTextExporter.getMax(USER, "USERID");
		// else
		// throw new Exception("User Already Exists");
	}

	private void saveUser() throws Exception {

		String info = null;
		String tT = null;
		String fT = null;
		String folT = null;
		String folwrsT = null;

		if (!PlainTextExporter
				.contains(conn, USER, "USERID", userID.toString())) {
			info = PlainTextExporter.reserveTable(conn, UserInfoTable.USERINFO,
					UserInfoTable.userInfoStructure);
			tT = PlainTextExporter.reserveTable(conn, "TWEETS",
					TweetTable.tweetStructure);
			fT = PlainTextExporter.reserveTable(conn, "FAVORITES",
					TweetTable.tweetStructure);
			folT = PlainTextExporter.reserveTable(conn, "FOLLOWEES",
					FollTable.folStructure);
			folwrsT = PlainTextExporter.reserveTable(conn, "FOLLOWERS",
					FollTable.folStructure);
			String toinsert = PlainTextExporter.escape(userID.toString()) + ","
					+ PlainTextExporter.escape(info) + ","
					+ PlainTextExporter.escape(tT) + ","
					+ PlainTextExporter.escape(fT) + ","
					+ PlainTextExporter.escape(folT) + ","
					+ PlainTextExporter.escape(folwrsT);
			PlainTextExporter.insert(conn, USER, columns, toinsert);
		} else {
			Statement stmt = conn.createStatement();
			try {
				stmt.execute("SELECT " + columns
						+ " FROM USER WHERE 	USERID = " + userID);
				ResultSet rs = stmt.getResultSet();
				rs.first();
				info = rs.getString(2);
				tT = rs.getString(3);
				fT = rs.getString(4);
				folT = rs.getString(5);
				folwrsT = rs.getString(6);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				stmt.close();
			}
		}
		final UserInfoTable iT = new UserInfoTable(conn, info, userFolder, userID);
		final TweetTable tweet = new TweetTable(conn, "TWEETS", tT, userFolder,
				userID);
		final TweetTable favs = new TweetTable(conn, "FAVORITES", fT, userFolder,
				userID);
		final FollTable followees = new FollTable(conn, "FOLLOWEES", folT,
				userFolder, userID);
		final FollTable followers = new FollTable(conn, "FOLLOWERS", folwrsT,
				userFolder, userID);

		iT.saveInfo();
		tweet.saveTweets();
		favs.saveTweets();
		followees.saveFolls();
		followers.saveFolls();
	}

	public static void save(Connection conn, File f) throws Exception {
		UserTable ut = new UserTable(conn, f);
		ut.saveUser();

	}
}
