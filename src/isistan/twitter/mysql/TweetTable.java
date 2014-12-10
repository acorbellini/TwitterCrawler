package isistan.twitter.mysql;

import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.CSVBuilder.RowListener;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;

public class TweetTable {

	private static final String SEP = ";";

	private String name;

	public static final String tweetStructure = ""
			+ "USERID BIGINT  NOT NULL," + "TWEETID VARCHAR(30)  NOT NULL,"
			+ "CREATED VARCHAR(100) ," + "CONTRIBUTORS VARCHAR(300) ,"
			+ "INREPLYTOUSERID  BIGINT ,"
			+ "INREPLYTOUSERSCREENNAME VARCHAR(45)  ,"
			+ "INREPLYTOTWEETID  VARCHAR(30)," + "RETWEETID   BIGINT,"
			+ "RETWEETCOUNT  INT ," + "SOURCE VARCHAR(300)  ,"
			+ "GEO_LAT FLOAT  ," + "GEO_LONG FLOAT  ,"
			+ "HASHTAGS VARCHAR(1500)  ," + "MEDIA VARCHAR(1500) ,"
			+ "PLACE_BOUNDING_BOX VARCHAR(100)  ,"
			+ "PLACE_COUNTRY VARCHAR(50)  ,"
			+ "PLACE_COUNTRY_CODE VARCHAR(100) ,"
			+ "PLACE_FULL_NAME VARCHAR(300)  ,"
			+ "PLACE_GEOMETRY_TYPE VARCHAR(100)  ,"
			+ "PLACE_ID VARCHAR(45)  ," + "PLACE_NAME VARCHAR(100)  ,"
			+ "PLACE_STREET_ADDR VARCHAR(200)  ,"
			+ "PLACE_BOUNDING_BOX_COORD VARCHAR(200)  ,"
			+ "MENTIONS VARCHAR(1000)  ," + "TEXT VARCHAR(1000), "
			+ "PRIMARY KEY (USERID, TWEETID)";
	
	protected static final int MAX_TWEETS_PER_INSERT = 5000;

	private String tweetPath;

	private Long userID;

	private String columns = "USERID,TWEETID," + "CREATED," + "CONTRIBUTORS,"
			+ "INREPLYTOUSERID," + "INREPLYTOUSERSCREENNAME,"
			+ "INREPLYTOTWEETID," + "RETWEETID," + "RETWEETCOUNT," + "SOURCE,"
			+ "GEO_LAT," + "GEO_LONG," + "HASHTAGS," + "MEDIA,"
			+ "PLACE_BOUNDING_BOX," + "PLACE_COUNTRY," + "PLACE_COUNTRY_CODE,"
			+ "PLACE_FULL_NAME," + "PLACE_GEOMETRY_TYPE," + "PLACE_ID,"
			+ "PLACE_NAME," + "PLACE_STREET_ADDR,"
			+ "PLACE_BOUNDING_BOX_COORD," + "MENTIONS," + "TEXT";

	private Connection conn;

	public TweetTable(Connection conn, String tweetType, String table,
			File userFolder, Long userID) throws Exception {
		this.conn = conn;
		this.userID = userID;
		this.name = table;
		this.tweetPath = userFolder + "/" + userID + "-" + tweetType + ".dat";
	}

	public String getTableName() {
		return name;
	}

	public void saveTweets() throws Exception {
		File tweetF = new File(tweetPath);
		if (!tweetF.exists()) {
			// System.out.println("Tweet file " + tweetPath +
			// " does not exist.");
			return;
		}
		Files.createDirectories(Paths.get("C:/TweeterCrawlerTemp/"));

		File tmpTweets = new File("C:/TweeterCrawlerTemp/" + name + "-"
				+ userID + ".tmp");
		//
		// final BufferedWriter tmpWriter = new BufferedWriter(new FileWriter(
		// tmpTweets));

		final PrintWriter tmpWriter = new PrintWriter(tmpTweets, "UTF-8");

		// final ArrayList<String> values = new ArrayList<>();

		CSVBuilder builder = new CSVBuilder(tweetF);
		builder.setExpectedFields(24);
		builder.setReplaceString("GeoLocation;@", "");
		builder.setReplaceString("&#114;", "r");
		builder.setReplaceString("&#105;", "i");
		builder.setReplaceString("&#111;", "o");
		builder.setReplaceString("&#116;", "e");
		builder.read(new RowListener() {
			@Override
			public void onNewRow(String[] toInsert) {
				if (toInsert[0].equals("ID")) // Has header
					return;
				try {
					tmpWriter.write(userID.toString() + SEP);
					boolean start = true;
					for (int i = 0; i < toInsert.length; i++) {
						String field = toInsert[i];
						if (i == 3 || i == 6 || i == 7 || i == 9 || i == 10) {
							if (field.isEmpty())
								field = "\\N";
						} else
							field = PlainTextExporter.escape(field, false);

						if (start) {
							start = false;
							tmpWriter.write(field);
						} else
							tmpWriter.write(SEP + field);
					}
					tmpWriter.write("\n");
				} catch (Exception e) {
					// TODO: handle exception
				}
				// StringBuilder builder = new StringBuilder(size);

				// values.add(builder.toString());
				// if (values.size() > MAX_TWEETS_PER_INSERT) {
				// try {
				// PlainTextExporter.insert(conn, name, columns, values);
				// } catch (SQLException e) {
				// System.out.println("Error inserting tweets for user "
				// + userID + ". Message:  " + e.getMessage());
				// }
				// values.clear();
				// }
			}
		});
		tmpWriter.close();
		try {
			PlainTextExporter.loadData(conn, name, tmpTweets.getAbsolutePath()
					.replaceAll("\\\\", "/"));
		} catch (Exception e) {
			System.out.println("Error inserting tweets for user " + userID
					+ ". Message:  " + e.getMessage());
		}
		tmpTweets.delete();

		// if (!values.isEmpty())
		// try {
		// PlainTextExporter.insert(conn, name, columns, values);
		// } catch (Exception e) {
		// System.out.println("Error inserting tweets for user " + userID
		// + ". Message:  " + e.getMessage());
		// }

	}
}
