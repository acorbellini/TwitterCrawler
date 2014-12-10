package isistan.twitter.mysqldump;

import isistan.twitter.mysql.PlainTextExporter;
import isistan.twitter.mysql.TweetTable;
import isistan.twitter.mysql.UserInfoTable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DumpLoader {
	private String folder;
	private Connection conn;

	public DumpLoader(String folder, String jdbc, String u, String pass)
			throws SQLException {
		this.folder = folder;
		this.conn = DriverManager.getConnection(jdbc, u, pass);
	}

	public static void main(String[] args) throws Exception {
		DumpLoader ldr = new DumpLoader(args[0], args[1], args[2], args[3]);
		ldr.start();
	}

	private void start() throws Exception {
		// loadInfo();
		loadTweets();
	}

	private void loadTweets() throws Exception {
		for (File f : new File(folder).listFiles()) {
			String name = f.getName();

			if (name.startsWith("tweets") || (name.startsWith("favorites"))) {

				if (name.contains("USERLIST")) {
					String tableName = name.substring(0,
							name.indexOf("USERLIST")).replace("-", "");
//					loadTweetTableLoc(f, tableName);
				} else {

					String tableName = name.substring(0, name.indexOf("."))
							.replace("-", "");
					PlainTextExporter.createIfNotExist(conn, tableName,
							TweetTable.tweetStructure);
					PlainTextExporter.copy(conn, tableName, f.getAbsolutePath()
							.replaceAll("\\\\", "/"));
				}
			}
		}

	}

	private void loadTweetTableLoc(File f, String tableName)
			throws SQLException, IOException {
		PlainTextExporter.createIfNotExist(conn, "TWEETSINFO",
				"USERID BIGINT, TWEETTABLE VARCHAR(100), PRIMARY KEY (USERID)");

		BufferedReader reader = new BufferedReader(new FileReader(f));
		while (reader.ready()) {
			String user = reader.readLine();
			PlainTextExporter.insert(conn, "TWEETSINFO", "USERID, TWEETTABLE",
					"'" + user + "','" + tableName + "'");
		}

	}

	private void loadInfo() throws Exception {
		File infoFile = new File(folder + "/USERINFO.txt");

		PlainTextExporter.createIfNotExist(conn, "USERINFO",
				UserInfoTable.userInfoStructure);

		PlainTextExporter.loadData(conn, "USERINFO", infoFile.getAbsolutePath()
				.replaceAll("\\\\", "/"));
	}
}
