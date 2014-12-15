package isistan.twitter.crawler.folder;

import isistan.def.utils.table.CSVBuilder;
import isistan.twitter.crawler.folder.UsersTable.User;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class UserFolder {

	public static int countLines(File filename) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(filename),
				64 * 1024);
		try {
			byte[] c = new byte[64 * 1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
		// LineNumberReader lineNumberReader = new LineNumberReader(
		// new FileReader(filename));
		// lineNumberReader.skip(Long.MAX_VALUE);
		// int lines = lineNumberReader.getLineNumber();
		// lineNumberReader.close();
		// return lines;

		// try {
		// return Integer.valueOf(execCommand("C:/cygwin64/bin/wc.exe", "-l",
		// filename.getPath()).split(" ")[0]);
		// } catch (NumberFormatException e) {
		// e.printStackTrace();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// return 0;

	}

	public static String execCommand(String... cmd) throws Exception {
		ProcessBuilder procbuilder = new ProcessBuilder(cmd);
		Process proc = procbuilder.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				proc.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String line = null;
		while ((line = br.readLine()) != null) {
			builder.append(line);
			builder.append(System.getProperty("line.separator"));
		}
		return builder.toString();

	}

	private String uf;

	private long user;

	private User userTables;

	public UserFolder(long u, String uf) {
		this.uf = uf;
		this.setUser(u);
		this.userTables = UsersTable.getUser(u);
	}

	public File getFavs() {
		return new File(uf + "/" + getUser() + "-FAVORITES.dat");
	}

	public int getFavsSize() throws IOException {
		File t = getFavs();
		if (t.exists())
			return countLines(t);
		return 0;
	}

	public File getFollowees() {
		return new File(uf + "/" + getUser() + "-FOLLOWEES.dat");
	}

	public File getFollowers() {
		return new File(uf + "/" + getUser() + "-FOLLOWERS.dat");
	}

	public File getInfo() {
		return new File(uf + "/" + getUser() + "-info.dat");
	}

	private File getProcessedTweets() {
		return new File(uf + "/" + getUser() + "-TWEETS-EN-PROC.dat");
	}

	private CSVBuilder getTweetBuilder(String type) {
		CSVBuilder builder = new CSVBuilder(new File(uf + "/" + getUser() + "-"
				+ type + ".dat"));
		builder.setExpectedFields(24);
		builder.setReplaceString("GeoLocation;@", "");
		builder.setReplaceString("&#114;", "r");
		builder.setReplaceString("&#105;", "i");
		builder.setReplaceString("&#111;", "o");
		builder.setReplaceString("&#116;", "e");
		return builder;
	}

	public File getTweets() {
		return new File(uf + "/" + getUser() + "-TWEETS.dat");

	}

	public int getTweetSize() throws IOException {
		File t = getTweets();
		if (t.exists())
			return countLines(t);
		return 0;
	}

	public int getTweetsProcessed() throws IOException {
		File t = getProcessedTweets();
		if (t.exists())
			return countLines(t);
		return 0;
	}

	public Long getUser() {
		return user;
	}

	public void setUser(long user) {
		this.user = user;
	}
}
