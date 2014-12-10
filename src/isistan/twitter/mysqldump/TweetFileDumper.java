package isistan.twitter.mysqldump;

import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.CSVReader;
import isistan.twitter.crawler.folder.UserFolder;
import isistan.twitter.mysql.PlainTextExporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class TweetFileDumper {
	
	//	TWEETID							0
	//	CREATED							1
	//	CONTRIBUTORS					2
	//	INREPLYTOUSERID					3
	//	INREPLYTOUSERSCREENNAME			4
	//	INREPLYTOTWEETID				5
	//	RETWEETID						6
	//	RETWEETCOUNT					7
	//	SOURCE							8
	//	GEO_LAT							9
	//	GEO_LONG						10
	//	HASHTAGS						11
	//	MEDIA							12
	//	PLACE_BOUNDING_BOX				13
	//	PLACE_COUNTRY					14
	//	PLACE_COUNTRY_CODE				15
	//	PLACE_FULL_NAME					16
	//	PLACE_GEOMETRY_TYPE				17
	//	PLACE_ID						18
	//	PLACE_NAME						19
	//	PLACE_STREET_ADDR				20
	//	PLACE_BOUNDING_BOX_COORD		21
	//	MENTIONS						22
	//	TEXT							23
	
	private static final String SEP = ";";
	protected static final int MAX_TWEETS = 3000;
	ExecutorService exec = Executors.newFixedThreadPool(50);
	BufferedWriter writer;
	List<Long> users = new ArrayList<>();
	private String name;
	private String type;
	private TweetDumper td;
	private String outputFolder;

	public TweetFileDumper(TweetDumper td, String of, String type, int count)
			throws IOException {
		this.td = td;
		this.outputFolder = of;
		File outputFile = new File(of + "/" + type + "-" + count + ".gzip");
		GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(
				outputFile));
		this.writer = new BufferedWriter(new OutputStreamWriter(gos, "UTF-8"),
				50 * 1024 * 1024);
		this.type = type;
		this.name = type + "-" + count;
	}

	public void shutdown() {
		exec.shutdown();
		Thread t = new Thread("TweetDumper " + name + " closer.") {
			@Override
			public void run() {
				try {
					exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

					notifyTerminated();

					TweetFileDumper.this.writer.close();
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							outputFolder + "/" + name + "-USERLIST.txt"));

					for (Long u : users)
						writer.write(u.toString() + "\n");

					writer.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		t.start();
	}

	protected void notifyTerminated() {
		td.dumperFinished();
	}

	public void addUser(final UserFolder u) {
		users.add(u.getUser());
		exec.execute(new Runnable() {

			@Override
			public void run() {
				try {
					dumpTweets(u);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}

	HashSet<Integer> TOBEEXCLUDED = new HashSet<>(Arrays.asList(new Integer[] {
			8, 9, 10, 11, 12, 13, 17, 20, 21, 22 }));

	public void dumpTweets(final UserFolder uf) throws IOException {
		final HashSet<String> tweetsIDs = new HashSet<String>();

		File tweetF = null;
		if (type.equals("tweets"))
			tweetF = uf.getTweets();
		else
			tweetF = uf.getFavs();

		if (!tweetF.exists()) {
			return;
		}
		final List<String> tweets = new ArrayList<>();

		CSVReader reader = new CSVReader(tweetF, true);
		while (reader.ready()) {
			String line = reader.readLine();
			if (line.startsWith("ID;"))
				continue;
			String[] split = split(line);

			if (!startsWithNumber(split)) {
				System.out.println("What the hell are you reading? "
						+ Arrays.toString(split));
				return;
			}
			while (split.length < 24) {
				line = line + reader.readLine();
				split = split(line);
			}

			// Hasta RETWEETCOUNT no hay chance de que haya problemas, a partir
			// de SOURCE puede haber puntos y coma.
			while (split.length > 24) {
				if ((split[9].isEmpty() || // GEO_LAT
						isNumber(split[9]))
						&& (split[10].isEmpty() || // GEO_LAT
						isNumber(split[10]))) {
					line = line.substring(0, line.lastIndexOf(";")) + "\\u003B"
							+ line.substring(line.lastIndexOf(";") + 1);

				} else { // Unir SOURCE
					StringBuilder builder = new StringBuilder();
					for (int i = 0; i < split.length; i++) {
						if (i == 0)
							builder.append(split[i]);
						else if (i == 8) {
							builder.append(";" + split[i] + "\\u003B"
									+ split[i + 1]);
							i++;
						} else
							builder.append(";" + split[i]);
					}
					line = builder.toString();
				}
				split = split(line);
			}

			String next = reader.peekLine();
			while (next != null && !startsWithNumber(split(next))) {
				line = line + reader.readLine();
				split = split(line);
				next = reader.peekLine();
			}

			String tweetID = split[0];
			if (tweetsIDs.contains(tweetID))
				continue;
			tweetsIDs.add(tweetID);

			int size = 16;
			size += uf.getUser().toString().length() + 1;
			for (String s : split) {
				size += s.length() + 1;
			}

			StringBuilder builder = new StringBuilder(size);
			builder.append(PlainTextExporter.escape(uf.getUser().toString(),
					true) + SEP);
			boolean start = true;
			for (int i = 0; i < split.length; i++) {
				if (!TOBEEXCLUDED.contains(i)) {
					String s = split[i];
					if (start) {
						start = false;
						builder.append(PlainTextExporter.escape(s, true));
					} else
						builder.append(SEP + PlainTextExporter.escape(s, true));
				}
			}
			tweets.add(builder.toString());
			if (tweets.size() > MAX_TWEETS) {
				writeTweets(tweets);
				tweets.clear();
			}
		}

		if (!tweets.isEmpty()) {
			writeTweets(tweets);
		}
		// if (!values.isEmpty())
		// try {
		// PlainTextExporter.insert(conn, name, columns, values);
		// } catch (Exception e) {
		// System.out.println("Error inserting tweets for user " + userID
		// + ". Message:  " + e.getMessage());
		// }

	}

	private boolean isNumber(String string) {
		try {
			Double.valueOf(string);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static boolean startsWithNumber(String[] split) {
		return split[0].trim().matches("^[0-9]+$");
	}

	private String[] split(String line) {
		String[] split = CSVBuilder.split(replaceAllCrap(line), ";");
		return split;
	}

	private String replaceAllCrap(String line) {
		line = line.replaceAll("GeoLocation;@", "");
		line = line.replaceAll("&#114;", "r");
		line = line.replaceAll("&#105;", "i");
		line = line.replaceAll("&#111;", "o");
		line = line.replaceAll("&#116;", "e");
		return line;
	}

	private void writeTweets(final List<String> tweets) {
		synchronized (writer) {
			for (String string : tweets) {
				try {
					writer.write(string + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}

	public void awaitTermination() {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {
		System.out
				.println(startsWithNumber(new String[] { "388968842623741952" }));
	}
}
