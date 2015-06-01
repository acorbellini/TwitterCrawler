package isistan.twitter.crawler.util;

import isistan.twitter.crawler.tweet.Tweet;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import edu.jlime.util.table.CSVReader;

public class TweetReader implements Iterator<Tweet> {
	Tweet curr = null;
	private File t;
	private CSVReader reader;
	int cont = 0;
	private long user;

	public TweetReader(long user, File tFile) {
		this.user = user;
		this.t = tFile;
		try {
			reader = new CSVReader(new FileReader(t), true, 5 * 1024 * 1024);
			curr = getNext();
		} catch (Exception e) {
		}
	}

	public Tweet getNext() {

		try {
			while (reader.ready()) {
				String line = reader.readLine();
				line = line.replaceAll("\0", "");
				cont++;
				if (line.startsWith("ID;"))
					continue;
				String[] split = StoreUtil.split(line);

				if (!StoreUtil.startsWithNumber(split)) {
					System.out.println("Error reading this line "
							+ Arrays.toString(split));
					continue;
				}
				try {
					while (split.length < 24) {
						line = line + reader.readLine();
						cont++;
						split = StoreUtil.split(line);
					}

					// Hasta RETWEETCOUNT no hay chance de que haya
					// problemas, a
					// partir
					// de SOURCE puede haber puntos y coma.
					while (split.length > 24) {
						if ((split[9].isEmpty() || // GEO_LAT
								StoreUtil.isNumber(split[9]))
								&& (split[10].isEmpty() || // GEO_LAT
								StoreUtil.isNumber(split[10]))) {
							line = line.substring(0, line.lastIndexOf(";"))
									+ "\\u003B"
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
						split = StoreUtil.split(line);
					}

					String next = reader.peekLine();
					while (next != null
							&& !StoreUtil.startsWithNumber(StoreUtil
									.split(next))) {
						line = line + reader.readLine();
						cont++;
						split = StoreUtil.split(line);
						next = reader.peekLine();
					}

					// 0 ID
					// 1 CREATED
					// 2 CONTRIBUTORS
					// 3 INREPLYTOUSERID
					// 4 INREPLYTOUSERSCREENNAME
					// 5 INREPLYTOTWEETID
					// 6 RETWEETID
					// 7 RETWEETCOUNT
					// 8 SOURCE
					// 9 GEO_LAT
					// 10 GEO_LONG
					// 11 HASHTAGS
					// 12 MEDIA
					// 13 PLACE_BOUNDING_BOX
					// 14 PLACE_COUNTRY
					// 15 PLACE_COUNTRY_CODE
					// 16 PLACE_FULL_NAME
					// 17 PLACE_GEOMETRY_TYPE
					// 18 PLACE_ID
					// 19 PLACE_NAME
					// 20 PLACE_STREET_ADDR
					// 21 PLACE_BOUNDING_BOX_COORD
					// 22 MENTIONS
					// 23 TEXT

					if (split[2].equals("[]"))
						split[2] = "";
					if (split[3].isEmpty())
						split[3] = "-1";
					if (split[4].isEmpty())
						split[4] = "";
					if (split[5].isEmpty())
						split[5] = "-1";

					Tweet tweet = new Tweet(user, Long.valueOf(split[0]),
							StoreUtil.toDate(split[1]),
							StoreUtil.removeTags(split[8]), split[11],
							split[22], split[23], Integer.valueOf(split[7]),
							Long.valueOf(split[6]), 0, false, false);
					tweet.setMedia(split[12]);
					tweet.setPlace(split[14], split[16], split[19]);
					tweet.setContributors(split[2]);
					tweet.setReply(split[4], Long.valueOf(split[3]),
							Long.valueOf(split[5]));

					return tweet;
				} catch (Exception e) {
					System.out.println("Error reading line " + cont
							+ " for user " + user + " and file " + t
							+ " with error" + e.getMessage() + " - "
							+ e.getCause());
				}

				// int size = 16;
				// size += uf.getUser().toString().length() + 1;
				// for (String s : split) {
				// size += s.length() + 1;
				// }
				// StringBuilder builder = new StringBuilder(size);
				// builder.append(PlainTextExporter.escape(uf.getUser().toString(),
				// true) + SEP);
				// boolean start = true;
				// for (int i = 0; i < split.length; i++) {
				// if (!TOBEEXCLUDED.contains(i)) {
				// String s = split[i];
				// if (start) {
				// start = false;
				// builder.append(PlainTextExporter.escape(s, true));
				// } else
				// builder.append(SEP + PlainTextExporter.escape(s, true));
				// }
				// }
				// tweets.add(builder.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return curr != null;
	}

	@Override
	public Tweet next() {
		Tweet ret = curr;
		curr = getNext();
		return ret;
	}

}