package isistan.twitter.crawler.util;

import gnu.trove.list.array.TLongArrayList;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.tweet.HashTagList;
import isistan.twitter.crawler.tweet.MediaList;
import isistan.twitter.crawler.tweet.MentionList;
import isistan.twitter.crawler.tweet.Place;
import isistan.twitter.crawler.tweet.Reply;
import isistan.twitter.crawler.tweet.Tweet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.table.CSVBuilder;
import edu.jlime.util.table.CSVReader;
import edu.jlime.util.table.Table;
import edu.jlime.util.table.ValueCell;

public class StoreUtil {
	private static final long VERSION_3_MAGIC = DataTypeUtils
			.byteArrayToLong("VER3TWIT".getBytes());

	public static byte[] tweetToByteArray(Tweet tweet) {
		ByteBuffer buffer = new ByteBuffer();
		buffer.putLong(VERSION_3_MAGIC);
		buffer.putLong(tweet.user);
		buffer.putLong(tweet.tweetid);
		buffer.putLong(tweet.created.getTime());
		buffer.putString(tweet.source);
		tweet.hashtags.writeTo(buffer);
		tweet.mentions.writeTo(buffer);
		buffer.putInt(tweet.retweetCount);
		buffer.putLong(tweet.retweetid);
		buffer.putString(tweet.text);
		tweet.place.writeTo(buffer);
		tweet.reply.writeTo(buffer);
		tweet.media.writeTo(buffer);
		buffer.putLongArray(tweet.contrib);
		// Version 2
		buffer.putInt(tweet.favoriteCount);
		buffer.putBoolean(tweet.favorited);
		buffer.putBoolean(tweet.possiblySensitive);

		return buffer.build();
	}

	public static Tweet byteArrayToTweet(byte[] byteArray) {
		long magic = DataTypeUtils.byteArrayToLong(byteArray);
		if (magic == VERSION_3_MAGIC) {

			ByteBuffer buffer = new ByteBuffer(byteArray);
			long magicAgain = buffer.getLong();
			Tweet ret = new Tweet();
			ret.setUser(buffer.getLong());
			ret.setTweetid(buffer.getLong());
			ret.setCreated(new Date(buffer.getLong()));
			ret.setSource(buffer.getString());
			ret.setHashtags(new HashTagList().readFrom(buffer));
			ret.setMentions(new MentionList().readFrom(buffer));
			ret.setRetweetCount(buffer.getInt());
			ret.setRetweetid(buffer.getLong());
			ret.setText(buffer.getString());
			ret.setPlace(new Place().readFrom(buffer));
			ret.setReply(new Reply().readFrom(buffer));
			ret.setMedia(new MediaList().readFrom(buffer));
			ret.setContributors(buffer.getLongArray());
			ret.setFavoriteCount(buffer.getInt());
			ret.setFavorited(buffer.getBoolean());
			ret.setPossiblySensitive(buffer.getBoolean());
			return ret;
		} else
			return readVersion2(byteArray);
	}

	private static Tweet readVersion2(byte[] byteArray) {
		ByteBuffer buffer = new ByteBuffer(byteArray);
		Tweet ret = new Tweet();
		ret.setUser(buffer.getLong());
		ret.setTweetid(buffer.getLong());
		ret.setCreated(new Date(buffer.getLong()));
		ret.setSource(buffer.getString());
		ret.setHashtags(StoreUtil.parseHashTags(buffer.getString()));
		ret.setMentions(StoreUtil.parseMentions(buffer.getString()));
		ret.setRetweetCount(buffer.getInt());
		ret.setRetweetid(buffer.getLong());
		ret.setText(buffer.getString());
		ret.setPlace(new Place(buffer.getString(), buffer.getString(), buffer
				.getString()));
		ret.setReply(new Reply(buffer.getString(), buffer.getLong(), buffer
				.getLong()));
		ret.setMedia(StoreUtil.parseMedia(buffer.getString()));
		ret.setContributors(StoreUtil.parseLongArray(buffer.getString()));

		// Version 2
		if (buffer.hasRemaining()) {
			ret.setFavoriteCount(buffer.getInt());
			ret.setFavorited(buffer.getBoolean());
			ret.setPossiblySensitive(buffer.getBoolean());
		}
		return ret;
	}

	static long[] parseLongArray(String string) {
		if (string.isEmpty())
			return new long[] {};
		String[] list = string.replace("[", "").replace("]", "").trim()
				.split(",");
		long[] ret = new long[list.length];
		for (int i = 0; i < ret.length; i++)
			ret[i] = Long.valueOf(list[i]);
		return ret;
	}

	public static UserInfo bytesToUserInfo(byte[] bytes) {
		ByteBuffer buffer = new ByteBuffer(bytes);
		return new UserInfo(buffer.getLong(), buffer.getString(),
				buffer.getString(), buffer.getString(), buffer.getString(),
				new Date(buffer.getLong()), buffer.getString(),
				buffer.getString(), buffer.getInt(), buffer.getInt(),
				buffer.getInt(), buffer.getInt(), buffer.getInt(),
				buffer.getInt(), buffer.getBoolean(), buffer.getBoolean());
	}

	public static boolean isNumber(String string) {
		try {
			Double.valueOf(string);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static ListReader listReader(File foll) throws Exception {
		return new ListReader(foll);
	}

	public static String removeTags(String string) {
		return string.replaceAll("<.+?>", "");
	}

	public static String replaceAllCrap(String line) {
		line = geo_patt.matcher(line).replaceAll("");
		line = r_patt.matcher(line).replaceAll("r");
		line = i_patt.matcher(line).replaceAll("i");
		line = o_patt.matcher(line).replaceAll("o");
		line = e_patt.matcher(line).replaceAll("e");
		return line;
	}

	public static String[] split(String line) {
		String[] split = sep.split(StoreUtil.replaceAllCrap(line), -1);
		return split;
	}

	public static boolean startsWithNumber(String[] split) {
		return split[0].trim().matches("^[0-9]+$");
	}

	public static Date toDate(String t) throws ParseException {
		return new Date(new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",
				Locale.US).parse(t).getTime());
	}

	public static UserInfo toInfo(File info) throws Exception {

		CSVBuilder builder = new CSVBuilder(info);
		Table t = builder.toTable();

		if (t.getRowLimit() == 0) {
			// System.out.println("User ID " + userID
			// + " has it's user info file empty.");
			return null;
		}

		while (t.getRowLimit() > 2)
			t.mergeRows(1, 2);

		String lang = t.get(4, 1).value(); // Where LANG Should be
		while (!lang.matches("[a-z][a-z]")
				&& !lang.matches("[a-z][a-z]-[a-z][a-z]")
				&& !lang.matches("[a-z][a-z][a-z]")
				&& !lang.matches("[a-z][a-z][a-z][a-z]")) {
			// Might not be a language
			t.merge(3, 1, 4, 1);// JOIN DESCRIPTION AND LANGUAGE
			lang = t.get(4, 1).value(); // Where LANG Should be
		}

		while (t.getRow(1).size() > 16) {
			t.merge(6, 1, 7, 1);// LOCATION AND TIMEZONE

		}

		while (t.getRow(1).size() < 16) {
			t.insCol(6);// LOCATION INSERTED
			t.set(6, 1, new ValueCell(""));
		}

		Date date = toDate(t.get(5, 1).value());
		return new UserInfo(Long.valueOf(t.get(0, 1).value()), t.get(1, 1)
				.value(), t.get(2, 1).value(), t.get(3, 1).value(), t.get(4, 1)
				.value(), date, t.get(6, 1).value(), t.get(7, 1).value(),
				Integer.valueOf(t.get(8, 1).value()), Integer.valueOf(t.get(9,
						1).value()), Integer.valueOf(t.get(10, 1).value()),
				Integer.valueOf(t.get(11, 1).value()), Integer.valueOf(t.get(
						12, 1).value()), Integer.valueOf(t.get(13, 1).value()),
				Boolean.valueOf(t.get(14, 1).value()), Boolean.valueOf(t.get(
						15, 1).value()));
	}

	public static long[] toList(File foll) throws Exception {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(foll), 10 * 1024 * 1024);
		} catch (Exception e) {
			return new long[] {};
		}
		TLongArrayList list = new TLongArrayList();
		int cont = 0;
		while (reader.ready()) {
			String f = reader.readLine();
			cont++;
			f = f.replaceAll("\0", "");
			try {
				list.add(Long.valueOf(f));
			} catch (Exception e) {
				System.out.println("Error reading line " + cont
						+ " for user file " + foll + " with error"
						+ e.getMessage() + " - " + e.getCause());
			}
		}
		reader.close();
		list.sort();
		return list.toArray();
	}

	public static Set<Tweet> toTweet(long user, File t) throws IOException,
			NumberFormatException, ParseException {
		final Set<Tweet> tweets = new HashSet<>();
		int cont = 0;
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(t), true, 1024 * 1024);
		} catch (Exception e) {
			return new HashSet<Tweet>();
		}
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

				// Hasta RETWEETCOUNT no hay chance de que haya problemas, a
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
						&& !StoreUtil.startsWithNumber(StoreUtil.split(next))) {
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
						StoreUtil.removeTags(split[8]),
						parseHashTags(split[11]), parseMentions(split[22]),
						split[23], Integer.valueOf(split[7]),
						Long.valueOf(split[6]), 0, false, false);
				tweet.setMedia(StoreUtil.parseMedia(split[12]));
				tweet.setPlace(new Place(split[14], split[16], split[19]));
				tweet.setContributors(parseLongArray(split[2]));
				tweet.setReply(new Reply(split[4], Long.valueOf(split[3]), Long
						.valueOf(split[5])));

				tweets.add(tweet);
			} catch (Exception e) {
				System.out.println("Error reading line " + cont + " for user "
						+ user + " and file " + t + " with error"
						+ e.getMessage() + " - " + e.getCause());
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
		return tweets;
	}

	public static MediaList parseMedia(String string) {
		MediaList ret = new MediaList();
		if (string.isEmpty())
			return ret;
		String[] list = string.replace("[", "").replace("]", "").trim()
				.split("-");
		for (String string2 : list) {
			String[] hashtag = string2.split(",");
			ret.addNew(Long.valueOf(hashtag[0]), hashtag[1], hashtag[2],
					Integer.valueOf(hashtag[3]), Integer.valueOf(hashtag[4]));
		}
		return ret;
	}

	public static HashTagList parseHashTags(String string) {

		HashTagList ret = new HashTagList();
		if (string.isEmpty())
			return ret;
		String[] list = string.replace("[", "").replace("]", "").trim()
				.split("-");
		for (String string2 : list) {
			String[] hashtag = string2.split(",");
			ret.addNew(hashtag[0], Integer.valueOf(hashtag[1]),
					Integer.valueOf(hashtag[2]));
		}
		return ret;
	}

	public static TweetReader tweetReader(long user, File tFile)
			throws Exception {
		return new TweetReader(user, tFile);
	}

	public static byte[] userInfoToBytes(UserInfo info) {
		ByteBuffer buffer = new ByteBuffer();
		buffer.putLong(info.uid);
		buffer.putString(info.scn);
		buffer.putString(info.name);
		buffer.putString(info.desc);
		buffer.putString(info.lang);
		buffer.putLong(info.date.getTime());
		buffer.putString(info.loc);
		buffer.putString(info.timezone);
		buffer.putInt(info.utc);
		buffer.putInt(info.followees);
		buffer.putInt(info.followers);
		buffer.putInt(info.favs);
		buffer.putInt(info.listed);
		buffer.putInt(info.tweets);
		buffer.putBoolean(info.verified);
		buffer.putBoolean(info.protectd);
		return buffer.build();
	}

	private static Pattern sep = Pattern.compile("(?<!\\\\);");

	private static Pattern geo_patt = Pattern.compile("GeoLocation;@");

	private static Pattern r_patt = Pattern.compile("&#114;");

	private static Pattern i_patt = Pattern.compile("&#105;");

	private static Pattern o_patt = Pattern.compile("&#111;");

	private static Pattern e_patt = Pattern.compile("&#116;");

	public static MentionList parseMentions(String string) {
		MentionList ret = new MentionList();
		if (string.isEmpty())
			return ret;
		try {
			String[] list = string.split("\\]-\\[");
			for (String string2 : list) {
				try {
					String[] hashtag = string2.replace("[", "")
							.replace("]", "").split(",");
					if (hashtag.length == 5)
						ret.addNew(Long.valueOf(hashtag[0]), hashtag[1],
								hashtag[2], Integer.valueOf(hashtag[3]),
								Integer.valueOf(hashtag[4]));
					else if (hashtag.length > 5) {
						String name = "";
						boolean first = true;
						for (int i = 2; i < 2 + (hashtag.length - 4); i++) {
							if (first)
								first = false;
							else
								name += ",";
							name += hashtag[i];
						}
						ret.addNew(Long.valueOf(hashtag[0]), hashtag[1], name,
								Integer.valueOf(hashtag[hashtag.length - 2]),
								Integer.valueOf(hashtag[hashtag.length - 1]));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
}
