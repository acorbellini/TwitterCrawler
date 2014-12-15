package isistan.twitter.crawler.util;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.request.CrawlerGetter;
import isistan.twitter.crawler.request.RequestType;
import isistan.twitter.crawler.request.TwitterCrawlerRequest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

public class CrawlerUtil {

	public static class UserIterator implements Iterator<Long> {
		Long peeked = null;
		private Scanner scanner;

		public UserIterator(String file) throws FileNotFoundException {
			scanner = new Scanner(new File(file));
		}

		@Override
		public boolean hasNext() {
			return scanner.hasNext();
		}

		@Override
		public Long next() {
			if (peeked != null) {
				Long ret = peeked;
				peeked = null;
				return ret;
			}
			return scanner.nextLong();
		}

		public Long peek() {
			return peeked = next();
		}

		@Override
		public void remove() {
		}

	}

	public static String escape(String text) {
		return text.replaceAll("\r\n", " ").replaceAll("\n", " ")
				.replaceAll("\r", " ").replace("\\", " ").replace(";", "\\;");
	}

	public static boolean filter(User u) {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		try {

			if (!u.getLang().startsWith("en")) {
				log.info("User " + u.getId() + " was escaped because LANG is "
						+ u.getLang());
				return true;
			}

			if (u.getStatusesCount() <= config.getMinTweetsToFilter()) {
				log.info("User " + u.getId()
						+ " was escaped because TWEETSCOUNT is "
						+ u.getStatusesCount() + " <= "
						+ config.getMinTweetsToFilter());
				return true;
			}
			if (u.getFriendsCount() <= config.getMinFolloweesToFilter()) {
				log.info("User " + u.getId()
						+ " was escaped because TWEETSCOUNT is "
						+ u.getFriendsCount() + " <= "
						+ config.getMinFolloweesToFilter());
				return true;
			}

		} catch (Exception e) {
			return true;
		}
		return false;
	}

	public static <R> R get(TwitterCrawlerRequest<R> req) {
		return new CrawlerGetter<R>(req).get();
	}

	public static Boolean isSuspended(final long u) {
		return CrawlerUtil.get(new TwitterCrawlerRequest<Boolean>() {
			@Override
			public Boolean exec(Twitter twitter) throws TwitterException {
				ResponseList<User> res = twitter.lookupUsers(new long[] { u });
				return res.contains(res);
			}

			@Override
			public RequestType getReqType() {
				return RequestType.INFO;
			}

			@Override
			public String toString() {
				return "IsSuspendedRequest - User: " + u;
			}
		});
	}

	public static Properties openProperties(String propString)
			throws IOException, FileNotFoundException {
		Properties prop = new Properties();
		File f = new File(propString);
		if (!f.exists()) {
			Files.createDirectories(Paths.get(f.getParent()));
			f.createNewFile();
		}
		FileInputStream ios = new FileInputStream(f);
		prop.load(ios);
		ios.close();
		return prop;
	}

	public static UserIterator scanIds(final String file)
			throws FileNotFoundException {
		return new UserIterator(file);
	}

	public static synchronized void updateProperty(String k, String v,
			Properties prop, String propPath) throws Exception {
		
		prop.setProperty(k, v);
		
		File f = new File(propPath);
		if (!f.exists())
			f.createNewFile();
		BufferedWriter fos = new BufferedWriter(new FileWriter(f));
		prop.store(fos, "");
		fos.close();
	}

	private static Logger log = Logger.getLogger(CrawlerUtil.class);

}
