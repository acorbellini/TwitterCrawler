package isistan.twitter.crawler;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.util.CrawlerUtil.UserIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import twitter4j.TwitterException;

public class TwitterCrawler {

	public static void main(String[] args) throws Exception {
		new TwitterCrawler().start(args[0]);
	}

	ExecutorService exec;

	ArrayBlockingQueue<Long> queue;

	Logger log = Logger.getLogger(TwitterCrawler.class);
	static final String FOLLOWEES = "FOLLOWEES";
	static final String FOLLOWERS = "FOLLOWERS";
	static final String FAVORITES = "FAVORITES";
	static final String TWEETS = "TWEETS";

	static final String MENTIONS = "MENTIONS";

	public void run(final Properties configProp) throws Exception {

		final CrawlerConfiguration config = CrawlerConfiguration
				.create(configProp);

		log.info("Starting crawler.");

		exec = Executors.newFixedThreadPool(config.getMaxThreads());

		final Semaphore sem = new Semaphore(config.getAccountSize());
		int count = 0;
		for (UserIterator iterator = config.getUsers(); iterator.hasNext();) {
			final Long user = (Long) iterator.next();
			try {
				if (user < config.getLatestCrawled()) {
					count++;
					continue;
				}
				sem.acquire();

				log.info("Starting user crawler on user " + user + " , number "
						+ count++ + " in the list file.");

				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							new UserCrawler(user).crawlUser();
						} catch (Exception e) {
							e.printStackTrace();
						}

						config.updateLatestCrawled(user);

						sem.release();
					}

				});

			} catch (Exception e) {
			}
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	public void saveFollowees(long userId, Properties userProp,
			String userPropPath, File userDir, File account)
			throws TwitterException, InterruptedException, IOException {
		saveFriends(userId, FOLLOWEES, userProp, userPropPath, userDir, account);
	}

	public void saveFollowers(long userId, Properties userProp,
			String userPropPath, File userDir, File account)
			throws TwitterException, InterruptedException, IOException {
		saveFriends(userId, FOLLOWERS, userProp, userPropPath, userDir, account);

	}

	public void saveFriends(long userId, String type, Properties userProp,
			String userPropPath, File userDir, File account)
			throws TwitterException, InterruptedException, IOException {

	}

	public void start(String configFile) throws Exception {
		System.setProperty("twitter4j.loggerFactory",
				"twitter4j.internal.logging.NullLoggerFactory");
		Properties config = new Properties();
		FileInputStream fis = new FileInputStream(new File(configFile));
		config.load(fis);
		fis.close();
		run(config);
	}
}