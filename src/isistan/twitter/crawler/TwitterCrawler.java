package isistan.twitter.crawler;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.util.CrawlerUtil.UserIterator;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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

		final ExecutorService execUser = Executors.newFixedThreadPool(
				config.getMaxThreads(), new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Per User Worker");
						return t;
					}
				});

		exec = Executors.newFixedThreadPool(config.getMaxThreads(),
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Crawler worker");
						return t;
					}
				});

		final Semaphore sem = new Semaphore(config.getAccountSize());
		int count = 0;
		for (UserIterator iterator = config.getUsers(); iterator.hasNext();) {
			final Long user = (Long) iterator.next();
			try {
				if (config.getLatestCrawled() != null
						&& user < config.getLatestCrawled()) {
					count++;
					continue;
				}
				
				sem.acquire();

				config.registerUser(user);

				log.info("Starting user crawler on user " + user + " , number "
						+ count++ + " in the list file.");

				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							new UserCrawler(user, execUser).crawlUser();
						} catch (Exception e) {
							e.printStackTrace();
						}

						try {
							config.updateLatestCrawled(user);
						} catch (Exception e) {
							e.printStackTrace();
						}

						sem.release();
					}

				});

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		execUser.shutdown();
		execUser.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		config.getStore().close();
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