package isistan.twitter.crawler;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class UserCrawler {

	private long u;

	private Logger log = Logger.getLogger(UserCrawler.class);

	public UserCrawler(long user) {
		this.u = user;
	}

	public void crawlUser() throws Exception {

		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();

		final UserStatus userProp = config.getStore().getUserStatus(u);

		if (userProp.isComplete())
			return;

		if (!config.mustRecrawlInfo() && userProp.isCompleted())
			return;

		if (userProp.isDisabled())
			return;

		if (config.mustRecrawlInfo() || !userProp.isInfoComplete()) {
			log.info("Storing User Info for " + u);
			try {
				userProp.getInfoCrawler().crawl();
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("Finished storing User Info for " + u);
		}
		ExecutorService execUser = Executors.newCachedThreadPool();
		if (!userProp.isDisabled()) {
			execUser.execute(new Runnable() {
				@Override
				public void run() {
					userProp.getFolloweeCrawler().crawl();
				}
			});
			if (!config.isCrawlOnlyFollowees()) {
				execUser.execute(new Runnable() {
					@Override
					public void run() {
						userProp.getFollowerCrawler().crawl();
					}

				});
				execUser.execute(new Runnable() {
					@Override
					public void run() {
						userProp.getTweetCrawler().crawl();
					}
				});

				execUser.execute(new Runnable() {
					@Override
					public void run() {
						userProp.getFavCrawler().crawl();
					}
				});
			}

		}
		execUser.shutdown();
		execUser.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		userProp.setComplete();

	}

}
