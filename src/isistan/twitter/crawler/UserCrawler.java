package isistan.twitter.crawler;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

public class UserCrawler {

	private long u;

	private Logger log = Logger.getLogger(UserCrawler.class);

	private ExecutorService execUser;

	public UserCrawler(long user, ExecutorService execUser) {
		this.u = user;
		this.execUser = execUser;
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
		List<Future<Void>> futs = new ArrayList<>();

		if (!userProp.isDisabled()) {
			futs.add(execUser.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					userProp.getFolloweeCrawler().crawl();
					return null;
				}
			}));
			if (!config.isCrawlOnlyFollowees()) {
				futs.add(execUser.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						userProp.getFollowerCrawler().crawl();
						return null;
					}

				}));
				futs.add(execUser.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						userProp.getTweetCrawler().crawl();
						return null;
					}

				}));

				futs.add(execUser.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						userProp.getFavCrawler().crawl();
						return null;
					}

				}));
			}

		}
		for (Future<Void> future : futs) {
			future.get();
		}
		userProp.setComplete();

	}
}
