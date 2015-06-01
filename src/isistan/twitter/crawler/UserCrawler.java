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

		final CrawlerConfiguration config = CrawlerConfiguration.getCurrent();

		final UserStatus userProp = config.getStore().getUserStatus(u);

		if (!config.isForceRecrawl() && userProp.isComplete()) {
			log.info("Skipping user " + u
					+ ": is completed and forceRecrawl is false");
			return;
		}

		if (userProp.isDisabled()) {
			log.info("User " + u + " is DISABLED (escaped= "
					+ userProp.isEscaped() + " protected="
					+ userProp.isProtected() + " suspended="
					+ userProp.isSuspended() + ").");
			return;
		}

		if (config.isForceRecrawl() || !userProp.isInfoComplete()) {
			log.info("Storing User Info for " + u);
			try {
				userProp.getInfoCrawler().crawl();
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("Finished storing User Info for " + u);
		}
		List<Future<?>> futs = new ArrayList<>();

		if (!userProp.isDisabled()) {
			final String username = config.getStore().getUserInfo(u).scn;

			if (config.isCrawlFollowees())
				futs.add(execUser.submit(new Runnable() {

					@Override
					public void run() {
						userProp.getFolloweeCrawler(config.isForceRecrawl())
								.crawl(username);
					}
				}));
			else
				log.info("Ignoring FOLLOWEES for " + u + " (@" + username
						+ ").");

			if (config.isCrawlFollowers())
				futs.add(execUser.submit(new Runnable() {

					@Override
					public void run() {
						userProp.getFollowerCrawler(config.isForceRecrawl())
								.crawl(username);
					}
				}));
			else
				log.info("Ignoring FOLLOWERS for " + u + " (@" + username
						+ ").");

			if (config.isCrawlTweets())
				futs.add(execUser.submit(new Runnable() {

					@Override
					public void run() {
						userProp.getTweetCrawler(config.isForceRecrawl())
								.crawl(username);
					}
				}));
			else
				log.info("Ignoring TWEETS for " + u + " (@" + username + ").");

			if (config.isCrawlFavorites())
				futs.add(execUser.submit(new Runnable() {

					@Override
					public void run() {
						userProp.getFavCrawler(config.isForceRecrawl()).crawl(
								username);
					}
				}));
			else
				log.info("Ignoring FAVS for " + u + " (@" + username + ").");
		}
		for (Future<?> future : futs) {
			future.get();
		}
		if (userProp.isFolloweeComplete() && userProp.isFollowerComplete()
				&& userProp.isTweetComplete() && userProp.isFavoriteComplete()
				&& userProp.isInfoComplete())
			userProp.setComplete();

	}
}
