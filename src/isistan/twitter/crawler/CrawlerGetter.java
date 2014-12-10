package isistan.twitter.crawler;

import isistan.twitter.crawler.config.Account;
import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitterapi.TwitterCrawlerRequest;
import isistan.twitterapi.TwitterOauthBuilder;

import org.apache.log4j.Logger;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public class CrawlerGetter<R> {
	Logger log = Logger.getLogger(CrawlerGetter.class);

	private TwitterCrawlerRequest<R> req;

	int untilReset = 0;

	int globaltries = 0;

	int tries = 0;

	boolean success = false;

	Twitter twitter = null;

	Account last = null;

	Account current = null;

	CrawlerConfiguration config;

	public CrawlerGetter(TwitterCrawlerRequest<R> req) {
		this.req = req;
		this.config = CrawlerConfiguration.getCurrent();
	}

	public R get() {
		R res = null;
		current = config.getAccount(req.getReqType());
		while (!success)
			try {
				twitter = TwitterOauthBuilder.build(current.getAccount());
				res = req.exec(twitter);
				success = true;
				config.getFailures().put(current, 0);
			} catch (Exception ex) {
				handleError(twitter, ex);
			}
		config.setTime(current, req.getReqType(), 0l);
		if (current != null)
			config.release(current, req.getReqType());
		return res;
	}

	private void handleError(Twitter twitter, Exception ex) {
		tries++;
		untilReset = CrawlerConfiguration.MIN_WAIT_TO_NEXT_REQUEST;
		if (ex instanceof TwitterException) {
			TwitterException e = (TwitterException) ex;
			if (isFatal(e)) {
				success = true;
				return;
			}

			// If any of these do not change default time to reset.
			if (e.getRateLimitStatus() != null
					&& !CrawlerErrorCodes.timedOut(e)
					&& !CrawlerErrorCodes.overcapacity(e))
				untilReset = e.getRateLimitStatus().getSecondsUntilReset();

			if (untilReset < 0)
				untilReset = config.TIME_IF_SUSPENDED;
			if (CrawlerErrorCodes.badAuthentication(e)
					|| CrawlerErrorCodes.invalidOrExpired(e)) {
				log.warn("DISCARDING account " + current.getAccount().getName());
				releaseAccount(true);
			}

			// || untilReset < 0) {
			// Suspended account
			// Integer failures = config.getFailures().get(current);
			// if (failures == null)
			// failures = 0;
			// config.getFailures().put(current, failures + 1);

			// if (CrawlerErrorCodes.badAuthentication(e)
			// || failures > config.MAX_ACCOUNT_FAILURES) {
			//
			// } else {
			// log.warn("CHANGING account " + current.account.getName()
			// + " because it is suspended.");
			// releaseAccount(false);
			// }
			// }
		}

		log.warn("Exception on request " + req + ", using account "
				+ current.getAccount().getName() + ": " + ex.getMessage());

		config.setTime(current, req.getReqType(), System.currentTimeMillis()
				+ untilReset * 1000);

		log.warn("CHANGING account " + current.getAccount().getName()
				+ ", trying to get another that has less waiting time.");
		releaseAccount(false);

		if (tries > CrawlerConfiguration.MAX_TRIES_BEFORE_DISCARD_ACCOUNT) {
			globaltries++;
			if (globaltries == config.getNumAccounts()) {
				log.error("Discarding request " + req + " I tried "
						+ globaltries + " times to obtain data.");
				success = true;
				return;
			}

			log.warn("Changing account " + current.getAccount().getName()
					+ " I tried " + tries + " times to make a request.");
			releaseAccount(false);
		}

		long toSleep = config.getTime(current, req.getReqType())
				- System.currentTimeMillis();

		if (toSleep < 0)
			toSleep = 0;

		log.info("Waiting " + (toSleep / 1000) + " seconds on account "
				+ current.getAccount().getName());
		try {
			while (toSleep > 0) {
				Thread.sleep(Math.min(toSleep, 30000));
				toSleep = toSleep - 30000;
				log.info("Still waiting, "
						+ (toSleep / 1000 > 0 ? toSleep / 1000 : 0)
						+ " seconds on account "
						+ current.getAccount().getName());
			}
		} catch (Exception e2) {

		}
	}

	private boolean isFatal(TwitterException e) {
		return CrawlerErrorCodes.isNotAuthorized(e)
				|| CrawlerErrorCodes.isSuspended(e)
				|| CrawlerErrorCodes.userDoesNotExist(e);

	}

	private void releaseAccount(boolean discard) {
		last = current;
		while (current == last) {
			if (discard)
				config.discardAccount(current);

			config.release(current, req.getReqType());

			current = config.getAccount(req.getReqType());
			// if (twitter != null)
			// twitter.
			tries = 0;
			untilReset = 0;
		}
	}

}
