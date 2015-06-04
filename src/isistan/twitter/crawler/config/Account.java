package isistan.twitter.crawler.config;

import isistan.twitter.crawler.TwitterOauthBuilder;
import isistan.twitter.crawler.request.RequestType;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import twitter4j.Twitter;

public class Account {
	private File account;
	Map<RequestType, Boolean> inUse = new ConcurrentHashMap<>();
	Map<RequestType, Long> times = new ConcurrentHashMap<>();
	Map<RequestType, Twitter> twitters = new ConcurrentHashMap<>();
	private volatile boolean discarded = false;

	public Account(File f) {
		this.setAccount(f);
	}

	public File getAccount() {
		return account;
	}

	public Long getTime(RequestType req) {
		Long time = times.get(req);
		if (time == null)
			return 0l;
		return time;
	}

	public Boolean isUsed(RequestType req) {
		Boolean time = inUse.get(req);
		if (time == null)
			return false;
		return time;
	}

	public void release(RequestType req) {
		inUse.put(req, false);
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		config.notifyAccountReleased();

	}

	public void setAccount(File account) {
		this.account = account;
	}

	public void setTime(RequestType req, long l) {
		times.put(req, l);
	}

	public void setUsed(RequestType req) {
		inUse.put(req, true);
	}

	public void setDiscarded() {
		discarded = true;
	}

	public boolean isDiscarded() {
		return discarded;
	}

	public Twitter getTwitter(RequestType req) throws Exception {
		Twitter twitter = twitters.get(req);
		if (twitter == null) {
			synchronized (twitters) {
				twitter = twitters.get(req);
				if (twitter == null) {
					twitter = TwitterOauthBuilder.build(getAccount());
					twitters.put(req, twitter);
				}
			}
		}
		return twitter;
	}
}