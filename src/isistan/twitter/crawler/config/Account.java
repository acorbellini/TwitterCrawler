package isistan.twitter.crawler.config;

import isistan.twitterapi.RequestType;

import java.io.File;
import java.util.HashMap;

public class Account {
	private File account;
	HashMap<RequestType, Boolean> inUse = new HashMap<>();
	HashMap<RequestType, Long> times = new HashMap<>();

	public Account(File f) {
		this.setAccount(f);
	}

	public Boolean isUsed(RequestType req) {
		Boolean time = inUse.get(req);
		if (time == null)
			return false;
		return time;
	}

	public void setUsed(RequestType req) {
		inUse.put(req, true);
	}

	public void release(RequestType req) {
		inUse.put(req, false);
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		synchronized (config) {
			config.notifyAll();
		}

	}

	public Long getTime(RequestType req) {
		Long time = times.get(req);
		if (time == null)
			return 0l;
		return time;
	}

	public void setTime(RequestType req, long l) {
		times.put(req, l);
	}

	public File getAccount() {
		return account;
	}

	public void setAccount(File account) {
		this.account = account;
	}
}