package isistan.twitter.crawler.store;

import isistan.twitter.crawler.status.UserStatus;

import java.util.Map;

public class StoredUserStatus extends UserStatus {

	private CrawlerStore db;

	public StoredUserStatus(long u, CrawlerStore dbCrawlerStore) {
		super(u);
		this.db = dbCrawlerStore;
	}

	public synchronized String get(String k) throws Exception {
		Map<String, String> hash = db.bt.getUserStatus(getUser());
		return hash.get(k);
	}

	public synchronized void set(String k, String v) throws Exception {
		Map<String, String> hash = db.bt.getUserStatus(getUser());
		hash.put(k, v);
		db.bt.saveUserStatus(getUser(), hash, true);
	}
}