package isistan.twitter.crawler.store;

import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.BigTextStore;

import java.util.Map;

public class StoredUserStatus extends UserStatus {

	private BigTextStore db;
	private Map<String, String> cached;

	public StoredUserStatus(long u, BigTextStore dbCrawlerStore) {
		super(u);
		this.db = dbCrawlerStore;
	}

	public synchronized String get(String k) throws Exception {
		if (cached == null) {
			cached = db.getUserStatus0(getUser());
		}
		return cached.get(k);
	}

	public synchronized void set(String k, String v) throws Exception {
		if (cached == null) {
			cached = db.getUserStatus0(getUser());
		}
		cached.put(k, v);
		db.saveUserStatus(getUser(), cached, true);
	}
}