package isistan.twitter.crawler.store;

import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.BigTextStore;

import java.util.Map;

public class StoredUserStatus extends UserStatus {

	private BigTextStore db;

	public StoredUserStatus(long u, BigTextStore dbCrawlerStore) {
		super(u);
		this.db = dbCrawlerStore;
	}

	public synchronized String get(String k) throws Exception {
		Map<String, String> hash = db.getUserStatus0(getUser());
		return hash.get(k);
	}

	public synchronized void set(String k, String v) throws Exception {
		Map<String, String> hash = db.getUserStatus0(getUser());
		hash.put(k, v);
		db.saveUserStatus(getUser(), hash, true);
	}
}