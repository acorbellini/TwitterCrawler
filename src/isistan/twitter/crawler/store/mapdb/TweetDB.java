package isistan.twitter.crawler.store.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;

public class TweetDB<K> {
	public DB db;
	public BTreeMap<K, byte[]> data;
	public BTreeMap<K, byte[]> geo;
	public BTreeMap<K, byte[]> reply;
	public BTreeMap<K, byte[]> media;
	public BTreeMap<K, byte[]> contrib;

	public TweetDB() {
	}
}