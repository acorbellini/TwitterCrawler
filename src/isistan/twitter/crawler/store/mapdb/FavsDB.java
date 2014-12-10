package isistan.twitter.crawler.store.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;

public class FavsDB {
	public DB favs;
	public BTreeMap<TweetKey, byte[]> favData;
	public BTreeMap<TweetKey, byte[]> favGeo;
	public BTreeMap<TweetKey, byte[]> favReply;
	public BTreeMap<TweetKey, byte[]> favMedia;
	public BTreeMap<TweetKey, byte[]> favContrib;

	public FavsDB() {
	}
}