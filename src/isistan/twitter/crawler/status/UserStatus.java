package isistan.twitter.crawler.status;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.adjacency.UserAdjacencyListCrawler;
import isistan.twitter.crawler.info.UserInfoCrawler;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.tweet.UserTweetsCrawler;

import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class UserStatus {

	protected long user;
	private TwitterStore db;
	private Map<String, String> cached;

	public UserStatus(long u, TwitterStore dbCrawlerStore) {
		this.user = u;
		this.db = dbCrawlerStore;
	}

	public synchronized String get(String k) throws Exception {
		if (cached == null) {
			cached = db.getStatusProperties(getUser());
		}
		return cached.get(k);
	}

	public synchronized void set(String k, String v) throws Exception {
		if (cached == null) {
			cached = db.getStatusProperties(getUser());
		}
		cached.put(k, v);
		db.saveUserStatus(getUser(), cached, true);
	}

	public UserTweetsCrawler getFavCrawler(boolean force) {
		return new UserTweetsCrawler(this, user, TweetType.FAVORITES, force);
	}

	public UserAdjacencyListCrawler getFolloweeCrawler(boolean force) {
		return new UserAdjacencyListCrawler(this, user, ListType.FOLLOWEES,
				force);
	}

	public UserAdjacencyListCrawler getFollowerCrawler(boolean force) {
		return new UserAdjacencyListCrawler(this, user, ListType.FOLLOWERS,
				force);
	}

	public UserInfoCrawler getInfoCrawler() {
		return new UserInfoCrawler(this, user);
	}

	public UserTweetsCrawler getTweetCrawler(boolean force) {
		return new UserTweetsCrawler(this, user, TweetType.TWEETS, force);
	}

	public long getUser() {
		return user;
	}

	public boolean has(String string) throws Exception {
		return get(string) != null;
	}

	public boolean isComplete() throws Exception {
		return isTrue("UserComplete");
	}

	public boolean isCompleted() throws Exception {
		return isInfoComplete() && isFolloweeComplete() && isFollowerComplete()
				&& isTweetComplete() && isFavoriteComplete();
	}

	public boolean isDisabled() throws Exception {
		return isEscaped() || isSuspended() || isProtected();
	}

	public boolean isEscaped() throws Exception {
		return isTrue("IS_ESCAPED");
	}

	public boolean isFavoriteComplete() throws Exception {
		return isTrue("FavoritesInfoComplete");
	}

	public boolean isFolloweeComplete() throws Exception {
		return isTrue("FolloweeInfoComplete");
	}

	public boolean isFollowerComplete() throws Exception {
		return isTrue("FollowerInfoComplete");
	}

	public boolean isInfoComplete() throws Exception {
		return isTrue("UserInfoComplete");
	}

	public boolean isProtected() throws Exception {
		return isTrue("IS_PROTECTED");
	}

	public boolean isSuspended() throws Exception {
		return isTrue("IS_SUSPENDED");
	}

	public boolean isTrue(String string) throws Exception {
		String val = get(string);
		// If != null and equals true
		if (val == null)
			return false;
		Boolean bool = Boolean.valueOf(val.toString());
		return bool;
	}

	public boolean isTweetComplete() throws Exception {
		return isTrue("TweetInfoComplete");
	}

	public void setComplete() throws Exception {
		set("UserComplete", "True");
	}

	public void setEscaped() throws Exception {
		set("IS_ESCAPED", "TRUE");
	}

	public void setFavoritesComplete() throws Exception {
		set("FavoritesInfoComplete", "True");
	}

	public void setFolloweeInfoComplete() throws Exception {
		set("FolloweeInfoComplete", "True");
	}

	public void setFollowerInfoComplete() throws Exception {
		set("FollowerInfoComplete", "True");
	}

	public void setInfoComplete() throws Exception {
		set("UserInfoComplete", "True");
	}

	public void setProtected() throws Exception {
		set("IS_PROTECTED", "TRUE");
	}

	public void setSuspended() throws Exception {
		set("IS_SUSPENDED", "TRUE");
	}

	public void setTweetsComplete() throws Exception {
		set("TweetInfoComplete", "True");
	}

	@Override
	public String toString() {
		JsonObject ret = new JsonObject();
		try {
			JsonArray arr = new JsonArray();
			JsonObject obj = new JsonObject();
			obj.addProperty("complete", isComplete());
			obj.addProperty("completed", isCompleted());
			obj.addProperty("disabled", isDisabled());
			obj.addProperty("escaped", isEscaped());
			obj.addProperty("fav_complete", isFavoriteComplete());
			obj.addProperty("followee_complete", isFolloweeComplete());
			obj.addProperty("follower_complete", isFollowerComplete());
			obj.addProperty("info_complete", isInfoComplete());
			obj.addProperty("protected", isProtected());
			obj.addProperty("suspended", isSuspended());
			arr.add(obj);
			ret.add("table", arr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret.toString();
	}

}