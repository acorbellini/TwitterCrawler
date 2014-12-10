package isistan.twitter.crawler.status;

import isistan.twitter.crawler.AdjacencyListCrawler;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.UserTweetsCrawler;
import isistan.twitter.crawler.info.UserInfoCrawler;

public abstract class UserStatus {

	protected long user;

	public UserStatus(long user) {
		this.user = user;
	}

	public abstract void set(String k, String v) throws Exception;

	public abstract String get(String string) throws Exception;

	public boolean has(String string) throws Exception {
		return get(string) != null;
	}

	public boolean isTrue(String string) throws Exception {
		String val = get(string);
		// If != null and equals true
		if (val == null)
			return false;
		Boolean bool = Boolean.valueOf(val.toString());
		return bool;
	}

	public boolean isCompleted() throws Exception {
		return isInfoComplete() && isFolloweeComplete() && isFollowerComplete()
				&& isTweetComplete() && isFavoriteComplete();
	}

	public boolean isFavoriteComplete() throws Exception {
		return isTrue("FavoritesInfoComplete");
	}

	public boolean isTweetComplete() throws Exception {
		return isTrue("TweetInfoComplete");
	}

	public boolean isFollowerComplete() throws Exception {
		return isTrue("FollowerInfoComplete");
	}

	public boolean isFolloweeComplete() throws Exception {
		return isTrue("FolloweeInfoComplete");
	}

	public boolean isInfoComplete() throws Exception {
		return isTrue("UserInfoComplete");
	}

	public boolean isDisabled() throws Exception {
		return isEscaped() || isSuspended() || isProtected();
	}

	public boolean isProtected() throws Exception {
		return isTrue("IS_PROTECTED");
	}

	public boolean isSuspended() throws Exception {
		return isTrue("IS_SUSPENDED");
	}

	public boolean isEscaped() throws Exception {
		return isTrue("IS_ESCAPED");
	}

	public void setInfoComplete() throws Exception {
		set("UserInfoComplete", "True");
	}

	public void setSuspended() throws Exception {
		set("IS_SUSPENDED", "TRUE");
	}

	public void setProtected() throws Exception {
		set("IS_PROTECTED", "TRUE");
	}

	public void setEscaped() throws Exception {
		set("IS_ESCAPED", "TRUE");
	}

	public void setFolloweeInfoComplete() throws Exception {
		set("FolloweeInfoComplete", "True");
	}

	public void setTweetsComplete() throws Exception {
		set("TweetInfoComplete", "True");
	}

	public void setFavoritesComplete() throws Exception {
		set("FavoritesInfoComplete", "True");
	}

	public UserInfoCrawler getInfoCrawler() {
		return new UserInfoCrawler(this, user);
	}

	public AdjacencyListCrawler getFolloweeCrawler() {
		return new AdjacencyListCrawler(this, user, ListType.FOLLOWEES);
	}

	public AdjacencyListCrawler getFollowerCrawler() {
		return new AdjacencyListCrawler(this, user, ListType.FOLLOWERS);
	}

	public void setFollowerInfoComplete() throws Exception {
		set("FollowerInfoComplete", "True");
	}

	public UserTweetsCrawler getTweetCrawler() {
		return new UserTweetsCrawler(this, user, TweetType.TWEETS);
	}

	public UserTweetsCrawler getFavCrawler() {
		return new UserTweetsCrawler(this, user, TweetType.FAVORITES);
	}

	public void setComplete() throws Exception {
		set("UserComplete", "True");
	}

	public long getUser() {
		return user;
	}

	public boolean isComplete() throws Exception {
		return isTrue("UserComplete");
	}

}