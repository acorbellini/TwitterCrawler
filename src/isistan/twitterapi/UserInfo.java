package isistan.twitterapi;

import isistan.twitter.crawler.CrawlerUtil;

import java.util.Date;

public class UserInfo {
	private long id;
	private String screenName;
	private String name;
	private String description;
	private String lang;
	private Date created;
	private String loc;
	private String timeZone;
	private int utcOffset;

	private int followeesCount;
	private int followersCount;
	private int favCount;
	private int listedCount;
	private int tweetsCount;
	private boolean isVerified;
	private boolean isProtected;
	private Boolean isSuspended;

	public String getTitle() {
		return "ID;" + "SCREEN_NAME;" + "NAME;" + "DESCRIPTION;" + "LANG;"
				+ "CREATED;" + "LOCATION;" + "TIMEZONE;" + "UTC;"
				+ "FOLLOWEESCOUNT;" + "FOLLOWERSCOUNT;" + "FAVCOUNT;"
				+ "LISTEDCOUNT;" + "TWEETSCOUNT;" + "ISVERIFIED;"
				+ "ISPROTECTED";
	}

	@Override
	public String toString() {
		return id + ";" + screenName + ";" + name + ";" + description + ";"
				+ lang + ";" + created + ";" + loc + ";" + timeZone + ";"
				+ utcOffset + ";" + followeesCount + ";" + followersCount + ";"
				+ favCount + ";" + listedCount + ";" + tweetsCount + ";"
				+ isVerified + ";" + isProtected;
	}

	public Date getCreated() {
		return created;
	}

	public String getDescription() {
		return description;
	}

	public int getFavCount() {
		return favCount;
	}

	public int getFolloweesCount() {
		return followeesCount;
	}

	public int getFollowersCount() {
		return followersCount;
	}

	public String getName() {
		return name;
	}

	public String getScreenName() {
		return screenName;
	}

	public String getLoc() {
		return loc;
	}

	public String getLang() {
		return lang;
	}

	public int getListedCount() {
		return listedCount;
	}

	public int getTweetsCount() {
		return tweetsCount;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public int getUtcOffset() {
		return utcOffset;
	}

	public void setCreated(Date createdAt) {
		this.created = createdAt;
	}

	public void setDescription(String description) {
		this.description = CrawlerUtil.escape(description);
	}

	public void setFavCount(int favouritesCount) {
		this.favCount = favouritesCount;
	}

	public void setFolloweesCount(int friendsCount) {
		this.followeesCount = friendsCount;
	}

	public void setFollowersCount(int followersCount) {
		this.followersCount = followersCount;
	}

	public void setName(String name) {
		this.name = CrawlerUtil.escape(name);
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public void setLoc(String location) {
		this.loc = CrawlerUtil.escape(location);
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public void setListedCount(int listedCount) {
		this.listedCount = listedCount;
	}

	public void setTweetsCount(int statusesCount) {
		this.tweetsCount = statusesCount;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	public void setUtcOffset(int utcOffset) {
		this.utcOffset = utcOffset;
	}

	public void setId(long id) {
		this.id = id;
	}

	public void setIsVerified(boolean verified) {
		this.isVerified = verified;
	}

	public boolean isVerified() {
		return isVerified;
	}

	public void setIsProtected(boolean protected1) {
		this.isProtected = protected1;
	}

	public boolean isProtected() {
		return isProtected;
	}

	public void setIsSuspended(Boolean s) {
		this.isSuspended = s;
	}

	public Boolean isSuspended() {
		return isSuspended;
	}

}
