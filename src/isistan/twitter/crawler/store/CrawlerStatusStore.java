package isistan.twitter.crawler.store;

public interface CrawlerStatusStore {
	public void setCrawlerStatus(String prop, String val);

	public void setUserStatus(long user, String prop, String val);
}
