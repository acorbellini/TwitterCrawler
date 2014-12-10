package isistan.twitter.crawler.folder;

public interface UserProvider {
	int getNextUser();

	boolean hasNext();
}
