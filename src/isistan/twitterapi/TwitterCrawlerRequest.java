package isistan.twitterapi;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public abstract class TwitterCrawlerRequest<R> {
	public abstract R exec(Twitter twitter) throws Exception;
	
	public abstract RequestType getReqType();
}