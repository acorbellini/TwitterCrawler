package isistan.twitter.crawler.request;

import twitter4j.Twitter;

public abstract class TwitterCrawlerRequest<R> {
	public abstract R exec(Twitter twitter) throws Exception;
	
	public abstract RequestType getReqType();
}