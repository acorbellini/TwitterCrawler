package isistan.twitter.crawler;

import twitter4j.TwitterException;

public class CrawlerErrorCodes {
	public static boolean badAuthentication(TwitterException e) {
		return e.getMessage().contains("Bad Authentication data")
				|| e.getMessage().contains("Could not authenticate you")
				|| e.getMessage().contains("Invalid or expired token");
	}
	public static boolean invalidOrExpired(TwitterException e) {
		return e.getMessage().contains("Invalid or expired token");
	}
	public static boolean isNotAuthorized(TwitterException e) {
		return e.getErrorCode() == CrawlerErrorCodes.NOT_AUTHORIZED
				&& e.getStatusCode() == 401;
	}
	public static boolean isSuspended(TwitterException e) {
		return e.getErrorCode() == CrawlerErrorCodes.SUSPENDED;
	}

	public static boolean overcapacity(TwitterException e) {
		return e.getErrorCode() == CrawlerErrorCodes.OVER_CAPACITY;
	}

	public static boolean timedOut(TwitterException e) {
		return e.getMessage().contains("timed out")
				|| e.getMessage().contains("Connection reset")
				|| e.getMessage().contains("api.twitter.com");
	}

	public static boolean userDoesNotExist(TwitterException e) {
		return e.getErrorCode() == CrawlerErrorCodes.DOES_NOT_EXISTS;
	}

	public static final int NOT_AUTHORIZED = -1;

	public static final int DOES_NOT_EXISTS = 34;

	public static final int OVER_CAPACITY = 130;

	public static final int SUSPENDED = 63;
}
