package isistan.twitter.crawler.request;

import isistan.twitter.crawler.adjacency.ListType;
import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

public final class GetFriendsRequest extends TwitterCrawlerRequest<IDs> {
	private final ListType type;
	private final long userId;
	private long next;

	public GetFriendsRequest(ListType type, long userId, long next) {
		this.type = type;
		this.userId = userId;
		this.next = next;
	}

	@Override
	public IDs exec(Twitter twitter) throws TwitterException {
		if (type.equals(ListType.FOLLOWEES))
			return twitter.getFriendsIDs(userId, next);
		else if (type.equals(ListType.FOLLOWERS))
			return twitter.getFollowersIDs(userId, next);
		return null;
	}

	@Override
	public RequestType getReqType() {
		return type.equals(ListType.FOLLOWEES) ? RequestType.FOLLOWEES
				: RequestType.FOLLOWERS;
	}

	@Override
	public String toString() {
		return "GetFriendsRequest - Type: " + type + " userID:" + userId;
	}
}