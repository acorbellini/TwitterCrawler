package isistan.twitter.crawler.adjacency;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.request.GetFriendsRequest;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.util.CrawlerUtil;

import org.apache.log4j.Logger;

import twitter4j.IDs;

public class UserAdjacencyListCrawler {

	private long u;
	private ListType type;
	private UserStatus status;
	private Logger log = Logger.getLogger(UserAdjacencyListCrawler.class);
	private boolean force;

	public UserAdjacencyListCrawler(UserStatus status, long u, ListType type,
			boolean force) {
		this.status = status;
		this.u = u;
		this.type = type;
		this.force = force;
	}

	public void crawl(String username) {
		try {
			if (force || !isComplete()) {
				log.info("Storing " + type + " for " + u + "(@" + username
						+ ").");
				saveList(username);
				setComplete();

				log.info("Finished storing " + type + " for " + u + "(@"
						+ username + ").");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean isComplete() throws Exception {
		if (type.equals(ListType.FOLLOWEES))
			return status.isFolloweeComplete();
		else
			return status.isFollowerComplete();
	}

	private void saveList(String username) throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		long next = -1;
		int cont = 0;
		if (status.has("FriendCrawlCursor_" + type)) {
			next = Long.valueOf(status.get("FriendCrawlCursor_" + type));
			cont = Integer.valueOf(status.get("FriendCrawlPosition_" + type));
			log.info("Resuming friend crawling(" + type + ") for " + u + "(@"
					+ username + ")." + " on cursor " + next);
		}

		TwitterStore store = config.getStore();
		IDs ids = null;
		do {
			ids = CrawlerUtil.get(new GetFriendsRequest(type, u, next));

			if (ids != null) {
				// if (log.isDebugEnabled())
				log.info("Obtained " + ids.getIDs().length + " friends ("
						+ "(@" + username + ")." + type + ") for user " + u
						+ "(@" + username + ").");
				store.addAdjacency(u, cont, type, ids.getIDs());
				cont++;
				if (ids.hasNext()) {
					next = ids.getNextCursor();
					status.set("FriendCrawlCursor_" + type, Long.valueOf(next)
							.toString());
					status.set("FriendCrawlPosition_" + type,
							Integer.valueOf(cont).toString());
				}
			}
		} while (ids != null && ids.hasNext() && !status.has("IS_ESCAPED"));

	}

	private void setComplete() throws Exception {
		if (type.equals(ListType.FOLLOWEES))
			status.setFolloweeInfoComplete();
		else
			status.setFollowerInfoComplete();
	}

}
