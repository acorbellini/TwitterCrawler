package isistan.twitter.crawler.tweet;

import java.util.Arrays;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.request.RequestType;
import isistan.twitter.crawler.request.TwitterCrawlerRequest;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.textcat.TextCategorizer;
import isistan.twitter.crawler.util.CrawlerUtil;

import org.apache.log4j.Logger;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;

public class UserTweetsCrawler {

	private UserStatus status;
	private long user;
	private TweetType type;

	private Logger log = Logger.getLogger(UserTweetsCrawler.class);
	private boolean force;
	private String lang;

	public UserTweetsCrawler(UserStatus status, long user, TweetType type,
			boolean force, String lang) {
		this.status = status;
		this.user = user;
		this.type = type;
		this.force = force;
		this.lang = lang;
	}

	public void crawl(String username) {
		try {
			if (force || !isComplete()) {
				log.info("Storing " + type + " for " + user + "(@" + username
						+ ").");
				saveTweets(username);
				setComplete();
				log.info("Finished storing " + type + " for " + user + "(@"
						+ username + ").");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean isComplete() throws Exception {
		if (type.equals(TweetType.TWEETS))
			return status.isTweetComplete();
		return status.isFavoriteComplete();
	}

	public void saveTweets(final String username) throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		int page = 1;

		TwitterStore store = config.getStore();
		if (status.has("Page-" + type)) {
			page = Integer.valueOf(status.get("Page-" + type)) + 1;
			log.info("Resuming tweet crawling(" + type + ") for " + user + "(@"
					+ username + ") " + " on page " + page);
		} else {
			String header = "ID;" + "CREATED;" + "CONTRIBUTORS;"
					+ "INREPLYTOUSERID;" + "INREPLYTOUSERSCREENNAME;"
					+ "INREPLYTOTWEETID;" + "RETWEETID;" + "RETWEETCOUNT;"
					+ "SOURCE;" + "GEO_LAT;" + "GEO_LONG;" + "HASHTAGS;"
					+ "MEDIA;" + "PLACE_BOUNDING_BOX;" + "PLACE_COUNTRY;"
					+ "PLACE_COUNTRY_CODE;" + "PLACE_FULL_NAME;"
					+ "PLACE_GEOMETRY_TYPE;" + "PLACE_ID;" + "PLACE_NAME;"
					+ "PLACE_STREET_ADDR;" + "PLACE_BOUNDING_BOX_COORD;"
					+ "MENTIONS;" + "TEXT" + "\r\n";
			// store.writeTweetsHeader(user, type, header);
		}

		ResponseList<Status> stats = null;
		do {
			final Paging paging = new Paging(page, 200);

			stats = CrawlerUtil
					.get(new TwitterCrawlerRequest<ResponseList<Status>>() {
						@Override
						public ResponseList<Status> exec(Twitter twitter)
								throws Exception {
							if (type.equals(TweetType.TWEETS))
								return twitter.getUserTimeline(user, paging);
							else if (type.equals(TweetType.FAVORITES))
								return twitter.getFavorites(user, paging);
							return null;
						}

						@Override
						public RequestType getReqType() {
							return type.equals(TweetType.TWEETS) ? RequestType.TWEETS
									: RequestType.FAVORITES;
						}

						@Override
						public String toString() {
							return "GetTweets - Type: " + type + " User: "
									+ user + "(@" + username + ")";
						}

					});

			if (stats != null && page == 1
					&& !lang.equals("any") && (type.equals(TweetType.TWEETS))) {
				StringBuilder b = new StringBuilder();
				for (Status status : stats) {
					b.append(" " + status.getText());
				}
				TextCategorizer cat = new TextCategorizer();
				String[] list = cat.categorize(b.toString());
				boolean containsLang = false;
				for (String string : list) {
					if (string.equals(lang))
						containsLang = true;
				}
				if (!containsLang) {
					log.info("Escaping " + user + "(@" + username + ")"
							+ " identified language as "
							+ Arrays.toString(list));
					status.setEscaped();
					return;
				}
			}

			if (stats != null) {
				if (log.isDebugEnabled())
					log.debug("Obtained " + stats.size() + " tweets (" + type
							+ ") for user " + user + "(@" + username + ").");
				store.writeTweets(user, type, stats);
			}
			status.set("Page-" + type, Integer.valueOf(page).toString());

			page++;
		} while (stats != null && !stats.isEmpty());
	}

	private void setComplete() throws Exception {
		if (type.equals(TweetType.TWEETS))
			status.setTweetsComplete();
		else
			status.setFavoritesComplete();
	}

}
