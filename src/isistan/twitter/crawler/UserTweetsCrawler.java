package isistan.twitter.crawler;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import isistan.twitterapi.RequestType;
import isistan.twitterapi.TwitterCrawlerRequest;

import java.io.IOException;

import org.apache.log4j.Logger;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import utils.textcat.TextCategorizer;

public class UserTweetsCrawler {

	private UserStatus status;
	private long user;
	private TweetType type;

	private Logger log = Logger.getLogger(UserTweetsCrawler.class);

	public UserTweetsCrawler(UserStatus status, long user, TweetType type) {
		this.status = status;
		this.user = user;
		this.type = type;
	}

	public void crawl() {
		try {
			if (!isComplete()) {
				log.info("Storing " + type + " for " + user);
				saveTweets();
				setComplete();
				log.info("Finished storing " + type + " for " + user);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setComplete() throws Exception {
		if (type.equals(TweetType.TWEETS))
			status.setTweetsComplete();
		else
			status.setFavoritesComplete();
	}

	private boolean isComplete() throws Exception {
		if (type.equals(TweetType.TWEETS))
			return status.isTweetComplete();
		return status.isFavoriteComplete();
	}

	public void saveTweets() throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		int page = 1;

		TwitterCrawlerStore store = config.getStore();
		if (status.has("Page-" + type)) {
			page = Integer.valueOf(status.get("Page-" + type));
			log.info("Resuming tweet crawling(" + type + ") for " + user
					+ " on page " + page);
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
			store.writeTweetsHeader(user, type, header);
		}

		ResponseList<Status> stats = null;
		do {
			final Paging paging = new Paging(page++, 200);

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
						public String toString() {
							return "GetTweets - Type: " + type + " User: "
									+ user;
						}

						@Override
						public RequestType getReqType() {
							return type.equals(TweetType.TWEETS) ? RequestType.TWEETS
									: RequestType.FAVORITES;
						}

					});

			if (type.equals(TweetType.TWEETS) && page == 1) {
				StringBuilder b = new StringBuilder();
				for (Status status : stats) {
					b.append(" " + status.getText());
				}
				TextCategorizer cat = new TextCategorizer();
				String[] list = cat.categorize(b.toString());
				if (!list[0].equals("english")) {
					log.info("Escaping " + user + " identified language as "
							+ list[0]);
					status.setEscaped();
					return;
				}
			}

			if (stats != null) {
				log.info("Obtained " + stats.size() + " tweets (" + type
						+ ") for user " + user);
				store.writeTweets(user, type, stats);
			}
			status.set("Page-" + type, Integer.valueOf(page).toString());
		} while (stats != null && !stats.isEmpty());
		store.finishedTweets(user, type);
	}

}
