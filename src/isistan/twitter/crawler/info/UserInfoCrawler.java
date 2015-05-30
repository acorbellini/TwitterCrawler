package isistan.twitter.crawler.info;

import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.request.RequestType;
import isistan.twitter.crawler.request.TwitterCrawlerRequest;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.util.CrawlerUtil;

import org.apache.log4j.Logger;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

public class UserInfoCrawler {

	Logger log = Logger.getLogger(UserInfoCrawler.class);

	private Long u;

	private UserStatus status;

	public UserInfoCrawler(UserStatus userStatus, Long user) {
		this.status = userStatus;
		this.u = user;
	}

	public void crawl() throws Exception {
		User user = CrawlerUtil.get(new TwitterCrawlerRequest<User>() {
			@Override
			public User exec(Twitter twitter) throws TwitterException {
				return twitter.showUser(u);
			}

			@Override
			public RequestType getReqType() {
				return RequestType.INFO;
			}

			@Override
			public String toString() {
				return "UserInfo - User: " + u;
			}
		});

		if (user == null)
			status.setSuspended();
		else {
			TwitterStore store = CrawlerConfiguration.getCurrent().getStore();
			store.writeInfo(user);
			if (user.isProtected())
				status.setProtected();

			if (CrawlerUtil.filter(user))
				status.setEscaped();

		}

		status.setInfoComplete();
	}
}
