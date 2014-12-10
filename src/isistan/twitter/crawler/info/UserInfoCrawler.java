package isistan.twitter.crawler.info;

import isistan.twitter.crawler.CrawlerUtil;
import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import isistan.twitterapi.RequestType;
import isistan.twitterapi.TwitterCrawlerRequest;
import isistan.twitterapi.UserInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.Logger;

import twitter4j.ResponseList;
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
			public String toString() {
				return "UserInfo - User: " + u;
			}

			@Override
			public RequestType getReqType() {
				return RequestType.INFO;
			}
		});

		if (user == null)
			status.setSuspended();
		else {
			TwitterCrawlerStore store = CrawlerConfiguration.getCurrent()
					.getStore();
			store.writeInfo(user);
			if (user.isProtected())
				status.setProtected();

			if (CrawlerUtil.filter(user))
				status.setEscaped();

		}

		status.setInfoComplete();
	}
}
