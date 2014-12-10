package isistan.twitter.crawler.test;

import isistan.twitterapi.TwitterCrawler;

import java.util.Properties;

public class CrawlerTest {
	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		prop.setProperty("oauthdir", "./crawler/oauth");
		prop.setProperty("input",
				"C:/Users/acorbellini/Desktop/Split 3rd Layer/En curso/3rdLayer-SplitPart-am");
		prop.setProperty("output", "C:/Users/acorbellini/Desktop/PruebaCrawler");

		TwitterCrawler crawler = new TwitterCrawler();
		crawler.run(prop);
	}
}
