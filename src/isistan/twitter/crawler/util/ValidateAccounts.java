package isistan.twitter.crawler.util;

import isistan.twitter.crawler.TwitterOauthBuilder;
import isistan.twitter.crawler.config.CrawlerConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ValidateAccounts {
	public static void main(String[] args) throws Exception {
		final String configFile = args[0];
		Properties config = new Properties();
		FileInputStream fis = new FileInputStream(new File(configFile));
		config.load(fis);
		File oauthdir = new File(
				config.getProperty(CrawlerConfiguration.OAUTHDIR));
		if (oauthdir.isDirectory() && oauthdir.exists()) {
			for (File f : oauthdir.listFiles()) {
				if (f.isFile()) {
					TwitterOauthBuilder.build(f);
				}
			}
		}
	}

}
