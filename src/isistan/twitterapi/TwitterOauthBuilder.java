package isistan.twitterapi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.conf.PropertyConfiguration;

public class TwitterOauthBuilder {
	public static Twitter build(File prop) throws Exception {
		Properties conf = new Properties();
		conf.load(new FileInputStream(prop));
		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setOAuthConsumerKey(conf.getProperty("oauth.consumerKey"));
		cb.setOAuthConsumerSecret(conf.getProperty("oauth.consumerSecret"));
		cb.setOAuthAccessToken(conf.getProperty("oauth.accessToken"));
		cb.setOAuthAccessTokenSecret(conf
				.getProperty("oauth.accessTokenSecret"));

		cb.setRestBaseURL("https://api.twitter.com/1.1/");
		cb.setOAuthRequestTokenURL("https://api.twitter.com/oauth/request_token");
		cb.setOAuthAccessTokenURL("https://api.twitter.com/oauth/access_token");
		cb.setOAuthAuthenticationURL("https://api.twitter.com/oauth/authenticate");
		cb.setOAuthAuthorizationURL("https://api.twitter.com/oauth/authorize");

		Configuration config = cb.build();

		Twitter twitter = new TwitterFactory(config).getInstance();
		AccessToken accessToken = null;
		try {
			RequestToken requestToken = twitter.getOAuthRequestToken();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			while (null == accessToken) {
				System.out
						.println("Open the following URL and grant access to your account:");
				System.out.println(requestToken.getAuthorizationURL());
				System.out
						.print("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
				String pin = br.readLine();
				try {
					if (pin.length() > 0) {
						accessToken = twitter.getOAuthAccessToken(requestToken,
								pin);
					} else {
						accessToken = twitter.getOAuthAccessToken();
					}
				} catch (TwitterException te) {
					if (401 == te.getStatusCode()) {
						System.out.println("Unable to get the access token.");
					} else {
						// te.printStackTrace();
					}
				}
			}
			Properties propFile = new Properties();
			propFile.load(new FileInputStream(prop));
			propFile.put("oauth.accessToken", accessToken.getToken());
			propFile.put("oauth.accessTokenSecret",
					accessToken.getTokenSecret());
			propFile.store(new FileOutputStream(prop), "");
		} catch (Exception e) {
			// e.printStackTrace();
		}
		return twitter;
	}

	public static void main(String[] args) throws Exception {
		for (File f : new File(args[0]).listFiles())
			build(f);
	}
}
