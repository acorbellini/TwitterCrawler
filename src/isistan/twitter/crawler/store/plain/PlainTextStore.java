package isistan.twitter.crawler.store.plain;

import isistan.twitter.crawler.CrawlerUtil;
import isistan.twitter.crawler.ListType;
import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.TwitterCrawlerStore;
import isistan.twitterapi.UserInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.UserMentionEntity;

public class PlainTextStore implements TwitterCrawlerStore {

	Logger log = Logger.getLogger(PlainTextStore.class);
	private Properties ltProp;
	private String outputdir;

	@Override
	public void writeInfo(User user) throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		final long u = user.getId();

		File f = new File(config.getUserCrawlDir(u) + "/" + u + "-info.dat");
		if (!f.exists()) {
			Files.createDirectories(Paths.get(config.getUserCrawlDir(u) + "/"));
			f.createNewFile();
		} else
			log.info("User info for user " + u + " already exist.");

		UserInfo info = new UserInfo();
		info.setId(user.getId());
		info.setCreated(user.getCreatedAt());
		info.setDescription(user.getDescription());
		info.setFavCount(user.getFavouritesCount());
		info.setFollowersCount(user.getFollowersCount());
		info.setFolloweesCount(user.getFriendsCount());
		info.setLang(user.getLang());
		info.setName(user.getName());
		info.setListedCount(user.getListedCount());
		info.setLoc(user.getLocation());
		info.setScreenName(user.getScreenName());
		info.setIsVerified(user.isVerified());
		info.setIsProtected(user.isProtected());
		info.setIsSuspended(CrawlerUtil.isSuspended(u));
		// Status status = user.getStatus();
		// long[] status_contrib = status.getContributors();
		// Date status_date = status.getCreatedAt();
		// long status_curr_user_retweet = status.getCurrentUserRetweetId();
		// GeoLocation geo = status.getGeoLocation();
		// double status_geo_lat = geo.getLatitude();
		// double status_geo_long = geo.getLongitude();
		// HashtagEntity[] entities = status.getHashtagEntities();
		// Place place = status.getPlace();

		info.setTweetsCount(user.getStatusesCount());
		info.setTimeZone(user.getTimeZone());
		info.setUtcOffset(user.getUtcOffset());

		BufferedWriter uInfoWriter = new BufferedWriter(new FileWriter(f));
		uInfoWriter.write(info.getTitle() + "\r\n");
		uInfoWriter.write(info.toString() + "\r\n");
		uInfoWriter.close();
	}

	@Override
	public void addAdjacency(long u, ListType type, long[] list)
			throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		for (Long friend : list) {
			File f = new File(config.getUserCrawlDir(u) + "/" + u + "-" + type
					+ ".dat");
			if (!f.exists())
				f.createNewFile();
			BufferedWriter writer = new BufferedWriter(new FileWriter(f));
			writer.write(Long.valueOf(friend).toString() + "\r\n");
			writer.close();
		}
	}

	@Override
	public void writeTweets(long user, TweetType type,
			ResponseList<Status> stats) throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		File f = new File(config.getUserCrawlDir(user) + "/" + user + "-"
				+ type + ".dat");
		if (!f.exists())
			f.createNewFile();
		BufferedWriter tweetsWriter = new BufferedWriter(new FileWriter(f));
		for (Status status : stats) {
			tweetsWriter.write(status.getId()
					+ ";"
					+ status.getCreatedAt()
					+ ";"
					+ Arrays.toString(status.getContributors())
					+ ";"
					+ status.getInReplyToUserId()
					+ ";"
					+ status.getInReplyToScreenName()
					+ ";"
					+ status.getInReplyToStatusId()
					+ ";"
					+ status.getCurrentUserRetweetId()
					+ ";"
					+ status.getRetweetCount()
					+ ";"
					+ CrawlerUtil.escape(status.getSource())
					+ ";"
					+ (status.getGeoLocation() != null ? status
							.getGeoLocation().getLatitude()
							+ ";"
							+ status.getGeoLocation().getLongitude() + ";"
							: ";;")
					+ formatHashtags(status.getHashtagEntities()) + ";"
					+ formatMedia(status.getMediaEntities()) + ";"
					+ formatPlace(status.getPlace()) + ";"
					// + status.getRetweetedStatus() + ";"
					+ formatMentionEntities(status.getUserMentionEntities())
					+ ";" + CrawlerUtil.escape(status.getText()) + "\r\n");
		}
		tweetsWriter.close();

	}

	public static String formatMentionEntities(
			UserMentionEntity[] userMentionEntities) {
		StringBuffer buf = new StringBuffer();
		for (UserMentionEntity userMentionEntity : userMentionEntities) {
			buf.append("-[" + userMentionEntity.getId() + ","
					+ userMentionEntity.getScreenName() + ","
					+ CrawlerUtil.escape(userMentionEntity.getName()) + ","
					+ userMentionEntity.getStart() + ","
					+ userMentionEntity.getEnd() + "]");
		}
		if (buf.length() == 0)
			return "";
		return buf.substring(1);
	}

	public static String formatPlace(Place place) {
		if (place == null)
			return ";;;;;;;;";// Separadores
		return place.getBoundingBoxType() + ";" + place.getCountry() + ";"
				+ place.getCountryCode() + ";" + place.getFullName() + ";"
				+ place.getGeometryType() + ";" + place.getId() + ";"
				+ place.getName() + ";" + place.getStreetAddress() + ";"
				+ formatCoordinates(place.getBoundingBoxCoordinates()) + ";";
	}

	public static String formatCoordinates(
			GeoLocation[][] boundingBoxCoordinates) {
		if (boundingBoxCoordinates == null)
			return "";

		StringBuffer buff = new StringBuffer();
		for (GeoLocation[] geoLocations : boundingBoxCoordinates) {

			StringBuffer buff2 = new StringBuffer();
			for (GeoLocation geoLocation : geoLocations) {
				buff2.append("-" + geoLocation.getLatitude() + ","
						+ geoLocation.getLongitude());
			}

			buff.append(":" + buff2.substring(1));

		}
		return buff.substring(1);
	}

	public static String formatMedia(MediaEntity[] mediaEntities) {
		StringBuffer buf = new StringBuffer();
		for (MediaEntity mediaEntity : mediaEntities) {
			buf.append("-[" + mediaEntity.getId() + "," + mediaEntity.getType()
					+ "," + mediaEntity.getURL() + "," + mediaEntity.getStart()
					+ "," + mediaEntity.getEnd() + "]");
		}
		if (buf.length() == 0)
			return "";
		return buf.substring(1);
	}

	public static String formatHashtags(HashtagEntity[] hashtagEntities) {
		StringBuffer buf = new StringBuffer();
		for (HashtagEntity hashtagEntity : hashtagEntities) {
			buf.append("-[" + hashtagEntity.getText() + ","
					+ hashtagEntity.getStart() + "," + hashtagEntity.getEnd()
					+ "]");
		}
		if (buf.length() == 0)
			return "";
		return buf.substring(1);
	}

	@Override
	public void writeTweetsHeader(long user, TweetType type, String header)
			throws Exception {
		CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
		File f = new File(config.getUserCrawlDir(user) + "/" + user + "-"
				+ type + ".dat");
		if (!f.exists())
			f.createNewFile();
		BufferedWriter tweetsWriter = new BufferedWriter(new FileWriter(f));
		tweetsWriter.write(header);
		tweetsWriter.close();
	}

	@Override
	public void finishedTweets(long user, TweetType type) {
	}

	@Override
	public void finishedAdjacency(long user, ListType type) {
	}

	public PlainTextStore() throws FileNotFoundException, IOException {
		this.outputdir = CrawlerConfiguration.getCurrent().getOutputdir();
		this.ltProp = CrawlerUtil.openProperties(outputdir
				+ "/LatestCrawled.prop");
	}

	public Long getLatestCrawled() {
		String lt = ltProp.getProperty("LatestCrawled");
		if (lt != null)
			return Long.valueOf(lt);
		return null;
	}

	@Override
	public void updateLatestCrawled(long user) throws Exception {
		CrawlerUtil.updateProperty("LatestCrawled", user + "", ltProp,
				outputdir + "/LatestCrawled.prop");
	}

	@Override
	public UserStatus getUserStatus(long u) throws Exception {
		return new PlainUserStatus(u);

	}

}
