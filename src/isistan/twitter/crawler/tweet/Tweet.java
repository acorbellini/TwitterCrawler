package isistan.twitter.crawler.tweet;

import isistan.twitter.crawler.util.CrawlerUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.UserMentionEntity;

import com.google.gson.JsonObject;

public class Tweet implements Serializable {

	public long user;
	public long tweetid;
	public Date created;
	public String source;
	public HashTagList hashtags;
	public MentionList mentions;
	public String text;
	public int retweetCount;
	public long retweetid;
	public Place place;
	public MediaList media;
	public long[] contrib;
	public Reply reply;
	public boolean favorited;
	public boolean possiblySensitive;
	public int favoriteCount;

	public Tweet() {
	}

	public Tweet(long user, long id, Date createdAt, String source,
			HashTagList hashTagList, MentionList m, String t, int rt,
			long retweetid, int favorites, boolean isFav, boolean isSensitive) {
		this.user = user;
		this.tweetid = id;
		this.created = createdAt;
		this.source = source;
		this.hashtags = hashTagList;
		this.mentions = m;
		this.text = t;
		this.retweetCount = rt;
		this.retweetid = retweetid;
		this.favoriteCount = favorites;
		this.favorited = isFav;
		this.possiblySensitive = isSensitive;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Tweet))
			return false;
		Tweet t = (Tweet) obj;
		return tweetid == t.tweetid;
	}

	public boolean fullEquals(Tweet other) {
		if (contrib == null) {
			if (other.contrib != null)
				return false;
		} else if (!contrib.equals(other.contrib))
			return false;
		if (created == null) {
			if (other.created != null)
				return false;
		} else if (!created.equals(other.created))
			return false;
		if (hashtags == null) {
			if (other.hashtags != null)
				return false;
		} else if (!hashtags.equals(other.hashtags))
			return false;
		if (media == null) {
			if (other.media != null)
				return false;
		} else if (!media.equals(other.media))
			return false;
		if (mentions == null) {
			if (other.mentions != null)
				return false;
		} else if (!mentions.equals(other.mentions))
			return false;
		if (place == null) {
			if (other.place != null)
				return false;
		} else if (!place.equals(other.place))
			return false;
		if (reply == null) {
			if (other.reply != null)
				return false;
		} else if (!reply.equals(other.reply))
			return false;
		if (retweetCount != other.retweetCount)
			return false;
		if (retweetid != other.retweetid)
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		if (tweetid != other.tweetid)
			return false;
		if (user != other.user)
			return false;
		if (favorited != other.favorited)
			return false;
		if (possiblySensitive != other.possiblySensitive)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		return Long.valueOf(tweetid).hashCode();
	}

	public void setContributors(long[] contrib) {
		this.contrib = contrib;

	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public void setFavoriteCount(int favoriteCount) {
		this.favoriteCount = favoriteCount;
	}

	public void setFavorited(boolean favorited) {
		this.favorited = favorited;
	}

	public void setHashtags(HashTagList hashtags) {
		this.hashtags = hashtags;
	}

	public void setMedia(MediaList mediaList) {
		this.media = mediaList;
	}

	public void setMentions(MentionList mentions) {
		this.mentions = mentions;
	}

	public void setPlace(Place p) {
		this.place = p;
	}

	public void setPossiblySensitive(boolean possiblySensitive) {
		this.possiblySensitive = possiblySensitive;
	}

	public void setReply(Reply r) {
		this.reply = r;
	}

	public void setRetweetCount(int retweetCount) {
		this.retweetCount = retweetCount;
	}

	public void setRetweetid(long retweetid) {
		this.retweetid = retweetid;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void setTweetid(long tweetid) {
		this.tweetid = tweetid;
	}

	public void setUser(long user) {
		this.user = user;
	}

	@Override
	public String toString() {
		JsonObject obj = new JsonObject();
		obj.addProperty("user", user);
		obj.addProperty("tweetid", tweetid);
		obj.addProperty("created", created.toString());
		obj.addProperty("source", source);
		obj.addProperty("hashtags", hashtags.toString());
		obj.addProperty("mentions", mentions.toString());
		obj.addProperty("text", text);
		obj.addProperty("retweetCount", retweetCount);
		obj.addProperty("retweetid", retweetid);
		obj.addProperty("place", place.toString());
		obj.addProperty("media", media.toString());
		obj.addProperty("contrib", Arrays.toString(contrib));
		obj.addProperty("reply", reply.toString());
		obj.addProperty("favorites", this.favoriteCount);
		obj.addProperty("favorited", this.favorited);
		obj.addProperty("possiblySensitive", this.possiblySensitive);
		return obj.toString();
		// return "{\"user\":\"" + user + "\", \"tweetid\":\"" + tweetid
		// + "\", \"created\":\"" + escape(created.toString())
		// + "\", \"source\":\"" + escape(source) + "\", \"hashtags\":\""
		// + escape(hashtags) + "\", \"mentions\":\"" + escape(mentions)
		// + "\", \"text\":\"" + escape(text) + "\", \"retweetCount\":\""
		// + retweetCount + "\", \"retweetid\":\"" + retweetid
		// + "\", \"place\":\"" + escape(place.toString())
		// + "\", \"media\":\"" + escape(media) + "\", \"contrib\":\""
		// + escape(contrib) + "\", \"reply\":\""
		// + escape(reply.toString()) + "\"}";
	}

	public static MentionList formatMentionEntities(
			UserMentionEntity[] userMentionEntities) {
		MentionList ret = new MentionList();
		for (UserMentionEntity userMentionEntity : userMentionEntities) {
			ret.addNew(userMentionEntity.getId(),
					userMentionEntity.getScreenName(),
					CrawlerUtil.escape(userMentionEntity.getName()),
					userMentionEntity.getStart(), userMentionEntity.getEnd());
		}
		return ret;
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

	public static MediaList formatMedia(MediaEntity[] mediaEntities) {
		MediaList ret = new MediaList();
		for (MediaEntity mediaEntity : mediaEntities) {
			ret.addNew(mediaEntity.getId(), mediaEntity.getType(),
					mediaEntity.getURL(), mediaEntity.getStart(),
					mediaEntity.getEnd());
		}
		return ret;
	}

	public static HashTagList formatHashtags(HashtagEntity[] hashtagEntities) {
		HashTagList list = new HashTagList();
		for (HashtagEntity hashtagEntity : hashtagEntities) {
			list.addNew(hashtagEntity.getText(), hashtagEntity.getStart(),
					hashtagEntity.getEnd());
		}
		return list;
	}

	public static String formatPlace(twitter4j.Place place) {
		if (place == null)
			return ";;;;;;;;";// Separadores
		return place.getBoundingBoxType() + ";" + place.getCountry() + ";"
				+ place.getCountryCode() + ";" + place.getFullName() + ";"
				+ place.getGeometryType() + ";" + place.getId() + ";"
				+ place.getName() + ";" + place.getStreetAddress() + ";"
				+ formatCoordinates(place.getBoundingBoxCoordinates()) + ";";
	}

}
