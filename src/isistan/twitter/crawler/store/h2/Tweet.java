package isistan.twitter.crawler.store.h2;

import java.io.Serializable;
import java.util.Date;

public class Tweet implements Serializable {

	public long user;
	public long tweetid;
	public Date created;
	public String source;
	public String hashtags;
	public String mentions;
	public String text;
	public int retweetCount;
	public long retweetid;
	public Place place;
	public String media;
	public String contrib;
	public Reply reply;
	public boolean favorited;
	public boolean possiblySensitive;
	public int favoriteCount;

	public Tweet() {
	}

	public Tweet(long user, long id, Date createdAt, String source, String h,
			String m, String t, int rt, long retweetid, int favorites,
			boolean isFav, boolean isSensitive) {
		this.user = user;
		this.tweetid = id;
		this.created = createdAt;
		this.source = source;
		this.hashtags = h;
		this.mentions = m;
		this.text = t;
		this.retweetCount = rt;
		this.retweetid = retweetid;
		this.favoriteCount = favorites;
		this.favorited = isFav;
		this.possiblySensitive = isSensitive;
	}

	public void setPlace(String country, String fullName, String name) {
		this.place = new Place(country, fullName, name);
	}

	public void setMedia(String media) {
		this.media = media;
	}

	public void setContributors(String contrib) {
		this.contrib = contrib;

	}

	public void setReply(String inReplyToScreenName, long inReplyToUserId,
			long inReplyToStatusId) {
		this.reply = new Reply(inReplyToUserId, inReplyToScreenName,
				inReplyToStatusId);
	}

	public void setUser(long user) {
		this.user = user;
	}

	public void setFavoriteCount(int favoriteCount) {
		this.favoriteCount = favoriteCount;
	}

	public void setFavorited(boolean favorited) {
		this.favorited = favorited;
	}

	public void setPossiblySensitive(boolean possiblySensitive) {
		this.possiblySensitive = possiblySensitive;
	}

	public void setTweetid(long tweetid) {
		this.tweetid = tweetid;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setHashtags(String hashtags) {
		this.hashtags = hashtags;
	}

	public void setMentions(String mentions) {
		this.mentions = mentions;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void setRetweetCount(int retweetCount) {
		this.retweetCount = retweetCount;
	}

	public void setRetweetid(long retweetid) {
		this.retweetid = retweetid;
	}

	@Override
	public String toString() {
		return "Tweet [user=" + user + ", tweetid=" + tweetid + ", created="
				+ created + ", source=" + source + ", hashtags=" + hashtags
				+ ", mentions=" + mentions + ", text=" + text
				+ ", retweetCount=" + retweetCount + ", retweetid=" + retweetid
				+ ", place=" + place + ", media=" + media + ", contrib="
				+ contrib + ", reply=" + reply + "]";
	}

	@Override
	public int hashCode() {
		return Long.valueOf(tweetid).hashCode();
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
}
