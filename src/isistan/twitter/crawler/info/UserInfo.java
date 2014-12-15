package isistan.twitter.crawler.info;

import java.sql.Date;

public class UserInfo {
	public long uid;
	public String scn;
	public String name;
	public String desc;
	public String lang;
	public Date date;
	public String loc;
	public String timezone;
	public int utc;
	public int followees;
	public int followers;
	public int favs;
	public int listed;
	public int tweets;
	public boolean verified;
	public boolean protectd;

	public static String getTitle() {
		return "ID;" + "SCREEN_NAME;" + "NAME;" + "DESCRIPTION;" + "LANG;"
				+ "CREATED;" + "LOCATION;" + "TIMEZONE;" + "UTC;"
				+ "FOLLOWEESCOUNT;" + "FOLLOWERSCOUNT;" + "FAVCOUNT;"
				+ "LISTEDCOUNT;" + "TWEETSCOUNT;" + "ISVERIFIED;"
				+ "ISPROTECTED";
	}

	public UserInfo(long uid, String scn, String name, String desc,
			String lang, Date date, String loc, String timezone, int utc,
			int followees, int followers, int favs, int listed, int tweets,
			boolean verified, boolean protectd) {
		this.uid = uid;
		this.scn = scn;
		this.name = name;
		this.desc = desc;
		this.lang = lang;
		this.date = date;
		this.loc = loc;
		this.timezone = timezone;
		this.utc = utc;
		this.followees = followees;
		this.followers = followers;
		this.favs = favs;
		this.listed = listed;
		this.tweets = tweets;
		this.verified = verified;
		this.protectd = protectd;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserInfo other = (UserInfo) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (desc == null) {
			if (other.desc != null)
				return false;
		} else if (!desc.equals(other.desc))
			return false;
		if (favs != other.favs)
			return false;
		if (followees != other.followees)
			return false;
		if (followers != other.followers)
			return false;
		if (lang == null) {
			if (other.lang != null)
				return false;
		} else if (!lang.equals(other.lang))
			return false;
		if (listed != other.listed)
			return false;
		if (loc == null) {
			if (other.loc != null)
				return false;
		} else if (!loc.equals(other.loc))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (protectd != other.protectd)
			return false;
		if (scn == null) {
			if (other.scn != null)
				return false;
		} else if (!scn.equals(other.scn))
			return false;
		if (timezone == null) {
			if (other.timezone != null)
				return false;
		} else if (!timezone.equals(other.timezone))
			return false;
		if (tweets != other.tweets)
			return false;
		if (uid != other.uid)
			return false;
		if (utc != other.utc)
			return false;
		if (verified != other.verified)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((desc == null) ? 0 : desc.hashCode());
		result = prime * result + favs;
		result = prime * result + followees;
		result = prime * result + followers;
		result = prime * result + ((lang == null) ? 0 : lang.hashCode());
		result = prime * result + listed;
		result = prime * result + ((loc == null) ? 0 : loc.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (protectd ? 1231 : 1237);
		result = prime * result + ((scn == null) ? 0 : scn.hashCode());
		result = prime * result
				+ ((timezone == null) ? 0 : timezone.hashCode());
		result = prime * result + tweets;
		result = prime * result + (int) (uid ^ (uid >>> 32));
		result = prime * result + utc;
		result = prime * result + (verified ? 1231 : 1237);
		return result;
	}

	@Override
	public String toString() {
		return "UserInfo [uid=" + uid + ", scn=" + scn + ", name=" + name
				+ ", desc=" + desc + ", lang=" + lang + ", date=" + date
				+ ", loc=" + loc + ", timezone=" + timezone + ", utc=" + utc
				+ ", followees=" + followees + ", followers=" + followers
				+ ", favs=" + favs + ", listed=" + listed + ", tweets="
				+ tweets + ", verified=" + verified + ", protectd=" + protectd
				+ "]";
	}

}