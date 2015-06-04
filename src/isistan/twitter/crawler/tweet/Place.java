package isistan.twitter.crawler.tweet;

import java.io.Serializable;

import edu.jlime.util.ByteBuffer;

public class Place implements Serializable {

	public String place = "";
	public String countryFull = "";
	public String country = "";

	public Place(String country, String countryFull, String name) {
		this.country = country;
		this.countryFull = countryFull;
		this.place = name;
	}

	public Place() {
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Place other = (Place) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (countryFull == null) {
			if (other.countryFull != null)
				return false;
		} else if (!countryFull.equals(other.countryFull))
			return false;
		if (place == null) {
			if (other.place != null)
				return false;
		} else if (!place.equals(other.place))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[place=" + place + ", countryFull=" + countryFull
				+ ", country=" + country + "]";
	}

	public void writeTo(ByteBuffer buffer) {
		if (place.isEmpty())
			buffer.put((byte) 0);
		else {
			buffer.put((byte) 1);
			buffer.putString(place);
			buffer.putString(country);
			buffer.putString(countryFull);
		}
	}

	public Place readFrom(ByteBuffer buffer) {
		byte type = buffer.get();
		if (type == 1) {
			this.place = buffer.getString();
			this.country = buffer.getString();
			this.countryFull = buffer.getString();
		}
		return this;
	}
}
