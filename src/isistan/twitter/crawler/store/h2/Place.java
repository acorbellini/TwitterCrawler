package isistan.twitter.crawler.store.h2;

import java.io.Serializable;

public class Place implements Serializable {

	public String place;
	public String countryFull;
	public String country;

	public Place(String country, String countryFull, String name) {
		this.country = country;
		this.countryFull = countryFull;
		this.place = name;
	}

	@Override
	public String toString() {
		return "Place [place=" + place + ", countryFull=" + countryFull
				+ ", country=" + country + "]";
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

}
