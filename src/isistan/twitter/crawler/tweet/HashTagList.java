package isistan.twitter.crawler.tweet;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import edu.jlime.util.ByteBuffer;

public class HashTagList {
	public class HashTag {
		String hashtag;
		int from;
		int to;

		public HashTag(String hashtag, int from, int to) {
			this.hashtag = hashtag;
			this.from = from;
			this.to = to;
		}

	}

	List<HashTag> hashtags = new ArrayList<>();

	public void addNew(String text, int start, int end) {
		hashtags.add(new HashTag(text, start, end));
	}

	public void writeTo(ByteBuffer buffer) {
		buffer.put((byte) hashtags.size());
		for (HashTag hashTag : hashtags) {
			buffer.putString(hashTag.hashtag);
			buffer.putInt(hashTag.from);
			buffer.putInt(hashTag.to);
		}
	}

	public HashTagList readFrom(ByteBuffer buffer) {
		byte b = buffer.get();
		for (int i = 0; i < b; i++) {
			addNew(buffer.getString(), buffer.getInt(), buffer.getInt());
		}
		return this;
	}

	@Override
	public String toString() {
		JsonArray ret = new JsonArray();
		for (HashTag hashTag : hashtags) {
			JsonObject obj = new JsonObject();
			obj.addProperty("hashtag", hashTag.hashtag);
			obj.addProperty("start", hashTag.from);
			obj.addProperty("end", hashTag.to);
			ret.add(obj);
		}
		return ret.toString();
	}
}
