package isistan.twitter.crawler.tweet;

import isistan.twitter.crawler.tweet.MentionList.Mention;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import edu.jlime.util.ByteBuffer;

public class MediaList {
	public static class Media {

		private long id;
		private String type;
		private String url;
		private int start;
		private int end;

		public Media(long id, String type, String url, int start, int end) {
			this.id = id;
			this.type = type;
			this.url = url;
			this.start = start;
			this.end = end;
		}

	}

	List<Media> list = new ArrayList<>();

	public void addNew(long id, String type, String url, int start,
			int end) {
		list.add(new Media(id, type, url, start, end));
	}

	@Override
	public String toString() {
		JsonArray ret = new JsonArray();
		for (Media mention : list) {
			JsonObject obj = new JsonObject();
			obj.addProperty("id", mention.id);
			obj.addProperty("type", mention.type);
			obj.addProperty("url", mention.url);
			obj.addProperty("start", mention.start);
			obj.addProperty("end", mention.end);
			ret.add(obj);
		}
		return ret.toString();
	}

	public MediaList readFrom(ByteBuffer buffer) {
		byte b = buffer.get();
		for (int i = 0; i < b; i++) {
			addNew(buffer.getLong(), buffer.getString(), buffer.getString(),
					buffer.getInt(), buffer.getInt());
		}
		return this;
	}

	public void writeTo(ByteBuffer buffer) {
		buffer.put((byte) list.size());
		for (Media m : list) {
			buffer.putLong(m.id);
			buffer.putString(m.type);
			buffer.putString(m.url);
			buffer.putInt(m.start);
			buffer.putInt(m.end);
		}
	}
}
