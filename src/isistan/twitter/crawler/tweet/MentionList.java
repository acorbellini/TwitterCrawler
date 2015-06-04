package isistan.twitter.crawler.tweet;

import isistan.twitter.crawler.tweet.HashTagList.HashTag;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import edu.jlime.util.ByteBuffer;

public class MentionList {
	public static class Mention {

		private long id;
		private String scn;
		private String name;
		private int start;
		private int end;

		public Mention(long id, String screenName, String name, int start,
				int end) {
			this.id = id;
			this.scn = screenName;
			this.name = name;
			this.start = start;
			this.end = end;
		}

	}

	List<Mention> list = new ArrayList<>();

	public void addNew(long id, String screenName, String name, int start,
			int end) {
		list.add(new Mention(id, screenName, name, start, end));
	}

	@Override
	public String toString() {
		JsonArray ret = new JsonArray();
		for (Mention mention : list) {
			JsonObject obj = new JsonObject();
			obj.addProperty("id", mention.id);
			obj.addProperty("scn", mention.scn);
			obj.addProperty("name", mention.name);
			obj.addProperty("start", mention.start);
			obj.addProperty("end", mention.end);
			ret.add(obj);
		}
		return ret.toString();
	}

	public MentionList readFrom(ByteBuffer buffer) {
		byte b = buffer.get();
		for (int i = 0; i < b; i++) {
			addNew(buffer.getLong(), buffer.getString(), buffer.getString(),
					buffer.getInt(), buffer.getInt());
		}
		return this;
	}

	public void writeTo(ByteBuffer buffer) {
		buffer.put((byte) list.size());
		for (Mention m : list) {
			buffer.putLong(m.id);
			buffer.putString(m.scn);
			buffer.putString(m.name);
			buffer.putInt(m.start);
			buffer.putInt(m.end);
		}
	}
}
