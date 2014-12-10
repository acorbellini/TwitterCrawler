package isistan.twitter.mysqldump;

import isistan.twitter.mysql.Counter;

import java.util.HashMap;

public class TableSizes {
	static HashMap<String, Counter> counts = new HashMap<>();

	public static Counter getSize(String tableName) {
		Counter u = counts.get(tableName);
		if (u == null) {
			synchronized (counts) {
				u = counts.get(tableName);
				if (u == null) {
					u = new Counter(0);
					counts.put(tableName, u);
				}
			}
		}
		return u;
	}

	public static synchronized String reserveTable(String prefix, int size,
			int max_size) {
		int count = 1;
		while (true) {
			String ret = prefix + "-" + count;
			Counter c = TableSizes.getSize(prefix + "-" + count);
			if (c.getCont() + size <= max_size) {
				c.incCont(size);
				return ret;
			} else
				count++;
		}
	}

}
