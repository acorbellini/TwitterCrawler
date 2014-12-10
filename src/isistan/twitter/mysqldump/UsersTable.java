package isistan.twitter.mysqldump;

import java.util.HashMap;

public class UsersTable {
	public static class User {
		long id;
		String info;
		String tweets;
		String favs;
		String followees;
		String followers;
	}

	private static HashMap<Long, User> table = new HashMap<>();

	public static User getUser(long id) {
		User u = table.get(id);
		if (u == null) {
			synchronized (table) {
				u = table.get(id);
				if (u == null) {
					u = new User();
					table.put(id, u);
				}
			}
		}
		return u;
	}

	public static void save(String string) {
		// TODO Auto-generated method stub

	}

}
