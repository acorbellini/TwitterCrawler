package isistan.twitter.crawler.util;

import isistan.twitter.crawler.store.bigtext.BigTextStore;

import java.io.File;
import java.util.Arrays;

public class GetUserList {
	public static void main(String[] args) throws Exception {
		BigTextStore store = new BigTextStore(new File(args[0]));
		System.out.println(store.getUserInfo(12l));
		System.out.println(store.getUserStatus0(12l));
		// System.out.println(store.getTweets(12l, TweetType.TWEETS));
		// System.out.println(store.getTweets(12l, TweetType.FAVORITES));
		// System.out.println(Arrays.toString(store.getAdjacency(12l,
		// ListType.FOLLOWEES)));
		// System.out.println(Arrays.toString(store.getAdjacency(12l,
		// ListType.FOLLOWERS)));
		long[] users = store.getUserList();
		Arrays.sort(users);
		System.out.println(users.length);
	}
}
