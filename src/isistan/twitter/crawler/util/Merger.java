package isistan.twitter.crawler.util;

import isistan.twitter.crawler.store.bigtext.TwitterStore;

import java.io.IOException;

public class Merger {
	public static void main(String[] args) throws IOException, Exception {
		TwitterStore.merge(args[0], args[1]);
	}
}
