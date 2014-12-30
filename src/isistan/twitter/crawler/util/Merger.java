package isistan.twitter.crawler.util;

import java.io.IOException;

import isistan.twitter.crawler.store.bigtext.BigTextStore;

public class Merger {
	public static void main(String[] args) throws IOException, Exception {
		BigTextStore.merge(args[0], args[1]);
	}
}
