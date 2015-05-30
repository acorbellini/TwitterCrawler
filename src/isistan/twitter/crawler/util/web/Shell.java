package isistan.twitter.crawler.util.web;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.tweet.TweetType;

public class Shell {

	private static GroovyShell shell;

	public static GroovyShell getShell() {
		return shell;
	}

	public static void init(TwitterStore store) throws Exception {
		Binding binding = new Binding();
		binding.setVariable("store", store);
		binding.setVariable("TweetType", TweetType.class);
		binding.setVariable("ListType", ListType.class);
		shell = new GroovyShell(binding);
	}
}
