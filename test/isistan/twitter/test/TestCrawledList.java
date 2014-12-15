package isistan.twitter.test;

import java.util.TreeSet;

public class TestCrawledList {
	public static void main(String[] args) {
		TestCrawledList test = new TestCrawledList();
		test.addLatest(1l);
		test.addLatest(2l);
		test.addLatest(3l);
		test.addLatest(4l);
		test.addLatest(0l);
		test.addLatest(11l);
		test.addLatest(10l);
		test.addLatest(5l);
		test.addLatest(7l);
		test.addLatest(9l);
		test.addLatest(8l);
		test.addLatest(6l);

		System.out.println(test.crawled);
	}

	// LinkedList<Long> crawled = new LinkedList<>();
	private TreeSet<Long> crawled = new TreeSet<Long>();

	public TestCrawledList() {
		crawled.add(-1l);
	}

	private void addLatest(Long user) {
		long latest = user;
		crawled.add(user);
		long next = crawled.first() + 1;
		while (crawled.size() > 1 && crawled.contains(next)) {
			crawled.remove(crawled.first());
			latest = next;
			next++;
		}
		// updateProperty("LatestCrawled", latest + "", userProp,
		// userPropertyFile);
	}
}
