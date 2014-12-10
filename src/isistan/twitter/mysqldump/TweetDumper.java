package isistan.twitter.mysqldump;

import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class TweetDumper {
	private String type;
	private String of;
	private CrawlFolder crawl;

	Semaphore sem = new Semaphore(100);

	public TweetDumper(String outputFolder, CrawlFolder crawl, String type) {
		this.of = outputFolder;
		this.crawl = crawl;
		this.type = type;
	}

	private static final int MAX_TWEETS_PER_DUMP = 50000000;

	public void start() throws IOException {
		int currentSize = 0;
		int count = 1;
		int userCount = 0;
		TweetFileDumper dumper = new TweetFileDumper(this, of, type, count++);
		for (UserFolder u : crawl) {
			userCount++;
			if (userCount % 1000 == 0) {
				System.out.println("Processed 1000 users, count : " + userCount
						+ ", current tweets :" + currentSize);
			}
			// System.out.println("Processing user " + u.getUser());
			int tweetSize = getSize(type, u);
			if (currentSize + tweetSize > MAX_TWEETS_PER_DUMP) {
				currentSize = 0;
				System.out.println("Creating new dump file, because current "
						+ type + " file exceded max.");
				dumper.shutdown();
				try {
					sem.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				dumper = new TweetFileDumper(this, of, type, count++);
			}

			dumper.addUser(u);
			currentSize += tweetSize;
		}
		dumper.shutdown();

	}

	private int getSize(String type2, UserFolder u) throws IOException {
		if (type.equals("tweets"))
			return u.getTweetSize();
		else if (type.equals("favorites"))
			return u.getFavsSize();
		else if (type.equals("tweets-processed"))
			u.getTweetsProcessed();
		return 0;
	}

	public void dumperFinished() {
		sem.release();
	}
}
