package isistan.twitter.mysqldump;

import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class DumpGenerator {
	private String folder;
	private String ul;
	private String outputFolder;

	public DumpGenerator(String folder, String userList, String outputFolder)
			throws IOException {
		this.folder = folder;
		this.ul = userList;
		this.outputFolder = outputFolder;

	}

	public static void main(final String[] args) throws Exception {
		// System.in.read();
		DumpGenerator dg = new DumpGenerator(args[0], args[1], args[2]);
		dg.run();
	}

	private void run() throws Exception {
		Files.createDirectories(Paths.get(outputFolder));
		CrawlFolder crawl = new CrawlFolder(folder, ul);

		// InfoDumper info = new InfoDumper(outputFolder, crawl);
		// info.start();
		// TweetDumper tweet = new TweetDumper(outputFolder, crawl, "tweets");
		// tweet.start();
		// TweetDumper favs = new TweetDumper(outputFolder, crawl, "favorites");
		// favs.start();
		// TweetDumper proc = new TweetDumper(outputFolder, crawl,
		// "tweets-processed");
		// proc.start();

		AdjacencyList adjacency = new AdjacencyList(outputFolder, crawl);
		adjacency.start();

		// UsersTable.save(outputFolder + "/users.txt");
	}
}
