package isistan.twitter.mysqldump;

import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;
import isistan.twitterapi.util.DataTypeUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class AdjacencyList {

	private String of;
	private CrawlFolder crawl;
	private BufferedOutputStream writer;

	public AdjacencyList(String outputFolder, CrawlFolder crawl)
			throws Exception {
		this.of = outputFolder;
		this.crawl = crawl;
		File outputFile = new File(of + "/adjacency.gzip");
		GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(
				outputFile));
		this.writer = new BufferedOutputStream(gos, 50 * 1024 * 1024);
	}

	public void start() throws Exception {
		for (UserFolder u : crawl) {
			writer.write(DataTypeUtils.longToByteArray(u.getUser()));
			File followees = u.getFollowees();
			File followers = u.getFollowers();
			try {
				saveList(u, followees);
			} catch (Exception e) {
				// TODO: handle exception
			}
			try {
				saveList(u, followers);
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
	}

	private void saveList(UserFolder u, File in) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(in));
		List<Long> list = new LinkedList<>();
		while (reader.ready()) {
			String f = reader.readLine();
			list.add(Long.valueOf(f));
		}

		writer.write(DataTypeUtils.intToByteArray(list.size()));
		for (Long long1 : list) {
			writer.write(DataTypeUtils.longToByteArray(long1));
		}
	}
}
