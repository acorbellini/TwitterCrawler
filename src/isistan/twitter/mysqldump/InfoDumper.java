package isistan.twitter.mysqldump;

import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.Cell;
import isistan.def.utils.table.Table;
import isistan.def.utils.table.ValueCell;
import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;
import isistan.twitter.mysql.PlainTextExporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class InfoDumper {
	Semaphore sem = new Semaphore(200);
	ExecutorService exec = Executors.newCachedThreadPool();
	private static final String SEP = ";";
	private CrawlFolder crawl;
	private BufferedWriter writer;

	public InfoDumper(String output, CrawlFolder crawl) throws IOException {
		File outputFile = new File(output + "/info.gzip");
		GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(
				outputFile));
		this.writer = new BufferedWriter(new OutputStreamWriter(gos, "UTF-8"),
				50 * 1024 * 1024);
		this.crawl = crawl;
	}

	public void dumpInfo(UserFolder uf) throws Exception {

		UsersTable.getUser(uf.getUser()).info = "USERINFO";
		File ifile = uf.getInfo();
		if (!ifile.exists())
			return;
		CSVBuilder builder = new CSVBuilder(ifile);

		StringBuilder built = new StringBuilder();

		Table t = builder.toTable();

		if (t.getRowLimit() == 0) {
			// System.out.println("User ID " + userID
			// + " has it's user info file empty.");
			return;
		}

		while (t.getRowLimit() > 2)
			t.mergeRows(1, 2);

		String lang = t.get(4, 1).value(); // Where LANG Should be
		while (!lang.matches("[a-z][a-z]")
				&& !lang.matches("[a-z][a-z]-[a-z][a-z]")
				&& !lang.matches("[a-z][a-z][a-z]")) {
			// Might not be a language
			t.merge(3, 1, 4, 1);// JOIN DESCRIPTION AND LANGUAGE
			lang = t.get(4, 1).value(); // Where LANG Should be
		}

		while (t.getRow(1).size() > 16) {
			t.merge(6, 1, 7, 1);// LOCATION AND TIMEZONE

		}

		while (t.getRow(1).size() < 16) {
			t.insCol(6);// LOCATION INSERTED
			t.set(6, 1, new ValueCell(""));
		}

		boolean first = true;
		for (int i = 0; i < 14; i++) {
			Cell c = t.get(i, 1);
			String val = "";
			if (c != null)
				val = c.value();
			if (first) {
				first = false;
				built.append(PlainTextExporter.escape(val, false));
			} else
				built.append(SEP + PlainTextExporter.escape(val, false));
		}

		try {
			built.append(SEP);
			built.append(Boolean.valueOf(t.get(14, 1).value()) ? "1" : "0");
			built.append(SEP);
			built.append(Boolean.valueOf(t.get(15, 1).value()) ? "1" : "0");
		} catch (Exception e) {
			e.printStackTrace();
		}
		synchronized (this) {
			writer.append(built.toString() + "\n");
		}
	}

	public void start() throws InterruptedException, IOException {

		int c = 0;
		for (final UserFolder uf : crawl) {
			// System.out.println("Saving info for user " + c++);
			sem.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						dumpInfo(uf);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}
			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		close();
	}

	public void close() throws IOException {
		writer.close();
	}
}
