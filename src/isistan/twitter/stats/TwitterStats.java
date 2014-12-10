package isistan.twitter.stats;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import isistan.def.utils.DataTypeUtils;
import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.CSVReader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class TwitterStats {
	private String path;
	private BufferedWriter out;
	private BufferedWriter out2;

	public TwitterStats(String string, String out) throws IOException {
		this.path = string;
		this.out = new BufferedWriter(new FileWriter(new File(out)),
				50 * 1024 * 1024);
		this.out2 = new BufferedWriter(new FileWriter(new File(out + ".followers")),
				50 * 1024 * 1024);
	}

	public static void main(String[] args) throws Exception {
		new TwitterStats(args[0], args[1]).run();
	}

	private void run() throws Exception {
		// CrawlFolder folder = new CrawlFolder(path);
		// ExecutorService exec = Executors.newFixedThreadPool(30);
		// final Semaphore sem = new Semaphore(30);
		// int cont = 0;
		// for (final UserFolder userFolder : folder) {
		// sem.acquire();
		// System.out.println(cont++ + " Processing user "
		// + userFolder.getUser());
		// exec.execute(new Runnable() {
		// @Override
		// public void run() {
		// String towrite;
		// try {
		// towrite = userFolder.getUser() + " "
		// + userFolder.getFavsSize() + " "
		// + userFolder.getTweetSize() + "\n";
		// synchronized (TwitterStats.this) {
		// out.write(towrite);
		// }
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// sem.release();
		// }
		// });
		//
		// }
		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		// out.close();
		// ExecutorService exec = Executors.newFixedThreadPool(10);
		// final Semaphore sem = new Semaphore(10);
		// for (final File file : new File(path).listFiles()) {
		// sem.acquire();
		// exec.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		// if (file.getName().contains("favorites"))
		// try {
		// tweetsStats(file);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// sem.release();
		// }
		// });
		// if (file.getName().contains("favorites"))
		// tweetsStats(file);
		// if (file.getName().contains("info"))
		// loadInfo(file);
		// }
		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		// TIntIntIterator it = tweets.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// out.write(it.key() + " " + it.value() + "\n");
		// }
		// out.close();
		adjacencyStats(new File(path));
		TLongIntIterator it = followees.iterator();
		while (it.hasNext()) {
			it.advance();
			out.write(it.key() + " " + it.value() + "\n");
		}
		out.close();
		
		it = followers.iterator();
		while (it.hasNext()) {
			it.advance();
			out2.write(it.key() + " " + it.value()  + "\n");
		}
		out2.close();
	}

	TLongIntHashMap followees = new TLongIntHashMap();

	TLongIntHashMap followers = new TLongIntHashMap();

	TIntIntHashMap tweets = new TIntIntHashMap();

	private void tweetsStats(File file) throws IOException {
		TIntIntHashMap internal = new TIntIntHashMap();
		CSVReader reader = new CSVReader(openGZip(file), false, 5 * 1024 * 1024);
		int count = 0;
		while (reader.ready()) {
			if (count % 100000 == 0)
				System.out.println("counted " + count + " file "
						+ file.getName());
			count++;
			String[] split = CSVBuilder.split(reader.readLine()
					.replace("'", ""), ";");
			String userid = split[0];
			internal.adjustOrPutValue(Integer.valueOf(userid), 1, 1);
		}
		synchronized (this) {
			TIntIntIterator it = internal.iterator();
			while (it.hasNext()) {
				it.advance();
				tweets.adjustOrPutValue(it.key(), it.value(), it.value());
			}
		}
		System.out.println(count);
	}

	protected BufferedReader openGZip(File file) throws FileNotFoundException,
			IOException {
		GZIPInputStream gis = new GZIPInputStream(new FileInputStream(file),
				64 * 1024);
		return new BufferedReader(new InputStreamReader(gis, "UTF-8"));
	}

	private void adjacencyStats(File f) throws Exception {
		long u;
		int cont = 0;
		BufferedInputStream bis = new BufferedInputStream(
				new FileInputStream(f), 5 * 1024 * 1024);

		while ((u = readLong(bis)) != -1) {
			System.out.println((cont++) + ":" + u);
			int followeesSize = readInt(bis) / 8;

			for (int i = 0; i < followeesSize; i++) {
				readLong(bis);
				followees.adjustOrPutValue(u, 1, 1);
			}

			int followersSize = readInt(bis) / 8;
			for (int i = 0; i < followersSize; i++) {
				readLong(bis);
				followers.adjustOrPutValue(u, 1, 1);
			}
		}
	}

	private static int readInt(BufferedInputStream bis) throws IOException {
		byte[] l = new byte[4];
		l[0] = (byte) bis.read();
		l[1] = (byte) bis.read();
		l[2] = (byte) bis.read();
		l[3] = (byte) bis.read();
		return DataTypeUtils.byteArrayToInt(l);
	}

	private static long readLong(BufferedInputStream bis) throws IOException {
		byte[] l = new byte[8];
		l[0] = (byte) bis.read();
		if (l[0] == -1)
			return -1;
		l[1] = (byte) bis.read();
		l[2] = (byte) bis.read();
		l[3] = (byte) bis.read();
		l[4] = (byte) bis.read();
		l[5] = (byte) bis.read();
		l[6] = (byte) bis.read();
		l[7] = (byte) bis.read();
		return DataTypeUtils.byteArrayToLong(l, 0);
	}
}
