package isistan.twitterapi;

import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.Cell;
import isistan.def.utils.table.Col;
import isistan.def.utils.table.Row;
import isistan.def.utils.table.Table;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import utils.textcat.TextCategorizer;

public class LangFiltering {

	public static void main(String[] args) throws Exception {
		final String baseDir = args[0];// "D:/TwitterCrawlData/crawl/";
		Scanner userList = new Scanner(new File(args[1]));
		final BufferedWriter output = new BufferedWriter(new FileWriter(
				new File("langFiltered.txt")));

		ExecutorService exec = Executors.newFixedThreadPool(10);

		int count = 0;
		while (userList.hasNext()) {

			final int user = userList.nextInt();
			System.out
					.println("Processing user " + user + " number " + count++);

			exec.execute(new Runnable() {

				@Override
				public void run() {
					File info = new File(baseDir + "/" + user + "/" + user
							+ "-info.dat");
					try {
						if (getLang(info).equals("en")
						// &&
						) {
							try {
								if (getLangFromTweets(baseDir, user))
									writeToOutput(output, user);
							} catch (Exception e) {
								writeToOutput(output, user);
								System.out.println("Problem reading " + user
										+ " tweets, adding, just in case.");
								e.printStackTrace();
							}
						}

					} catch (Exception e) {
						System.out
								.println("Could not obtain language from file "
										+ info);
					}

				}
			});

		}

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		userList.close();
		output.close();
	}

	private static synchronized void writeToOutput(BufferedWriter output,
			int user) throws IOException {
		output.write(user + "\n");
	}

	private static boolean getLangFromTweets(String baseDir, int user)
			throws Exception {
		String tweetFile = baseDir + "/" + user + "/" + user + "-TWEETS.dat";
		File tweetF = new File(tweetFile);
		if (!tweetF.exists())
			return true;

		CSVBuilder builder = new CSVBuilder(tweetF);
		builder.setExpectedFields(24);
		builder.setReplaceString("GeoLocation;@", "");
		builder.setMaxLines(100);
		Table t = builder.toTable();
		StringBuilder b = new StringBuilder();
		boolean head = true;
		for (Cell c : t.getCol("TEXT")) {
			if (head) {
				head = false;
			} else {
				b.append(c.value() + ". ");
			}
		}

		TextCategorizer guesser = new TextCategorizer();
		String[] cat = guesser.categorize(b.toString());
		if (cat[0].equals("english")
		// || cat[1].equals("english")
		// || cat[2].equals("english")
		)
			return true;
		return false;
	}

	public static String getLang(File info) throws Exception {
		Table t = Table.readCSV(info, ";", ",");
		if (!t.isEmpty()) {
			Row row = t.getRow(1);
			while (t.getRowLimit() >= 3) {
				row.add(t.getRow(2));
				t.delRow(2);
			}
			Col langCol = t.getCol("LANG");
			Cell val = langCol.get(1);
			if (val == null)
				throw new Exception("No Lang Detected");
			else {
				while (val != null && val.value().length() > 2) {
					t.merge(langCol.column() - 1, 1, langCol.column(), 1);
					langCol = t.getCol("LANG");
					val = langCol.get(1);
				}
			}
			return val.value();
		} else
			throw new Exception("File is empty");
	}
}
