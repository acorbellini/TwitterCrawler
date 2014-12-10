package isistan.twitterapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Scanner;

public class MergeFollowees {
	public static void main(String[] args) throws IOException {

		// HashSet<Long> fol = new HashSet<>();

		String inputList = args[0];
		String crawlFolder = args[1];
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				"followees.txt")));
		Scanner input = new Scanner(new File(inputList));
		int count = 0;
		while (input.hasNext()) {

			long user = input.nextLong();
			System.out.println("Generating followee list for user " + user
					+ " position " + count++);
			try {
				String prop = crawlFolder + "/crawl-status/" + user + ".prop";
				Properties p = new Properties();
				p.load(new FileInputStream(new File(prop)));
				if (p.getProperty("IS_ESCAPED") == null
						&& p.getProperty("IS_SUSPENDED") == null
						&& p.getProperty("IS_PROTECTED") == null) {
					String file = crawlFolder + "/crawl/" + user + "/" + user
							+ "-FOLLOWEES.dat";
					Scanner scanner = new Scanner(new File(file));
					while (scanner.hasNext()) {
						long followee = scanner.nextLong();
						// fol.add(followee);
						writer.append(followee + "\n");
					}
					scanner.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}
}
