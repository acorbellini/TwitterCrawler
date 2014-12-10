package isistan.twitterapi.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class CreateFilteredList {
	public static void main(String[] args) throws IOException {
		Scanner input = new Scanner(new File(args[0]));
		String crawl = args[1];
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				"filtered.txt")));
		while (input.hasNext()) {
			long user = input.nextLong();
			try {
				Properties p = new Properties();
				p.load(new FileReader(new File(crawl + "/crawl-status/" + user
						+ ".prop")));
				if (p.getProperty("IS_ESCAPED") == null
						&& p.getProperty("IS_SUSPENDED") == null
						&& p.getProperty("IS_PROTECTED") == null) {
					writer.append(user + "\n");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}

		}
		writer.close();
	}
}
