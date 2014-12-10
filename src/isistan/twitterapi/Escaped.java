package isistan.twitterapi;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

public class Escaped {

	public static void main(String[] args) throws Exception {
		Scanner scanner = new Scanner(new File(args[0]));
		final String baseDir = args[1];
		while (scanner.hasNext()) {
			final int u = scanner.nextInt();
			File info = new File(baseDir + "/crawl-status/" + u + ".prop");
			Properties prop = new Properties();
			try {
				prop.load(new FileReader(info));
				if (prop.getProperty("IS_ESCAPED") != null) {
					System.out.println("Deleting " + info);
					info.delete();
				}
			} catch (Exception e) {
				// System.out.println("Could not read " + info);
			}

		}
	}
}
