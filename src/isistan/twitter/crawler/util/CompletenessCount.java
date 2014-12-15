package isistan.twitter.crawler.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class CompletenessCount {

	public static class Count {
		List<Integer> followeesCrawled = new ArrayList<>();
		List<Integer> followersCrawled = new ArrayList<>();
		List<Integer> followeesMissing = new ArrayList<>();
		List<Integer> followersMissing = new ArrayList<>();
		public int tweets;
	}

	private static String listToString(List<Integer> followersCrawled) {
		if (followersCrawled.isEmpty())
			return "";
		StringBuilder builder = new StringBuilder();
		for (Integer integer : followersCrawled) {
			builder.append(",");
			builder.append(integer);
		}
		return builder.substring(1);
	}

	public static void main(String[] args) throws IOException {
		// BufferedWriter followeesWriter = new BufferedWriter(new FileWriter(
		// new File("followees.txt")));
//		HashSet<Long> followeesSet = new HashSet<Long>();
		// HashSet<Long> missingFolloweesSet = new HashSet<Long>();
		BufferedWriter missingFollowees = new BufferedWriter(new FileWriter(
				new File("missingFollowees.txt")));

		// BufferedWriter missingFollowers = new BufferedWriter(new FileWriter(
		// new File("missingFollowers.txt")));

		// HashMap<Integer, Count> count = new HashMap<>();
		String baseFile = args[0];
		String currentCrawled = args[1];

		HashSet<Long> existing = new HashSet<Long>();
		Scanner existingScanner = new Scanner(new File(currentCrawled));
		while (existingScanner.hasNext()) {
			existing.add(existingScanner.nextLong());
		}
		existingScanner.close();
		String crawlBase = args[2];
		String crawlDataFolder = crawlBase + "/crawl";
		Scanner scannerBaseUsers = new Scanner(new File(baseFile));
		while (scannerBaseUsers.hasNext()) {
			long user = scannerBaseUsers.nextLong();

			File status = new File(crawlBase + "/crawl-status/" + user
					+ ".prop");
			boolean escaped = true;
			Properties prop = new Properties();
			try {
				prop.load(new FileInputStream(status));
				if (prop.getProperty("IS_ESCAPED") == null
						&& prop.getProperty("IS_SUSPENDED") == null
						&& prop.getProperty("IS_PROTECTED") == null)
					escaped = false;
			} catch (Exception e) {
				System.out.println("File " + status + " not found.");
			}

			if (escaped)
				System.out.println("User " + user + " was escaped. ");
			else {

				// Count countUser = new Count();
				try {

					File followees = new File(crawlDataFolder + "/" + user
							+ "/" + user + "-FOLLOWEES.dat");

					// File followers = new File(crawlDataFolder + "/" + user
					// + "/" + user + "-FOLLOWERS.dat");
					//
					// File tweets = new File(crawlDataFolder + "/" + user + "/"
					// + user + "-TWEETS.dat");

					// BufferedReader reader = new BufferedReader(new
					// FileReader(
					// tweets));
					// while (reader.ready()) {
					// reader.readLine();
					// // countUser.tweets++;
					// }
					// reader.close();

					Scanner followeesScanner = new Scanner(followees);
					while (followeesScanner.hasNext()) {
						long f = followeesScanner.nextLong();
						// followeesSet.add(f);
						if (!existing.contains(f)) {
							if(f<12 || f > 10000000000l)
								System.out.println("Found " + f + " in " + followees);
							missingFollowees.write(f + "\n");
							// missingFolloweesSet.add(f);
						}

						// countUser.followeesCrawled.add(f);
						// } else {
						// countUser.followeesMissing.add(f);
						// missingFollowees.write(f + "\n");
						// }
					}
					followeesScanner.close();

					// Scanner followersScanner = new Scanner(followers);
					// while (followersScanner.hasNext()) {
					// int f = followersScanner.nextInt();
					// if (existing.contains(f)) {
					// countUser.followersCrawled.add(f);
					// } else {
					// countUser.followersMissing.add(f);
					// // missingFollowers.write(f + '\n');
					// }
					// }
					// followersScanner.close();
					// count.put(user, countUser);

				} catch (Exception e) {
					System.out.println(user + " has a protected profile.");
				}
			}
		}

		// for (Long f : missingFolloweesSet)
		// missingFollowees.write(f + "\n");
		//
		// for (Long long1 : followeesSet) {
		// followeesWriter.append(long1 + "\n");
		// }

		// BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
		// "BaseUsersCount.csv")));

		// String smalltitle = "USER;COMPLETE;FOLLOWEES;NUMTWEETS" + "\n";
		// writer.write(smalltitle);
		// for (Entry<Integer, Count> e : count.entrySet()) {
		// Count c = e.getValue();
		//
		// int neighbourhoodSize = (c.followeesCrawled.size()
		// + c.followersCrawled.size() + c.followeesMissing.size() +
		// c.followersMissing
		// .size());
		// int followeesSize = c.followeesCrawled.size()
		// + c.followeesMissing.size();
		// if (followeesSize <= 10)
		// System.out.println("Followees crawled is less than 10.");
		// else {
		// float missingPercentage = (c.followeesMissing.size() +
		// c.followersMissing
		// .size()) / (float) neighbourhoodSize;
		// String smallline = e.getKey() + ";"
		// + String.format("%.2f", missingPercentage) + ";"
		// + followeesSize + ";" + c.tweets;
		// writer.write(smallline + "\n");
		// }
		// }
		// writer.close();
		missingFollowees.close();
		// followeesWriter.close();
		// missingFollowers.close();
		scannerBaseUsers.close();
	}
}
