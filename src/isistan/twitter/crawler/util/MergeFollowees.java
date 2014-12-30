package isistan.twitter.crawler.util;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.status.UserStatus;
import isistan.twitter.crawler.store.bigtext.BigTextStore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class MergeFollowees {
	public static void main(String[] args) throws Exception {

		// HashSet<Long> fol = new HashSet<>();

		String inputList = args[0];
		String crawlFolder = args[1];
		BigTextStore bt = new BigTextStore(new File(crawlFolder));
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				args[2])));// "followees.txt"
		Scanner input = new Scanner(new File(inputList));
		int count = 0;
		while (input.hasNext()) {

			long user = input.nextLong();
			System.out.println("Generating followee list for user " + user
					+ " position " + count++);
			try {
				
				UserStatus stat = bt.getUserStatus(user);
				if (!stat.isDisabled()) {
					for (long followee : bt.getAdjacency(user,
							ListType.FOLLOWEES))
						writer.append(followee + "\n");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		writer.close();
		input.close();
	}
}
