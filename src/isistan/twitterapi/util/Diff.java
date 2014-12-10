package isistan.twitterapi.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

public class Diff {

	public static void main(String[] args) throws IOException {
		HashSet<Long> secondSet = new HashSet<Long>();

		BufferedWriter diffWriter = new BufferedWriter(new FileWriter(
				"diff.txt"));
		Scanner second = new Scanner(new File(args[1]));

		while (second.hasNext())
			secondSet.add(second.nextLong());

		Scanner first = new Scanner(new File(args[0]));

		while (first.hasNext()) {
			long long1 = first.nextLong();
			if (!secondSet.contains(long1))
				diffWriter.append(long1 + "\n");
		}
		first.close();
		second.close();
		diffWriter.close();
	}
}
