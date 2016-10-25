package isistan.bigtextformat.text;

import java.io.File;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.store.bigtext.TwitterStore;

public class NoRecordTest {
	public static void main(String[] args) throws Exception {
		// for (int i = 0; i < 10000; i++) {
		{
			TwitterStore store = new TwitterStore(new File(args[0]));
			try {
				long[] adj = store.getAdjacency(10165452l, ListType.FOLLOWEES);
				if (adj == null)
					System.out.println("No records");
				else
					System.out.println("Found "  + adj.length);
			} finally {
				store.close();
			}
		}
		{
			TwitterStore store = new TwitterStore(new File(args[1]));
			try {
				long[] adj = store.getAdjacency(10165452l, ListType.FOLLOWEES);
				if (adj == null)
					System.out.println("No records");
				else
					System.out.println("Found "+ adj.length);
			} finally {
				store.close();
			}
		}

		// }

	}
}
