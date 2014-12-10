package isistan.twitter.compressiontest;

import isistan.twitter.crawler.TweetType;
import isistan.twitter.crawler.store.h2.H2Mode;
import isistan.twitter.crawler.store.h2.H2Store;
import isistan.twitter.crawler.store.h2.Tweet;

import java.io.File;
import java.util.List;
import java.util.Scanner;

public class CompressionTest {
	public static void main(String[] args) throws IllegalAccessException,
			Exception {
		H2Store store = null;
		Scanner scan = null;
		try {
			store = new H2Store(new File(
					"C:/Users/acorbellini/Desktop/crawlerPrueba"),
					H2Mode.READ_ONLY);
			scan = new Scanner(
					new File(
							"C:/Users/acorbellini/Desktop/Split 3rd Layer/Restantes/3rdLayer-SplitPart-ap"));
			while (scan.hasNext()) {
				long nextLong = scan.nextLong();
				List<Tweet> t = store.getTweets(nextLong,
						TweetType.TWEETS);
				// DEFByteBuffer buff = new DEFByteBuffer();
				System.out.println(nextLong);
				for (Tweet tweet : t) {
					System.out.println(tweet);
					// buff.putByteArray(StoreUtil.tweetToByteArray(tweet));
				}

				// byte[] build = buff.build();
				// System.out.println(StoreUtil.lz4compress(build).length);
				// System.out.println(StoreUtil.gzipcompress(build).length);
				// System.out.println(StoreUtil.bzipcompress(build).length);
				// System.out.println(StoreUtil.xzcompress(build).length);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			store.close();
			scan.close();
		}

	}
}
