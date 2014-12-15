package isistan.twitter.crawler.migration;

import gnu.trove.set.hash.TLongHashSet;
import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.store.bigtext.BigTextStore;
import isistan.twitter.crawler.tweet.Tweet;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.util.StoreUtil;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Validate {

	public static void main(String[] args) throws Exception {
		String userFile = null;
		if (args.length == 3)
			userFile = args[2];
		new Validate(args[0], args[1], userFile);
		// .run();
	}

	private CrawlFolder folder;

	private BigTextStore store;

	ExecutorService exec = Executors.newFixedThreadPool(50);

	Semaphore max = new Semaphore(100);

	public Validate(String folder, String db, String userFile) throws Exception {
		if (userFile == null)
			this.folder = new CrawlFolder(folder);
		else
			this.folder = new CrawlFolder(folder, userFile);
		// this.store = new MapDBStore(new File(db));
		this.store = new BigTextStore(new File(db));
		this.store.close();
	}

	private void run() throws InterruptedException {

		int cont = 0;
		int start = 1000000;
		long lastTime = System.currentTimeMillis();
		for (UserFolder userFolder : folder) {
			if (cont++ < start)
				continue;
			if (cont + 1 % 1000 == 0) {
				System.out.println("Current count " + cont + " Rate " + 1000
						/ ((System.currentTimeMillis() - lastTime) / 1000));
				lastTime = System.currentTimeMillis();
			}

			try {
				validateTweets(userFolder, TweetType.TWEETS);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("error validating tweets for user "
						+ userFolder.getUser() + " : " + e.getClass() + " "
						+ e.getMessage() + " " + e.getCause());
			}
			try {
				validateTweets(userFolder, TweetType.FAVORITES);
			} catch (Exception e) {
				System.out.println("error validating favorites for user "
						+ userFolder.getUser() + " : " + e.getClass() + " "
						+ e.getMessage() + " " + e.getCause());
			}
			try {
				validateInfo(userFolder);
			} catch (Exception e) {
				System.out.println("error validating userinfo for user "
						+ userFolder.getUser() + " : " + e.getClass() + " "
						+ e.getMessage() + " " + e.getCause());
			}
			try {
				validateAdj(userFolder, ListType.FOLLOWEES);
			} catch (Exception e) {
				System.out.println("error validating followees for user "
						+ userFolder.getUser() + " : " + e.getClass() + " "
						+ e.getMessage() + " " + e.getCause());
			}
			try {
				validateAdj(userFolder, ListType.FOLLOWERS);
			} catch (Exception e) {
				System.out.println("error validating followers for user "
						+ userFolder.getUser() + " : " + e.getClass() + " "
						+ e.getMessage() + " " + e.getCause());
			}

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	private void validateAdj(final UserFolder userFolder, final ListType type)
			throws Exception {
		max.acquire();
		exec.execute(new Runnable() {

			@Override
			public void run() {
				try {
					TLongHashSet list = null;
					try {
						list = new TLongHashSet(store.getAdjacency(
								userFolder.getUser(), type));
					} catch (Exception e) {
						e.printStackTrace();
					}
					TLongHashSet list2 = null;
					if (type.equals(ListType.FOLLOWEES))
						list2 = new TLongHashSet(StoreUtil.toList(userFolder
								.getFollowees()));
					else
						list2 = new TLongHashSet(StoreUtil.toList(userFolder
								.getFollowers()));
					if ((list == null ^ list2 == null)
							|| (list != null && list2 != null && !list
									.equals(list2))) {
						System.out.println(type + " don't match for user "
								+ userFolder.getUser());
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					max.release();
				}

			}
		});
	}

	private void validateInfo(final UserFolder userFolder) throws Exception {
		max.acquire();
		exec.execute(new Runnable() {

			@Override
			public void run() {
				try {
					UserInfo info = null;
					try {
						info = store.getUserInfo(userFolder.getUser());
					} catch (Exception e) {
						e.printStackTrace();
					}
					UserInfo info2 = null;
					try {
						info2 = StoreUtil.toInfo(userFolder.getInfo());
					} catch (Exception e) {
						System.out.println("Error processing "
								+ userFolder.getUser());
						e.printStackTrace();
					}
					if ((info != null && info2 == null)
							|| (info == null && info2 != null)
							|| (info != null && info2 != null && !info
									.equals(info2))) {
						synchronized (Validate.this) {
							System.out.println("Infos don't match for user "
									+ userFolder.getUser());
							System.out.println(info);
							System.out.println(info2);
						}

					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					max.release();
				}
			}
		});
	}

	private void validateTweets(final UserFolder userFolder,
			final TweetType type) throws Exception {
		max.acquire();
		exec.execute(new Runnable() {

			@Override
			public void run() {
				try {
					HashMap<Long, Tweet> tweets = new HashMap<>();
					List<Tweet> tweetList = store.getTweets(
							userFolder.getUser(), type);
					for (Tweet tweet : tweetList) {
						tweets.put(tweet.tweetid, tweet);
					}
					File tFile = null;
					if (type.equals(TweetType.TWEETS))
						tFile = userFolder.getTweets();
					else
						tFile = userFolder.getFavs();
					Set<Tweet> tweetsInFile = StoreUtil.toTweet(
							userFolder.getUser(), tFile);
					if (tweets.size() != tweetsInFile.size())
						System.out.println("Missing " + type + " "
								+ (tweetsInFile.size() - tweets.size()));
					for (Tweet tweet : tweetsInFile) {
						Tweet t = tweets.get(tweet.tweetid);
						if (t == null) {
							System.out.println("ERROR. Tweet " + type + " id "
									+ tweet.tweetid + " NOT FOUND.");
						} else if (!t.fullEquals(tweet)) {
							synchronized (Validate.this) {
								System.out.println("ERROR. Tweet " + type
										+ " id " + tweet.tweetid
										+ " does not match.");
								System.out.println(tweet);
								System.out.println(t);
							}
						}
					}
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					max.release();
				}
			}
		});
	}

}
