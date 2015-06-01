package isistan.twitter.crawler.migration;

import isistan.twitter.crawler.adjacency.ListType;
import isistan.twitter.crawler.folder.CrawlFolder;
import isistan.twitter.crawler.folder.UserFolder;
import isistan.twitter.crawler.info.UserInfo;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.tweet.TweetType;
import isistan.twitter.crawler.util.StoreUtil;
import isistan.twitter.crawler.util.TwitterStoreShell;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;

public class Migrator {
	public static void main(String[] args) throws Exception {
		new Migrator(args[0], args[1], args.length == 3 ? args[2] : null).run();
	}

	private static final int MAX_THREADS = 50;
	// private static final int MAX_SUB_THREADS = 50;

	private static final int COMPACT_SIZE = 100000;
	// ExecutorService TweetsExecutor = createExec("Tweet");
	// ExecutorService FavsExecutor = createExec("Favs");
	// ExecutorService FolloweesExecutor = createExec("Followees");
	// ExecutorService FollowersExecutor = createExec("Followers");
	// ExecutorService InfoExecutor = createExec("Info");

	// ExecutorService StatusExecutor = createExec("Status");

	long plainSizeOnDisk = 0;
	long followeeSize = 0;
	long followerSize = 0;

	TwitterStore store;
	CrawlFolder folder;

	private File statusDir;
	private Properties prop;
	private File propFile;

	Long lastUser = null;
	int cont = 0;
	List<Long> migrated = new ArrayList<>();
	HashSet<Long> done = new HashSet<>();
	boolean skipChecking = true;
	private TwitterStoreShell shell;

	public Migrator(String folder, String db, String userList) throws Exception {
		// System.in.read();
		if (userList != null)
			this.folder = new CrawlFolder(folder, userList);
		else
			this.folder = new CrawlFolder(folder);
		// this.store = new MapDBStore(new File(db));
		this.store = new TwitterStore(new File(db), false); // WARNING, sync is
															// false, crashes
															// might lose data.
		this.shell = new TwitterStoreShell(store, 8080);
		shell.start();
		this.statusDir = new File(folder + "/crawl-status");
		this.prop = new Properties();
		propFile = new File(db + "/" + "lastMigrated.prop");
		if (!propFile.exists())
			propFile.createNewFile();
		else {
			prop.load(new FileReader(propFile));
			if (prop.containsKey("last")) {
				lastUser = Long.valueOf(prop.getProperty("last"));
			}
		}
	}

	// private ExecutorService createExec(final String name) {
	// return Executors.newFixedThreadPool(MAX_SUB_THREADS,
	// new ThreadFactory() {
	//
	// @Override
	// public Thread newThread(Runnable r) {
	// ThreadFactory tf = Executors.defaultThreadFactory();
	// Thread t = tf.newThread(r);
	// t.setName("User Converter Thread for " + name);
	// return t;
	// }
	// });
	// }

	private void run() throws Exception {
		// Profiler profiler = new Profiler();
		// profiler.startCollecting();
		// application code

		long init = System.currentTimeMillis();
		try {

			ExecutorService exec = Executors.newFixedThreadPool(MAX_THREADS,
					new ThreadFactory() {

						@Override
						public Thread newThread(Runnable r) {
							ThreadFactory tf = Executors.defaultThreadFactory();
							Thread t = tf.newThread(r);
							t.setName("Per User Thread");
							return t;
						}
					});
			final Semaphore outer = new Semaphore(MAX_THREADS);

			boolean found = false;

			long rateTime = System.currentTimeMillis();

			for (final UserFolder userFolder : folder) {
				if (lastUser == null || userFolder.getUser().equals(lastUser)) {
					found = true;
				}

				if (!found)
					continue;

				if ((cont + 1) % 100 == 0) {
					float l = System.currentTimeMillis() - rateTime;
					if (l != 0) {
						System.out.println("Current count " + cont + " user "
								+ userFolder.getUser() + " " + 100 / (l / 1000)
								+ " users per second");
					}
					rateTime = System.currentTimeMillis();
				}
				if ((cont + 1) % COMPACT_SIZE == 0) {
					while (outer.availablePermits() != MAX_THREADS) {
						System.out.println("Waiting to start compacting...");
						Thread.sleep(10000);
					}
					System.out.println("Compacting...");

					store.commit();

					System.out.println("Finished Compacting...");
				}
				outer.acquire();
				synchronized (migrated) {
					migrated.add(userFolder.getUser());
				}

				cont++;

				plainSizeOnDisk += userFolder.getFavs().length()
						+ userFolder.getTweets().length()
						+ userFolder.getFollowees().length()
						+ userFolder.getFollowers().length()
						+ userFolder.getInfo().length();

				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							// Semaphore inner = new Semaphore(-5);
							saveTweets(userFolder);// , inner);
							saveAdjacency(userFolder);// , inner);
							saveInfo(userFolder);// , inner);
							saveStatus(userFolder.getUser(), statusDir);// ,
																		// inner);
							// inner.acquire();
							synchronized (migrated) {
								Long u = migrated.get(0);
								done.add(userFolder.getUser());
								if (u.equals(userFolder.getUser())) {
									Iterator<Long> it = migrated.iterator();
									while (it.hasNext()) {
										Long curr = it.next();
										if (done.contains(curr)) {
											saveLastMigrated(curr);
											it.remove();
											done.remove(curr);
										} else
											break;
									}
								}

								// migrated.remove(userFolder.getUser());

							}

						} catch (Exception e) {
							e.printStackTrace();
							// System.out.println(e.getCause() + " : "
							// + e.getMessage());
						} finally {
							outer.release();
						}
					}

				});

			}
			exec.shutdown();
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			// FavsExecutor.shutdown();
			// TweetsExecutor.shutdown();
			// StatusExecutor.shutdown();
			// InfoExecutor.shutdown();
			// FolloweesExecutor.shutdown();
			// FollowersExecutor.shutdown();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("Closing Store");
			store.close();
			shell.stop();

			System.out.println("Tamaño total en formato plano: "
					+ plainSizeOnDisk / (1024 * 1024) + " MB ");

		}
		System.out.println("Tiempo de exportación: "
				+ (System.currentTimeMillis() - init) / 1000);
		// System.out.println(profiler.getTop(3));

	}

	private void saveAdjacency(final UserFolder userFolder) {
		// FolloweesExecutor.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		try {
			// if (!store.hasAdjacency(userFolder.getUser(),
			// ListType.FOLLOWEES)) {
			// long[] followees =
			// followeeSize += followees.length;
			store.saveAdjacents(userFolder.getUser(), ListType.FOLLOWEES,
					StoreUtil.listReader(userFolder.getFollowees()),
					skipChecking);
			// }
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception storing FOLLOWEES("
					+ userFolder.getUser() + "): " + e.getClass() + ":"
					+ e.getMessage() + ":" + e.getCause());
		}
		// sem.release();
		// }
		// });
		// FollowersExecutor.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		try {
			// if (!store.hasAdjacency(userFolder.getUser(),
			// ListType.FOLLOWERS)) {
			store.saveAdjacents(userFolder.getUser(), ListType.FOLLOWERS,
					StoreUtil.listReader(userFolder.getFollowers()),
					skipChecking);
			// }
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception storing FOLLOWERS("
					+ userFolder.getUser() + "): " + e.getClass() + ":"
					+ e.getMessage() + ":" + e.getCause());
		}
		// sem.release();
		// }
		// });

	}

	private void saveInfo(final UserFolder userFolder) {
		// InfoExecutor.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		try {
			// if (!store.hasInfo(userFolder.getUser())) {
			UserInfo info = StoreUtil.toInfo(userFolder.getInfo());
			if (info == null)
				// System.out.println("Info for user "
				// + userFolder.getUser()
				// + " not found or badly parsed.");
				;
			else {

				store.saveUserInfo(info, skipChecking);
			}
			// }
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception storing INFO ("
					+ userFolder.getUser() + "): " + e.getClass() + ":"
					+ e.getMessage() + ":" + e.getCause());
		}
		// sem.release();
		// }
		// });

	}

	private synchronized void saveLastMigrated(Long long1) {
		prop.put("last", new Long(long1).toString());
		try {
			File file = new File(propFile.getPath() + ".new");
			FileWriter fileWriter = new FileWriter(file);
			prop.store(fileWriter, "");
			fileWriter.flush();
			fileWriter.close();
			Files.move(file, propFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void saveStatus(final Long user, final File userStatus) {
		// StatusExecutor.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		try {
			// if (!store.hasStatus(user)) {
			Properties prop = new Properties();
			String name = userStatus.getPath() + "/" + user + ".prop";
			if (!new File(name).exists()) {
				System.out.println("Properties file not found for " + user
						+ ".");
				return;
			}
			prop.load(new FileInputStream(name));
			Map<String, String> map = new HashMap<>();
			for (Entry<Object, Object> e : prop.entrySet()) {
				map.put(e.getKey().toString(), e.getValue().toString());
			}
			store.saveUserStatus(user, map, skipChecking);
			// }
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass() + ":" + e.getMessage() + ":"
					+ e.getCause());
		} finally {
			// sem.release();
		}
		//
		// }
		// });

	}

	private void saveTweets(final UserFolder userFolder) {
		// TweetsExecutor.execute(new Runnable() {
		// @Override
		// public void run() {
		try {
			// if (!store.hasTweets(userFolder.getUser(),
			// TweetType.TWEETS))
			store.saveTweets(
					userFolder.getUser(),
					TweetType.TWEETS,
					StoreUtil.tweetReader(userFolder.getUser(),
							userFolder.getTweets()), skipChecking);
			// store.compactTweets(userFolder.getUser(),
			// TweetType.TWEETS);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception storing TWEETS("
					+ userFolder.getUser() + "): " + e.getClass() + ":"
					+ e.getMessage() + ":" + e.getCause());
		}
		// sem.release();
		// }
		// });
		// FavsExecutor.execute(new Runnable() {
		// @Override
		// public void run() {
		try {
			// if (!store.hasTweets(userFolder.getUser(),
			// TweetType.FAVORITES))
			store.saveTweets(
					userFolder.getUser(),
					TweetType.FAVORITES,
					StoreUtil.tweetReader(userFolder.getUser(),
							userFolder.getFavs()), skipChecking);
			// store.compactTweets(userFolder.getUser(),
			// TweetType.FAVORITES);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception storing FAVORITES("
					+ userFolder.getUser() + "): " + e.getClass() + ":"
					+ e.getMessage() + ":" + e.getCause());
		}
		// sem.release();
		// }
		// });
	}
}
