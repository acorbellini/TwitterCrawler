package isistan.twitter.crawler.config;

import isistan.twitter.crawler.request.RequestType;
import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.util.CrawlerUtil;
import isistan.twitter.crawler.util.CrawlerUtil.UserIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class CrawlerConfiguration {

	public static CrawlerConfiguration create(Properties config)
			throws Exception {
		return current = new CrawlerConfiguration(config);
	}

	public static CrawlerConfiguration getCurrent() {
		return current;
	}

	public static final int MAX_TRIES_BEFORE_DISCARD_ACCOUNT = 2;

	public static final int TIME_IF_SUSPENDED = 150;

	public static final int MIN_WAIT_TO_NEXT_REQUEST = 60;

	// Claves del property

	public static final Integer MIN_TWEETS = 10;

	public static final Integer MIN_FOLLOWEES = 10;

	public static final String CONFIGDIR = "configdir";

	public static final String OAUTHDIR = "oauthdir";

	public static final String INPUT_FILE = "input";

	public static final String OUTPUTDIR = "output";

	public static final String MAX_THREADS = "threads";

	public static final Integer MAX_ACCOUNT_FAILURES = 10;

	private static final String STORE = "store";

	private static final String LANGUAGE = "lang";

	private static CrawlerConfiguration current = null;

	HashSet<Long> done = new HashSet<>();

	private String lang;

	private int numAccounts;

	private String outputdir;

	private String userListFile;

	private File crawlDir;

	private File crawlstatus;

	private List<File> oauthdirs;

	private boolean forceRecrawl = false;

	private boolean crawlFollowees = false;
	private boolean crawlFollowers = false;
	private boolean crawlTweets = false;
	private boolean crawlFavorites = false;

	private int max_threads;

	private Map<Account, Integer> failures = new ConcurrentHashMap<Account, Integer>();

	private ArrayList<Account> accountPool = new ArrayList<>();

	private Logger log = Logger.getLogger(CrawlerConfiguration.class);

	private List<Long> minimumAlreadyCrawled = new ArrayList<Long>();

	private UserIterator users;

	private Properties ltProp;

	private TwitterStore store;

	private Long latestCrawled;

	private CrawlerConfiguration(Properties config) throws Exception {
		outputdir = config.getProperty(OUTPUTDIR);
		userListFile = config.getProperty(INPUT_FILE);
		crawlDir = new File(outputdir + "/crawl");
		crawlstatus = new File(outputdir + "/crawl-status");

		store = new TwitterStore(new File(outputdir));
		// store = new CrawlerStore(bt, outputdir);
		oauthdirs = new ArrayList<File>();

		for (String s : config.getProperty(OAUTHDIR).split(",")) {
			oauthdirs.add(new File(s));
		}

		forceRecrawl = false;
		if (config.getProperty("forceRecrawl") != null)
			forceRecrawl = new Boolean(config.getProperty("forceRecrawl"));
		String crawlList = config.getProperty("crawl");
		if (crawlList != null) {
			for (String el : crawlList.replace(" ", "").split(",")) {
				switch (el) {
				case "TWEETS":
					crawlTweets = true;
					break;
				case "FAVORITES":
					crawlFavorites = true;
					break;
				case "FOLLOWEES":
					crawlFollowees = true;
					break;
				case "FOLLOWERS":
					crawlFollowers = true;
					break;
				default:
					break;
				}
			}
		} else {
			crawlFollowees = true;
			crawlFollowers = true;
			crawlTweets = true;
			crawlFavorites = true;
		}
		for (File oauthdir : oauthdirs) {
			if (oauthdir.isDirectory() && oauthdir.exists()) {
				for (File f : oauthdir.listFiles()) {
					if (f.isFile()) {
						accountPool.add(new Account(f));
					}
				}
			}
		}

		max_threads = config.getProperty(MAX_THREADS) == null ? accountPool
				.size() : Integer.valueOf(config.getProperty(MAX_THREADS));

		numAccounts = accountPool.size();

		users = CrawlerUtil.scanIds(userListFile);

		latestCrawled = store.getLatestCrawled();

		lang = config.getProperty(LANGUAGE) == null ? "english" : config
				.getProperty(LANGUAGE);
		// if (latestCrawled == null) {
		// minimumAlreadyCrawled.add(users.peek() - 1);
		// latestCrawled = users.peek() - 1;
		// } else
		// minimumAlreadyCrawled.add(latestCrawled);
	}

	public void discardAccount(Account current2) {
		current2.setDiscarded();
	}

	public Account getAccount(final RequestType reqType) {
		while (true) {
			ArrayList<Account> sorted = new ArrayList<Account>();
			final ArrayList<Long> times = new ArrayList<>();
			ArrayList<Integer> indexes = new ArrayList<Integer>();
			int i = 0;
			long curr = System.currentTimeMillis();
			for (Account account : accountPool) {
				if (!account.isDiscarded()) {
					Long t = account.getTime(reqType) - curr;
					times.add(t);
					sorted.add(account);
					indexes.add(i++);
				}
			}
			Collections.sort(indexes, new Comparator<Integer>() {

				@Override
				public int compare(Integer o1, Integer o2) {
					return times.get(o1).compareTo(times.get(o2));
				}

			});
			for (Integer integer : indexes) {
				Account account = sorted.get(integer);
				synchronized (account) {
					if (!account.isDiscarded() && !account.isUsed(reqType)) {
						account.setUsed(reqType);
						return account;
					}
				}
			}
			synchronized (accountPool) {
				try {
					accountPool.wait(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public int getAccountSize() {
		return accountPool.size();
	}

	public File getCrawlDir() {
		return crawlDir;
	}

	public File getCrawlstatus() {
		return crawlstatus;
	}

	public Map<Account, Integer> getFailures() {
		return failures;
	}

	public Long getLatestCrawled() {
		return latestCrawled;
	}

	public int getMaxThreads() {
		return max_threads;
	}

	public int getMinFolloweesToFilter() {
		return MIN_FOLLOWEES;
	}

	public int getMinTweetsToFilter() {
		return MIN_TWEETS;
	}

	public int getNumAccounts() {
		return numAccounts;
	}

	public List<File> getOauthdirs() {
		return oauthdirs;
	}

	public String getOutputdir() {
		return outputdir;
	}

	public TwitterStore getStore() {
		return store;
	}

	public String getUserCrawlDir(long u) {
		return crawlDir + "/" + u;
	}

	public String getUserListFile() {
		return userListFile;
	}

	public String getUserPropertiesPath(long u) {
		return crawlstatus.getPath() + "/" + u + ".prop";
	}

	public UserIterator getUsers() {
		return users;
	}

	public boolean isForceRecrawl() {
		return forceRecrawl;
	}

	public void updateLatestCrawled(final long user) throws Exception {
		synchronized (minimumAlreadyCrawled) {
			Long u = minimumAlreadyCrawled.get(0);
			done.add(user);
			if (u.equals(user)) {
				Iterator<Long> it = minimumAlreadyCrawled.iterator();
				Long curr = null;
				while (it.hasNext()) {
					curr = it.next();
					if (done.contains(curr)) {
						it.remove();
						done.remove(curr);
					} else
						return;
				}
				if (curr != null)
					store.updateLatestCrawled(curr);
			}
		}
	}

	public void registerUser(Long user) {
		synchronized (minimumAlreadyCrawled) {
			minimumAlreadyCrawled.add(user);
		}
	}

	public void notifyAccountReleased() {
		synchronized (accountPool) {
			accountPool.notifyAll();
		}
	}

	public boolean isCrawlFavorites() {
		return crawlFavorites;
	}

	public boolean isCrawlTweets() {
		return crawlTweets;
	}

	public boolean isCrawlFollowers() {
		return crawlFollowers;
	}

	public boolean isCrawlFollowees() {
		return crawlFollowees;
	}

	public String getLanguage() {
		return lang;
	}

}
