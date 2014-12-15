package isistan.twitter.crawler.config;

import isistan.twitter.crawler.request.RequestType;
import isistan.twitter.crawler.store.CrawlerStore;
import isistan.twitter.crawler.store.bigtext.BigTextStore;
import isistan.twitter.crawler.util.CrawlerUtil;
import isistan.twitter.crawler.util.CrawlerUtil.UserIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

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

	private static CrawlerConfiguration current = null;

	private int numAccounts;

	private String outputdir;

	private String userListFile;

	private File crawlDir;

	private File crawlstatus;

	private List<File> oauthdirs;

	private boolean recrawlInfo;

	private boolean crawlOnlyFollowees;

	private int max_threads;

	private HashMap<Account, Integer> failures = new HashMap<Account, Integer>();

	private ArrayList<Account> accountPool = new ArrayList<>();

	private Logger log = Logger.getLogger(CrawlerConfiguration.class);

	private TreeSet<Long> minimumAlreadyCrawled = new TreeSet<Long>();

	private UserIterator users;

	private Properties ltProp;

	private CrawlerStore store;

	private Long latestCrawled;

	private CrawlerConfiguration(Properties config) throws Exception {
		outputdir = config.getProperty(OUTPUTDIR);
		userListFile = config.getProperty(INPUT_FILE);
		crawlDir = new File(outputdir + "/crawl");
		crawlstatus = new File(outputdir + "/crawl-status");

		BigTextStore bt = new BigTextStore(new File(outputdir));
		store = new CrawlerStore(bt, outputdir);
		oauthdirs = new ArrayList<File>();

		for (String s : config.getProperty(OAUTHDIR).split(",")) {
			oauthdirs.add(new File(s));
		}

		recrawlInfo = false;
		if (config.getProperty("recrawlInfo") != null)
			recrawlInfo = new Boolean(config.getProperty("recrawlInfo"));
		crawlOnlyFollowees = false;
		if (config.getProperty("crawlOnlyFollowees") != null)
			crawlOnlyFollowees = new Boolean(
					config.getProperty("crawlOnlyFollowees"));
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

		if (latestCrawled == null) {
			minimumAlreadyCrawled.add(users.peek() - 1);
			latestCrawled = users.peek() - 1;
		} else
			minimumAlreadyCrawled.add(latestCrawled);
	}

	public synchronized void discardAccount(Account current2) {
		accountPool.remove(current2);
	}

	public synchronized Account getAccount(final RequestType reqType) {
		while (true) {
			final Long curr = System.currentTimeMillis();
			ArrayList<Account> sortedByTime = new ArrayList<>(accountPool);
			Collections.sort(sortedByTime, new Comparator<Account>() {
				@Override
				public int compare(Account o1, Account o2) {
					return Long.valueOf(o1.getTime(reqType) - curr).compareTo(
							o2.getTime(reqType) - curr);
				}
			});
			for (Account account : sortedByTime) {
				if (!account.isUsed(reqType)) {
					account.setUsed(reqType);
					return account;
				}
			}
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
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

	public HashMap<Account, Integer> getFailures() {
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

	public CrawlerStore getStore() {
		return store;
	}

	public synchronized long getTime(Account current2, RequestType reqType) {
		return current2.getTime(reqType);
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

	public boolean isCrawlOnlyFollowees() {
		return crawlOnlyFollowees;
	}

	public boolean mustRecrawlInfo() {
		return recrawlInfo;
	}

	public synchronized void release(Account current2, RequestType reqType) {
		current2.release(reqType);
	}

	public synchronized void setTime(Account current2, RequestType reqType,
			long l) {
		current2.setTime(reqType, l);
	}

	public void updateLatestCrawled(final long user) {
		synchronized (minimumAlreadyCrawled) {
			long latest = user;
			minimumAlreadyCrawled.add(user);
			long next = minimumAlreadyCrawled.first() + 1;
			while (minimumAlreadyCrawled.size() > 1
					&& minimumAlreadyCrawled.contains(next)) {
				minimumAlreadyCrawled.remove(minimumAlreadyCrawled.first());
				latest = next;
				next++;
			}
			try {
				store.updateLatestCrawled(user);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
