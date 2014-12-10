package isistan.twitter.crawler.store.plain;

import isistan.twitter.crawler.CrawlerUtil;
import isistan.twitter.crawler.config.CrawlerConfiguration;
import isistan.twitter.crawler.status.UserStatus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PlainUserStatus extends UserStatus {

	private CrawlerConfiguration config = CrawlerConfiguration.getCurrent();
	private String path;
	private Properties prop = new Properties();

	public PlainUserStatus(long user) throws FileNotFoundException, IOException {
		super(user);
		this.user = user;
		this.path = config.getUserPropertiesPath(user);
		this.prop = CrawlerUtil.openProperties(path);
	}

	@Override
	public String get(String string) {
		Object val = prop.get(string);
		if (val != null)
			return val.toString();
		return null;
	}

	@Override
	public void set(String k, String v) {
		try {
			CrawlerUtil.updateProperty(k, v, prop, path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
