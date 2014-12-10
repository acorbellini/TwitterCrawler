package isistan.twitter.crawler.folder;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Scanner;

public class CrawlFolder implements Iterable<UserFolder> {
	private String basedir;
	private String ul;

	public CrawlFolder(String baseDir, String ul) {
		this.basedir = baseDir;
		this.ul = ul;
	}

	public CrawlFolder(String path) {
		this(path, null);
	}

	@Override
	public Iterator<UserFolder> iterator() {
		if (ul != null)
			try {
				return new Iterator<UserFolder>() {
					Scanner prov = new Scanner(new File(ul));

					@Override
					public boolean hasNext() {
						boolean hasNext = prov.hasNext();
						if (!hasNext)
							prov.close();
						return hasNext;
					}

					@Override
					public UserFolder next() {
						long u = prov.nextLong();
						return new UserFolder(u, basedir + "/crawl/" + u);
					}

					@Override
					public void remove() {

					}
				};
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		else
			return new Iterator<UserFolder>() {
				String[] list = new File(basedir + "/crawl/").list();
				int cont = 0;

				@Override
				public boolean hasNext() {
					while (cont != list.length
							&& !Files.isDirectory(Paths.get(basedir + "/crawl/"
									+ list[cont])))
						cont++;
					if (cont != list.length)
						return true;
					return false;
				}

				@Override
				public UserFolder next() {
					long u = Long.valueOf(list[cont++]);
					return new UserFolder(u, basedir + "/crawl/" + u);
				}

				@Override
				public void remove() {

				}
			};
		return null;
	}
}
