package isistan.twitter.crawler.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class ListReader implements Iterator<Long> {

	private BufferedReader reader;
	private Long curr = null;
	int cont = 0;
	private File foll;

	public ListReader(File foll) {
		this.foll = foll;
		try {
			this.reader = new BufferedReader(new FileReader(foll));
			this.curr = getNext();
		} catch (FileNotFoundException e) {
			this.curr = null;
		}
	}

	private Long getNext() {
		try {
			while (reader.ready()) {
				String f = reader.readLine();
				cont++;
				f = f.replaceAll("\0", "");
				try {
					return Long.valueOf(f);
				} catch (Exception e) {
					System.out.println("Error reading line " + cont
							+ " for user file " + foll + " with error"
							+ e.getMessage() + " - " + e.getCause());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return curr != null;
	}

	@Override
	public Long next() {
		Long ret = curr;
		curr = getNext();
		return ret;

	}

}