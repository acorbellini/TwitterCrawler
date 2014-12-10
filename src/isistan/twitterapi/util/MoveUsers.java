package isistan.twitterapi.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MoveUsers {
	public static void main(final String[] args) throws IOException,
			InterruptedException {
		final String basePath = args[0];
		final String destPath = args[1];

		ExecutorService exec = Executors.newFixedThreadPool(100);

		Scanner userList = new Scanner(new File(args[2]));
		final AtomicInteger count = new AtomicInteger();

		final Semaphore max = new Semaphore(100);
		while (userList.hasNext()) {

			final Long user = userList.nextLong();
			max.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					System.out.println("Moving user " + user
							+ " which is number " + count.incrementAndGet()
							+ " in list " + args[2]);
					Path orig = Paths.get(basePath + "/crawl/" + user);
					Path dest = Paths.get(destPath + "/crawl/" + user);
					try {
						Files.createDirectories(Paths.get(destPath + "/crawl/"));
					} catch (IOException e1) {
					}
					try {
						Files.move(orig, dest,
								StandardCopyOption.REPLACE_EXISTING);
					} catch (NoSuchFileException nsf) {

					} catch (Exception e) {
						e.printStackTrace();
					}

					orig = Paths.get(basePath + "/crawl-status/" + user
							+ ".prop");
					dest = Paths.get(destPath + "/crawl-status/" + user
							+ ".prop");
					try {
						Files.createDirectories(Paths.get(destPath
								+ "/crawl-status/"));
					} catch (IOException e1) {
					}
					try {
						Files.move(orig, dest);
					} catch (NoSuchFileException nsf) {

					} catch (Exception e) {
						e.printStackTrace();
					}

					max.release();

				}
			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		userList.close();
	}
}
