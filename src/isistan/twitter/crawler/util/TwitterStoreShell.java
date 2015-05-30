package isistan.twitter.crawler.util;

import isistan.twitter.crawler.store.bigtext.TwitterStore;
import isistan.twitter.crawler.util.web.Shell;

import java.io.File;
import java.net.URI;
import java.util.logging.LogManager;

import javax.ws.rs.core.UriBuilder;

import org.slf4j.bridge.SLF4JBridgeHandler;

import twitter4j.Logger;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.net.httpserver.HttpServer;

public class TwitterStoreShell {
	private TwitterStore store;
	private int port;
	private HttpServer server;

	public TwitterStoreShell(TwitterStore store, int port) {
		this.store = store;
		this.port = port;
	}

	public static void main(String[] args) throws Exception {
		TwitterStore store = new TwitterStore(new File(args[0]));
		int port = 8080;
		if (args.length > 1)
			port = Integer.valueOf(args[1]);
		new TwitterStoreShell(store, port).start();

	}

	public void start() throws Exception {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.install();

		ResourceConfig config = new PackagesResourceConfig(
				"isistan.twitter.crawler.util.web");
		Shell.init(store);
		URI uri = getURI(port);
		this.server = HttpServerFactory.create(uri, config);
		server.start();
		Logger log = Logger.getLogger(TwitterStoreShell.class);
		log.info("Started Web Server at " + uri);
		// List<String> history = new ArrayList<>();
		// Binding binding = new Binding();
		// binding.setVariable("store", new TwitterStore(new File(args[0])));
		// GroovyShell shell = new GroovyShell(binding);
		// String exp = "";
		// while (!exp.equals("exit")) {
		// System.out.print("> ");
		// BufferedReader reader = new BufferedReader(new InputStreamReader(
		// System.in));
		// exp = reader.readLine();
		// Object value;
		// try {
		// value = shell.evaluate(exp);
		// history.add(exp);
		// if (value != null)
		// System.out.println(value.toString());
		// } catch (Exception e) {
		// System.out.println(e.getMessage());
		// }
		//
		// }
	}

	private static URI getURI(int port) {
		return UriBuilder.fromUri("http://localhost/").port(port).build();
	}

	public void stop() {
		this.server.stop(0);
	}
}
