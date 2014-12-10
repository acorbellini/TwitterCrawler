package isistan.twitter.mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class PlainTextExporter {
	private String from;
	private String list;
	public static final int MAX_ROWS = 100000000;
	public static final int MAX_CONN = 20;

	private static ArrayList<Connection> connectionPool = new ArrayList<>();
	private static HashMap<String, Counter> counts = new HashMap<>();
	private static HashSet<String> alreadyCreated = new HashSet<>();

	public PlainTextExporter(String from, String mysql, String user,
			String pass, String list) throws SQLException {
		this.from = from;
		this.list = list;
		fillPool(mysql, user, pass);
	}

	private void fillPool(String mysql2, String user, String pass)
			throws SQLException {
		for (int i = 0; i < MAX_CONN; i++)
			connectionPool.add(DriverManager.getConnection(mysql2, user, pass));
	}

	public static void main(String[] args) throws Exception {
		PlainTextExporter exp = new PlainTextExporter(args[0], args[1],
				args[2], args[3], args[4]);
		exp.run();
	}

	public void run() throws InterruptedException, FileNotFoundException {
		// String crawlPath = new String(from + "/crawl");
		// String crawlStatusPath = new String(from + "/crawl-status");

		ExecutorService exec = Executors.newFixedThreadPool(MAX_CONN);
		final Semaphore max = new Semaphore(MAX_CONN);

		Scanner scanner = new Scanner(new File(list));

		// for (final File f : new File(crawlPath).listFiles()) {
		int count = 0;
		while (scanner.hasNext()) {
			final long user = scanner.nextLong();

			final Connection conn = connectionPool.get(count % MAX_CONN);
			System.out.println(count + ": Procesing user " + user + ".");
			count++;
			max.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						UserTable.save(conn, new File(from + "/crawl/" + user));
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						max.release();
					}

				}
			});
		}
		scanner.close();
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

	}

	public static class Counter {
		int cont = 0;

		public Counter(int cont) {
			this.cont = cont;
		}

		public synchronized int getCont() {
			return cont;
		}

		public synchronized void incCont() {
			cont++;
		}
	}

	public static String findTable(Connection conn, String tableName)
			throws Exception {
		int count = 1;
		String table = tableName + count;
		while (true) {
			try {
				// Counter cant = getCounter(conn, table);
				int size = getCant(conn, table);
				// if (cant.getCont() >= PlainTextExporter.MAX_ROWS)
				if (size >= PlainTextExporter.MAX_ROWS)
					table = tableName + ++count;
				else
					return table;
			} catch (Exception e) {
				return table;
			}
		}
	}

	private static Counter getCounter(Connection conn, String table)
			throws SQLException {
		Counter cant = counts.get(table);
		if (cant == null)
			synchronized (counts) {
				cant = counts.get(table);
				if (cant == null) {
					cant = new Counter(getCant(conn, table));
					counts.put(table, cant);
				}

			}
		return cant;
	}

	private static Integer getCant(Connection conn, String table)
			throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("SELECT COUNT(*) FROM " + table);
			ResultSet rs = stmt.getResultSet();
			return rs.getInt(0);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			stmt.close();
		}
		return 0;

	}

	// static synchronized Connection getConnection() {
	// return connectionPool
	// .get((int) (Math.random() * connectionPool.size()));
	// }

	public static boolean create(Connection conn, String name,
			String structure, boolean ifNotExists) throws SQLException {
		if (alreadyCreated.contains(name))
			return false;
		else {
			synchronized (alreadyCreated) {
				if (alreadyCreated.contains(name))
					return false;
				Statement stmt = conn.createStatement();
				try {
					stmt.execute("CREATE TABLE "
							+ (ifNotExists ? "IF NOT EXISTS " : "") + name
							+ "(" + structure + " ) ;");
					// ENGINE = MyISAM;");
					alreadyCreated.add(name);
					return true;
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				} finally {
					stmt.close();
				}
			}
		}

	}

	public static void createIfNotExist(Connection conn, String name,
			String structure) throws SQLException {
		create(conn, name, structure, true);
	}

	public static String escape(String value) {
		return escape(value, true);
	}

	public static String escape(String value, boolean quote) {
		if (value == null)
			return "\\N";

		if (value.isEmpty())
			return "\\N";

		if (value.toUpperCase().equals("NULL"))
			return "\\N";

		value = value.replace("\0", "");

		value = value.replace("\\", "\\\\");

		value = value.replace(";", "\\u003B");

		value = value.replace("'", "\\u0022");

		if (quote) {
			if (!value.startsWith("'") || !value.endsWith("'"))
				value = "'" + value + "'";
		}
		return value;
	}

	public static void insert(Connection conn, String table, String columns,
			String values) throws SQLException {
		ArrayList<String> cont = new ArrayList<>();
		cont.add(values);
		insert(conn, table, columns, cont);
	}

	public static void insert(Connection conn, String table, String columns,
			List<String> rows) throws SQLException {
		int size = 20;
		for (String string : rows) {
			size += string.length() + 1;
		}
		boolean start = true;
		StringBuilder builder = new StringBuilder(size);
		for (String v : rows) {
			if (start) {
				start = false;
				builder.append("( " + v + ")");
			} else
				builder.append(",( " + v + ")");
		}

		Statement stmt = conn.createStatement();
		try {
			stmt.execute("INSERT INTO " + table + "(" + columns + ") VALUES "
					+ builder.toString());

			Counter cant = getCounter(conn, table);
			cant.incCont();
		} catch (Exception e) {
			throw e;
			// System.out.println(e.getMessage());
			// System.out.println("INSERT INTO " + table + "(" + columns
			// + ") VALUES " + builder.toString());
		} finally {
			stmt.close();
		}
	}

	public static boolean contains(Connection conn, String table,
			String column, String val) throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("SELECT " + column + " FROM " + table + " WHERE "
					+ column + " = " + val);
			ResultSet rs = stmt.getResultSet();

			return rs.first();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			stmt.close();
		}
		return false;
	}

	public static void loadData(Connection conn, String table, String file)
			throws Exception {
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("LOAD DATA INFILE '"
					+ file
					+ "' INTO TABLE "
					+ table
					+ " FIELDS TERMINATED BY ';' ENCLOSED BY '' ESCAPED BY '\\\\'");
		} catch (Exception e) {
			throw e;
		} finally {
			stmt.close();
		}
	}

	public static void copy(Connection conn, String table, String file)
			throws Exception {
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("COPY "
					+ table
					+ " FROM '"
					+ file
					+ "' WITH (FORMAT csv, DELIMITER ';', ESCAPE '\\', NULL '\\N', QUOTE '''')");
		} catch (Exception e) {
			throw e;
		} finally {
			stmt.close();
		}
	}

	public static Integer getMax(Connection conn, String table, String col)
			throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			stmt.execute("SELECT MAX(" + col + ") FROM " + table);
			ResultSet rs = stmt.getResultSet();

			return rs.getInt(1);
		} catch (Exception e) {
		} finally {
			stmt.close();
		}
		return 1;
	}

	public static String reserveTable(Connection conn, String t,
			String structure) throws Exception {
		String table = findTable(conn, t);
		createIfNotExist(conn, table, structure);
		return table;
	}

	// public static void insert(String name, String columns, List<String>
	// toInsert)
	// throws SQLException {
	// insert(name, columns, toInsert.toArray(new String[] {}));
	// }

	// public static ResultSet get(String table, String column, String val)
	// throws SQLException {
	// Statement stmt = getConnection().createStatement();
	// try {
	// stmt.execute("SELECT " + column + " FROM " + table + " WHERE "
	// + column + " = " + val);
	// ResultSet rs = stmt.getResultSet();
	// stmt.close();
	// return rs;
	// } catch (Exception e) {
	// }
	// return null;
	// }

}
