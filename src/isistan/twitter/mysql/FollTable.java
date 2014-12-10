package isistan.twitter.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Scanner;

public class FollTable {

	private String name;
	private Long userID;
	private String follPath;
	private int MAX_FOLLS = 1000000;
	private Connection conn;

	public static final String columns = "USERID, NUM, LIST";
	
	public static final String folStructure = "`USERID` BIGINT NOT NULL,"
			+ "`NUM` INT NOT NULL," + "`LIST` LONGTEXT,"
			+ "PRIMARY KEY (`USERID`,`NUM`)";

	public FollTable(Connection conn, String name, String folT,
			File userFolder, Long userID) throws Exception {
		this.conn = conn;
		this.name = folT;
		this.userID = userID;
		this.follPath = userFolder + "/" + userID + "-" + name + ".dat";
	}

	public String getTableName() {
		return name;
	}

	public void saveFolls() throws SQLException, IOException {
		File folFile = new File(follPath);
		if (!folFile.exists()) {
			// System.out.println("Followers file " + follPath
			// + " does not exist.");
			return;
		}

		int cont = 0;

		LinkedList<String> folls = new LinkedList<>();

		BufferedReader scn = new BufferedReader(new FileReader(folFile));
		while (scn.ready()) {
			String line = scn.readLine();
			folls.add(line);

			if (folls.size() > MAX_FOLLS) {
				addFolls(folls, cont++);
				folls.clear();
			}

		}
		addFolls(folls, cont);
		scn.close();
	}

	private void addFolls(LinkedList<String> folls, int cont)
			throws SQLException {
		if (folls.isEmpty())
			return;
		boolean start = true;

		StringBuilder builder = new StringBuilder();
		builder.append("'" + userID.toString() + "',");
		builder.append("'" + cont + "','");
		for (String l : folls) {
			if (start) {
				builder.append(l);
				start = false;
			} else
				builder.append("," + l);
		}
		builder.append("'");

		PlainTextExporter.insert(conn, name, columns, builder.toString());
	}

}
