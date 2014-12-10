package isistan.twitter.mysql;

import isistan.def.utils.table.CSVBuilder;
import isistan.def.utils.table.Cell;
import isistan.def.utils.table.Table;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class UserInfoTable {

	private String columns = "USERID, " + "SCREEN_NAME, " + "NAME, "
			+ "DESCRIPTION, " + "LANG, " + "CREATED," + "LOCATION, "
			+ "TIMEZONE, " + "UTC, " + "FOLLOWEESCOUNT, " + "FOLLOWERSCOUNT,"
			+ "FAVCOUNT," + "LISTEDCOUNT, " + "TWEETSCOUNT, " + "ISVERIFIED, "
			+ "ISPROTECTED";

	private String userInfoPath;

	private String name;

	private Long userID;

	private Connection conn;

	public static final String userInfoStructure = "`USERID` BIGINT NOT NULL ,"
			+ "`SCREEN_NAME` VARCHAR(45) ," + "`NAME` VARCHAR(100)  ,"
			+ "`DESCRIPTION` VARCHAR(400)  ," + "`LANG` VARCHAR(10)  ,"
			+ "`CREATED` VARCHAR(30)  ," + "`LOCATION` VARCHAR(100)  ,"
			+ "`TIMEZONE` VARCHAR(60)  ," + "`UTC` INTEGER  ,"
			+ "`FOLLOWEESCOUNT` INTEGER  ," + "`FOLLOWERSCOUNT` INTEGER  ,"
			+ "`FAVCOUNT` INTEGER  ," + "`LISTEDCOUNT`INTEGER  ,"
			+ "`TWEETSCOUNT` INTEGER  ," + "`ISVERIFIED` BOOLEAN  ,"
			+ "`ISPROTECTED` BOOLEAN  ," + "PRIMARY KEY (`USERID`)";

	// private Long userID;

	public static final String USERINFO = "USERINFO";

	public UserInfoTable(Connection conn, String table, File userFolder,
			Long userID) throws Exception {
		this.name = table;
		this.conn = conn;
		this.userID = userID;
		this.userInfoPath = userFolder.getPath() + "/" + userID + "-info.dat";
	}

	public void saveInfo() throws Exception {
		List<String> toInsert = new ArrayList<>();

		File userInfoF = new File(userInfoPath);
		if (!userInfoF.exists()) {
			// System.out.println("User Info file " + userInfoPath
			// + "does not exist.");
			return;
		}
		CSVBuilder builder = new CSVBuilder(userInfoF);
		// builder.setExpectedFields(16);
		Table t = builder.toTable();

		if (t.getRowLimit() == 0) {
			// System.out.println("User ID " + userID
			// + " has it's user info file empty.");
			return;
		}

		while (t.getRowLimit() > 2)
			t.mergeRows(1, 2);

		String lang = t.get(4, 1).value(); // Where LANG Should be
		while (!lang.matches("[a-z][a-z]")
				&& !lang.matches("[a-z][a-z]-[a-z][a-z]")
				&& !lang.matches("[a-z][a-z][a-z]")) {
			// Might not be a language
			t.merge(3, 1, 4, 1);// JOIN DESCRIPTION AND LANGUAGE
			lang = t.get(4, 1).value(); // Where LANG Should be
		}

		while (t.getRow(1).size() > 16) {
			t.merge(6, 1, 7, 1);// LOCATION AND TIMEZONE

		}

		while (t.getRow(1).size() < 16)
			t.insCol(6);// LOCATION INSERTED

		int size = 16;
		for (int i = 0; i < 14; i++) {
			Cell c = t.get(i, 1);
			if (c != null) {
				String val = c.value();
				toInsert.add(val);
				size += val.length() + 1;
			} else
				toInsert.add("");
		}

		try {
			toInsert.add(Boolean.valueOf(t.get(14, 1).value()) ? "1" : "0");
			toInsert.add(Boolean.valueOf(t.get(15, 1).value()) ? "1" : "0");
		} catch (Exception e) {
			e.printStackTrace();
		}

		StringBuilder valBuilder = new StringBuilder(size);
		boolean first = true;
		for (String s : toInsert) {
			if (first) {
				first = false;
				valBuilder.append(PlainTextExporter.escape(s));
			} else
				valBuilder.append("," + PlainTextExporter.escape(s));
		}
		PlainTextExporter.insert(conn, name, columns, valBuilder.toString());
	}
}
