package com.sds.file;

import java.sql.PreparedStatement;

import com.sds.db.DB;

public class SymbolTable {

	public static int getSymbolID(String symbol) {
		int symbolID = 0;
		try {
			symbolID = DB.getSymbolID(symbol);
			if (symbolID == 0) {
				int nextID = DB.getNextSymbolID();
				PreparedStatement stmnt = DB.getSymbolInsertStatement();
				stmnt.setInt(1, nextID);
				stmnt.setString(2, symbol);
				stmnt.execute();
			}
		} catch (Exception ex) {
			System.out.println("Date insertion failed...");
			ex.printStackTrace(System.out);
		}

		return symbolID;
	}
}
