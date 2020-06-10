package com.sds.driver;

import com.sds.db.DB;
import java.sql.*;
import java.io.*;

public class WatchList {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String query = "select a.STOCKID,SYMBOL,CLOSE, PERCENT, MARKCAP FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and BT9=9 and YP10=0 and DATEID=8931 ORDER BY MARKCAP DESC";
		export(query);
	}

	public static void export(String query) {
		try {
			FileWriter fr = new FileWriter("C:\\Users\\Udemy\\dockerDevOps\\test\\append.txt", true);
			BufferedWriter br = new BufferedWriter(fr);
			

			
			Statement stmnt = DB.getStatement();
			ResultSet rs = stmnt.executeQuery(query);
			while (rs.next()) {
				String symbol = rs.getString(2);
				br.write(symbol);
				br.newLine();
				System.out.println(symbol);
			}
			br.close();
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);

		}

	}
}
