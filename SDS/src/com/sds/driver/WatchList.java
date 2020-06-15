package com.sds.driver;

import com.sds.db.DB;
import java.sql.*;
import java.io.*;
import java.util.*;

public class WatchList {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// String query = "select a.STOCKID,SYMBOL,CLOSE, PERCENT, MARKCAP FROM BBROCK
		// a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and BT9=9 and YP10=0 and DATEID=8931
		// ORDER BY MARKCAP DESC";
		String query = "select a.STOCKID,SYMBOL,CLOSE, PERCENT, MARKCAP FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and DATEID=8931 ORDER BY b.STOCKID ASC limit 60";

		//export(query);
		printUnprocessedStocks(12);
	}

	public static void export(String query) {
		try {
			FileWriter fr = new FileWriter("C:\\Users\\Udemy\\dockerDevOps\\test\\stocks.txt", true);
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

	public static void printUnprocessedStocks(int num) {
		try {
			FileWriter fr = new FileWriter("C:\\Users\\Udemy\\dockerDevOps\\test\\stocks.txt", true);
			BufferedWriter br = new BufferedWriter(fr);

			String query1 = "SELECT SYMBOL, STOCKID FROM SYMBOLS ORDER BY STOCKID ASC";
			String query2 = "SELECT STOCKID FROM BBROCK WHERE DATEID=8900";
			Statement stmnt = DB.getStatement();
			ResultSet rs2 = stmnt.executeQuery(query2);
			Hashtable exists = new Hashtable();
			while (rs2.next()) {
				int stkId = rs2.getInt(1);
				exists.put("" + stkId, "Processed");

			}

			int selectedStockCount = 0;
			ResultSet rs1 = stmnt.executeQuery(query1);
			
			while(rs1.next()){
				String symbol = rs1.getString(1);
				int stockID = rs1.getInt(2);
				if (!exists.containsKey("" + stockID)) {
					br.write(symbol);
					br.newLine();
					System.out.println(symbol);
					selectedStockCount++;
					
					if(selectedStockCount>=num)
						break;
				}
			}

			br.close();
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);

		}

	}
}
