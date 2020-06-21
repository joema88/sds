package com.sds.driver;

import com.sds.db.DB;
import java.sql.*;
import java.io.*;
import java.util.*;
import com.sds.util.*;

public class WatchList {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// String query = "select a.STOCKID,SYMBOL,CLOSE, PERCENT, MARKCAP FROM BBROCK
		// a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and BT9=9 and YP10=0 and DATEID=8931
		// ORDER BY MARKCAP DESC";
		String query1 = "select SYMBOL,a.STOCKID,CLOSE, PERCENT, MARKCAP FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and DATEID=8931 ORDER BY MARKCAP DESC limit 1000";

		//export(query);
		//String query1 = "select SYMBOL,A.STOCKID, SUM(PASS),SUM(CCX), SUM(APAS) FROM BBROCK A, SYMBOLS B  WHERE A.STOCKID=B.STOCKID AND A.DATEID<=8936 AND A.DATEID>=8927  GROUP BY SYMBOL, A.STOCKID ORDER BY SUM(PASS)+SUM(APAS) DESC LIMIT 200 ";
		//printUnprocessedStocks(200, query1);
		printUnprocessedStocksBasedOnFiles(500, query1);
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

	public static void printUnprocessedStocksBasedOnFiles(int num, String query1) {
		try {
			FileWriter fr = new FileWriter("C:\\Users\\Udemy\\dockerDevOps\\test\\stocks.txt", true);
			BufferedWriter br = new BufferedWriter(fr);

			if(query1==null)
			query1 = "SELECT SYMBOL, STOCKID FROM SYMBOLS ORDER BY STOCKID ASC";
			Statement stmnt = DB.getStatement();
			Hashtable exists = BackTestFiles.processedStocks();
			

			int selectedStockCount = 0;
			ResultSet rs1 = stmnt.executeQuery(query1);
			System.out.println("----   Fresh stocks -----");
			System.out.println("                ");
			while(rs1.next()){
				String symbol = rs1.getString(1).strip();
				int stockID = rs1.getInt(2);
				if (!exists.containsKey(symbol)) {
					br.write(symbol);
					br.newLine();
					System.out.println(symbol);
					selectedStockCount++;
					
					if(selectedStockCount>=num)
						break;
				}else {
				//	System.out.println(symbol+" has been processed...");
				}
			}

			br.close();
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);

		}

	}
	public static void printUnprocessedStocks(int num, String query1) {
		try {
			FileWriter fr = new FileWriter("C:\\Users\\Udemy\\dockerDevOps\\test\\stocks.txt", true);
			BufferedWriter br = new BufferedWriter(fr);

			if(query1==null)
			query1 = "SELECT SYMBOL, STOCKID FROM SYMBOLS ORDER BY STOCKID ASC";
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
