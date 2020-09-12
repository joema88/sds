package com.sds.file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.sds.db.*;
import java.sql.*;

import com.sds.util.DateConvertion;

public class BackTestBaseCVS  {

	private static PreparedStatement stmnt = null;
	private static PreparedStatement symbolStmnt = null;
	private static PreparedStatement dateStmnt = null;
	

	public static void init() {
		stmnt = DB.getBackTestInsertStatement();
		symbolStmnt = DB.getSymbolInsertStatement();
		dateStmnt = DB.getDateInsertStatement();
	}

	public static void main(String[] args) {
		

	}

	public static boolean checkSymbol(String symbol) {
		boolean inserted = false;
		if (!DB.checkSymbolExist(symbol)) {
			try {
				int ID = DB.getNextSymbolID();
				
				symbolStmnt.setInt(1, ID);
				symbolStmnt.setString(2, symbol);
				symbolStmnt.execute();
				Thread.sleep(10);

				inserted = true;
			} catch (Exception ex) {

			}
		}
		return inserted;
	}

	public static boolean checkDate(String dateStr) {
		boolean inserted = false;
		// java.sql.Date startDate = new java.sql.Date(dateStr);
		if (!DB.checkDateExist(dateStr)) {
			try {

				int ID = DB.getNextDateID();
				
				dateStmnt.setInt(1, ID);
				dateStmnt.setString(2, dateStr);
				dateStmnt.execute();
				Thread.sleep(10);

				inserted = true;
			} catch (Exception ex) {

			}
		}
		return inserted;
	}

	public static void processStock(String path,String symbol) {
		init();
		checkSymbol(symbol);
		//String csvFile = path+"StrategyReports_"+symbol+"_Base.csv";
		String csvFile ="/media/sf_dockerDevOps/test/simple/StrategyReports_AAPL_Base.csv";
		//                "/media/sf_dockerDevOps/test/simple/StrategyReports_﻿AAPL_Base.csv"

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";

		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			String preDate = "";
			while ((line = br.readLine()) != null) {
				if (line.indexOf("Amount;Price;") > 0) {
					start = true;
				}
				int id = 0;
				float close = 0.0f;
				String date = "";
				// use comma as separator
				if (start && line.indexOf("Buy to Open") > 0) {
					String[] data = line.split(cvsSplitBy);
					try {
						id = Integer.parseInt(data[0].strip());
					} catch (Exception ex) {
						System.out.println("ID parsing error");
						ex.printStackTrace(System.out);
					}
					try {
						close = Float.parseFloat(data[4].replaceAll(",", "").strip().substring(1));
					} catch (Exception ex) {
						System.out.println("Close parsing error");
						ex.printStackTrace(System.out);
					}
					try {
						date = data[5].strip();
					} catch (Exception ex) {
						System.out.println("Date parsing error");
						ex.printStackTrace(System.out);
					}

					if (preDate.length() > 0) {
						String dateStr = DateConvertion.convertDateFormat(preDate);
						System.out.println(id + ": " + dateStr + " : " + close);
						checkDate(dateStr);
						int dateID = DateTable.getDateID(dateStr);
						int stockID = SymbolTable.getSymbolID(symbol);
						if (!DB.checkBBRecordExist(stockID, dateID)) {
							try {
								stmnt.setInt(1, stockID);
								stmnt.setInt(2, dateID);
								stmnt.setFloat(3, close);
								stmnt.execute();
							} catch (Exception ex) {
								System.out.println("Stock record insertion failed...");
								ex.printStackTrace(System.out);
							}
						}
						preDate = date;
					} else {
						preDate = date;
					}

				}

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
