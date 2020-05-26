package com.sds.file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.sds.db.*;
import java.sql.*;

import com.sds.util.DateConvertion;

public class BackTestBaseCVS {

	private static PreparedStatement stmnt = null;

	public static void init() {
		stmnt = DB.getBackTestInsertStatement();
	}

	public static void main(String[] args) {
		

	}

	public static void processStock(String path,String symbol) {
		init();
		String csvFile = path+"StrategyReports_"+symbol+"_Base.csv";
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
