package com.sds.file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.sds.db.*;
import java.sql.*;

import com.sds.util.DateConvertion;

public class BackTestCX520CVS {

	private static PreparedStatement stmnt = null;

	public static void init() {
		stmnt = DB.get520CXRangeUpdate();
	}

	public static void main(String[] args) {

	}

	public static void processStock(String path, String symbol) {
		init();
		//String csvFile = path + "StrategyReports_" + symbol + "_CX520.csv";
		String csvFile ="/media/sf_dockerDevOps/test/simple/StrategyReports_CZR_CX520.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";

		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			int preDateID = 0;
			while ((line = br.readLine()) != null) {

				int id = 0;
				int cx520 = 0;
				String date = "";
				// use comma as separator
				if (start && (line.indexOf("Buy to Open") > 0||line.indexOf("Sell to Close") > 0)) {
					String[] data = line.split(cvsSplitBy);
					try {
						id = Integer.parseInt(data[0].strip());
					} catch (Exception ex) {
						System.out.println("ID parsing error");
						ex.printStackTrace(System.out);
					}
					try {
						//history start with earlier dates
						if (data[2].strip().equalsIgnoreCase("Sell to Close")) {
							//sell point, so forward is below
							cx520 = -1;
						} else if (data[2].strip().equalsIgnoreCase("Buy to Open")) {
							//buy to open, so forward is above
							cx520 = 1;
						}

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

					int dateID = DateTable.getDateID(DateConvertion.convertDateFormat(date));
					int stockID = SymbolTable.getSymbolID(symbol);

					try {
						stmnt.setInt(1, cx520);
						stmnt.setInt(2, stockID);
					//	stmnt.setInt(3, preDateID);
						stmnt.setInt(3, dateID-1);
						stmnt.executeUpdate();
						preDateID = dateID;
					} catch (Exception ex) {
						System.out.println("Stock record insertion failed...");
						ex.printStackTrace(System.out);
					}
				}

				if (line.indexOf("Amount;Price;") > 0) {
					start = true;
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
