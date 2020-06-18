package com.sds.driver;

import com.sds.file.*;
import com.sds.analysis.*;
import com.sds.db.DB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;
import java.sql.*;

public class BackTestBatch {
//StrategyReports_TSLA_Base
//	/home/joma/share/test/simple/
	public static void main(String[] args) {
		try {
			long t1 = System.currentTimeMillis();
			PreparedStatement chHistory = DB.checkHistoryExists();
			String file = "/home/joma/share/test/stocks.txt";
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			int totalStocks = 0;
			String lastProcessedStock = "ICDD";

			while ((line = br.readLine()) != null) {
				String symbol = line.strip();
				if (symbol.length() > 1) {
					int stockID = DB.getSymbolID(symbol);

					boolean exist = false;

					chHistory.setInt(1, stockID);
					ResultSet rs1 = chHistory.executeQuery();
					if (rs1.next()) {
						if (rs1.getInt(1) > 0) {
							exist = true;
							System.out.println("Already processed stock " + symbol);
						}else {
							System.out.println("New symbol to be processed "+symbol);
						}
					}

					if (symbol.equalsIgnoreCase(lastProcessedStock)) {
						exist = false;
					}

					String path = "/home/joma/share/test/simple/";
					// symbol = "WAFU";
					// only process those have not been processed
					// after program restart
					if (!exist && checkFilesExist(path, symbol)) {
						BackTestBaseCVS.processStock(path, symbol);
						System.out.println("Processing teal records...");
						BackTestTealCVS.processStock(path, symbol);
						System.out.println("Processing yellow records...");
						BackTestYellowCVS.processStock(path, symbol);
						System.out.println("Processing pink records...");
						BackTestPinkCVS.processStock(path, symbol);
						System.out.println("Processing CX520 records..");
						BackTestCX520CVS.processStock(path, symbol);
						System.out.println("Processing summary...");
						Summary.processStock(symbol, 0);
						System.out.println("Processing CCX History...");
						Summary.processCCXHistory(symbol, 0);
						System.out.println("Processing PT9...");
						PT9.processStockHistory(symbol, 0, 0);
						System.out.println("Processing bull pattern one ...");
						OneBullPattern.processStock(symbol, -1, -1, false);
						OneBullPattern.findPassPoints(symbol, -1, false);
						TwoBullPattern.mergeBDCXHistory(symbol, -1, false);
						TwoBullPattern.updatePTCP2History(symbol, -1, false);
						System.out.println("Processing ALTERNATIVE bull pattern one ...");
						ALTBT9.findAltBT9(symbol, -1);
						System.out.println("Process marking pass points");
						ALTBT9.markPassPoints(symbol, -1);
					}
				}

				long t2 = System.currentTimeMillis();
				System.out.println("Time cost is " + ((t2 - t1) * 1.0f) / (1000 * 60.0f) + " minutes");

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static boolean checkFilesExist(String path, String symbol) {
		boolean exists = false;
		String fullFileName1 = path + "StrategyReports_" + symbol + "_Teal.csv";
		File fileTeal = new File(fullFileName1);
		String fullFileName2 = path + "StrategyReports_" + symbol + "_Yellow.csv";
		File fileYellow = new File(fullFileName1);
		String fullFileName3 = path + "StrategyReports_" + symbol + "_Pink.csv";
		File filePink = new File(fullFileName1);
		String fullFileName4 = path + "StrategyReports_" + symbol + "_Base.csv";
		File fileBase = new File(fullFileName1);
		String fullFileName5 = path + "StrategyReports_" + symbol + "_CX520.csv";
		File fileCX520 = new File(fullFileName1);

		if (fileTeal.exists() && fileYellow.exists() && filePink.exists() && fileBase.exists() && fileCX520.exists())
			exists = true;

		return exists;
	}
}
