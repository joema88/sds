package com.sds.driver;

import com.sds.file.*;
import com.sds.analysis.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class BackTestData {
//StrategyReports_TSLA_Base
//	/home/joma/share/test/simple/
	public static void main(String[] args) {

		String path = "/home/joma/share/test/simple/";
		String symbol = "GE";

		long t1 = System.currentTimeMillis();
		
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

		long t2 = System.currentTimeMillis();
		System.out.println("Time cost is " + ((t2 - t1) * 1.0f) / (1000 * 60.0f) + " minutes");

	}

}
