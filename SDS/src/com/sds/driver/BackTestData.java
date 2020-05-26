package com.sds.driver;

import com.sds.file.*;
import com.sds.analysis.*;

public class BackTestData {
//StrategyReports_TSLA_Base
//	/home/joma/share/test/simple/
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = System.currentTimeMillis();
		String path = "/home/joma/share/test/simple/";
		String symbol = "SLS";
		BackTestBaseCVS.processStock(path, symbol);
		System.out.println("Processing teal records...");
		BackTestTealCVS.processStock(path, symbol);
		System.out.println("Processing yellow records...");
		BackTestYellowCVS.processStock(path, symbol);
		System.out.println("Processing pink records...");
		BackTestPinkCVS.processStock(path, symbol);
		System.out.println("Processing summary...");
		Summary.processStock(symbol, 0);
		System.out.println("Processing PT9...");
		PT9.processStock(symbol,0,0);
		long t2 = System.currentTimeMillis();
		System.out.println("Time cost is "+((t2-t1)*1.0f)/(1000*60.0f)+" minutes");

	}

}
