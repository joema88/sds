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
		String symbol = "TSLA";
		BackTestBaseCVS.processStock(path, symbol);
		BackTestTealCVS.processStock(path, symbol);
		BackTestYellowCVS.processStock(path, symbol);
		Summary.processStock(symbol);
		PT9.processStock(symbol);
		long t2 = System.currentTimeMillis();
		System.out.println("Time cost is "+((t2-t1)*1.0f)/(1000*60.0f)+" minutes");

	}

}
