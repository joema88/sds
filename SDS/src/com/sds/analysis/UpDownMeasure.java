package com.sds.analysis;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.sds.db.DB;

public class UpDownMeasure {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	// private static int startDateID = 8455;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	// private static int startDateID = 8707;
	private static int startDateID = 8994; // RECALCULATE FROM 8980
	private static PreparedStatement colorRankPriceCheckStmnt = null;

	private static int eDays = 40;
	private static int tDays = 36;
	private static int daysForHold = 250; // 3 months
	private static int totalSC = 0;
	private static int totalLC = 0;
	private static int totalHC = 0;
	private static int totalCount = 0;
	private static int totalSCRandom = 0;
	private static int totalCountRandom = 0;
	private static int totalRandomHC = 0;
	private static int totalRandomLC = 0;

	private static float peakYield = 0.0f;
	private static float troughYield = 0.0f;
	private static float peakYieldRandom = 0.0f;
	private static float troughYieldRandom = 0.0f;
	private static float yieldQaulified1 = 0.5f;
	private static float yieldQaulified2 = -0.5f;
	private static boolean debug = true;
	private static Hashtable excludeStocks = null;
	private static int currentDateID = 9045;
	private static int upDays = 30; // this is used to measure 40% up days
	private static int downDays = 250; // this is used to measure 60% down days
	private static int tbkStartDateId = 8261;
	// int buyDateId = 9007; //buy date
	private static int buyDateId = 9034; // sell date, accumulator start here

	public static int[] buyDateIds = new int[4];

	public static void initBuyDateIDS() {
		// 9007,9028
		buyDateIds[0] = 9007;
		buyDateIds[1] = 9028;
		buyDateIds[2] = 9034;
		buyDateIds[3] = 9095;

		// 9091 is the sell date of last boduang
	}

	public static void main(String[] args) {

		// DAILY ROUTINE
		// currentDateID = 9045;
		initCurrentDateID();
		// processUpDownHistory();//no longer do DM update
		// daily step 1
		// processDMAHistory(); //DM update here
		// daily step 2, today only
		// processDMRankAvgDMHistory();
		// daily step 3
		// processFUCHistory();
		// daily step 4
		// Summary.processDailyUTurnSummary(currentDateID);
		// int buyDateId = 9007; //buy date

		// daily step 5
		// processTodayAllPDY(currentDateID, buyDateId,-1);
		// daily step 6
		// processTodayIndustryAVGPDY(currentDateID, -1);
		// daily step 7, update daily OBI (Over bought indicator)
		// processOBIHistory(1);
		// daily step 8, update daily f1, f8 count
		// processF18Today(currentDateID) ;
		// daily step 9, process D2, D9 for each stock
		// processD2D9History(true);
		// daily step 10, process today's VBI
		// processVBIHistory(true);
		// daily step 11, process EE8
		// processTodayEE8(currentDateID);
		// daily step 12, process IAYD
		// processTodayIndustryAVGPDYDelta(currentDateID, -1);
		// daily step 13, process BDA [(Delta of SAY)*100 + (Delta of IAYD)]
		// processTodayBDA(currentDateID, -1);
		// daily step 14, this may have to be switched with step 14 once finalized
		// processRTSHistory(true);
		// daily step 15, process last day TBK, last 30 days breakout bullish pattern
		// base on 30 days breakout mark set in step 14
		// processTBKHistory(true);
		// daily step 16, process last day AVI
		// processAVIHistory(true);
		/// daily step 17, process last day TTA
		// processTTAHistory(true);

		// two more task, 1. 15 days fresh TTA>100,
		// 2. Merrill export sort and group

		// Why TME not printed out??? TTA=128
		//printOutBullStocks(9103, 9103);

		//printOutWeeklyBullMonthlyBear(9103, 9103);

		// processStockTTAHistory(6660, false);
		// processTTAHistory(false);
		// processStockAVIHistory(297, false); //test FB AVI first
		// processAVIHistory(false);
		// process entire Rolling Thirty days Sum(P+Y) and MCP(if qualified>=90%)
		// history

		// processRTSHistory(false);
		//// process entire TBK history
		// processStockTBKHistory(963);
		// resetAllStocksTBKHistory();
		// processTBKHistory(false);
		// process entire BDA history
		// processBDAHistory();
		// calculate entire history
		// processIndustryAVGPDYDeltaHistory(-1);
		// processEE8History();
		// processStockVBIHistory(963);
		// processVBIHistory(false);
		// processD2D9History(false);
		// processPDYHistory(buyDateId);
		// processIndustryAVGPDYHistory(buyDateId);
		// processOBIHistory(320) ;

		// ROUTINE AFTER STOCK SPLIT PROCESSING...
		// After stock split, we need to download history, recalculate this
		// update recalculate stock DM, DA
		// int stockId = 1621;
		// processStockUpDownHistory(stockId);
		// processStockDMAHistory(stockId);
		// update DMRankAvg SET ADM = ?, RK = ?
		// processStockDMRankAvgDMHistory(stockId);
		// update FUC history
		// processStockFUCHistory(stockId);
		// transfer missing data

	}

	public static void printOutWeeklyBullMonthlyBear(int startDateId, int endDateId) {
		System.out.println("Processing weekly bulls, monthly bears");
		PreparedStatement weekSumBullToday = DB.weekSumBullToday();
		PreparedStatement monthlySumBearToday = DB.monthlySumBearToday();
		PreparedStatement cDate = DB.getCDate();
		int limit = 50;

		try {
			for (int k = startDateId; k <= endDateId; k++) {
				HashMap weekSumBullMap = new HashMap();
				HashMap weekSumBullPriceMap = new HashMap();
				HashMap monthlySumBearMap = new HashMap();
				HashMap monthlySumBearPriceMap = new HashMap();

				StringBuffer allBullStocksBuffer = new StringBuffer();
				StringBuffer allBullPriceBuffer = new StringBuffer();
				StringBuffer allBearStocksBuffer = new StringBuffer();
				StringBuffer allBearPriceBuffer = new StringBuffer();
				// String query = "select a.STOCKID,b.SYMBOL,ROUND(SUM(NETCHANGE),2) AS
				// SUM_NETCHANGE,ROUND(SUM(PERCENT),2) AS SUM_PERCENT,ROUND(AVG(MARKCAP),2) AS
				// AVG_MARKCAP, ROUND(AVG(CLOSE),2) AS AVG_CLOSE, MAX(CLOSE),MIN(CLOSE) FROM
				// BBROCK a, SYMBOLS b WHERE a.STOCKID=b.STOCKID and DATEID<=? and DATEID>=?
				// GROUP BY a.STOCKID, b.SYMBOL HAVING AVG(MARKCAP)>1000 AND SUM(PERCENT)>35 AND
				// AVG(CLOSE)>10 order by SUM(PERCENT) DeSC limit ?";
				weekSumBullToday.setInt(1, k);
				weekSumBullToday.setInt(2, k - 4);
				weekSumBullToday.setInt(3, limit);
				ResultSet rs1 = weekSumBullToday.executeQuery();
				int count1 = 0;
				while (rs1.next()) {
					String stock = rs1.getString(2);
					String closePrice = "" + rs1.getFloat(6);
					weekSumBullPriceMap.put(stock, closePrice);
					// System.out.println("Find stock " + stock);
					if (weekSumBullMap.containsKey(stock)) {
						int newCount = Integer.parseInt(weekSumBullMap.get(stock).toString()) + 1;
						weekSumBullMap.put(stock, "" + (newCount + 1));

					} else {
						weekSumBullMap.put(stock, "" + 1);
					}
					count1++;
				}

				if (count1 == limit) {
					System.out.println("weekSumBull today reached limit " + limit + " at dateId " + k);
				}

				// String query = "select a.STOCKID,b.SYMBOL,ROUND(SUM(NETCHANGE),2) AS
				// SUM_NETCHANGE,ROUND(SUM(PERCENT),2) AS SUM_PERCENT,ROUND(AVG(MARKCAP),2) AS
				// AVG_MARKCAP, ROUND(AVG(CLOSE),2) AS AVG_CLOSE, MAX(CLOSE),MIN(CLOSE) FROM
				// BBROCK a, SYMBOLS b WHERE a.STOCKID=b.STOCKID and DATEID<=? and DATEID>=?
				// GROUP BY a.STOCKID, b.SYMBOL HAVING AVG(MARKCAP)>1000 AND SUM(PERCENT)<-30
				// AND AVG(CLOSE)>10 order by SUM(PERCENT) ASC limit ?";
				monthlySumBearToday.setInt(1, k);
				monthlySumBearToday.setInt(2, k - 24);
				monthlySumBearToday.setInt(3, limit);
				ResultSet rs2 = monthlySumBearToday.executeQuery();
				int count2 = 0;
				while (rs2.next()) {
					String stock = rs2.getString(2);
					String closePrice = "" + rs2.getFloat(6);
					monthlySumBearPriceMap.put(stock, closePrice);
					// System.out.println("Find stock " + stock);
					if (monthlySumBearMap.containsKey(stock)) {
						int newCount = Integer.parseInt(monthlySumBearMap.get(stock).toString()) + 1;
						monthlySumBearMap.put(stock, "" + (newCount + 1));

					} else {
						monthlySumBearMap.put(stock, "" + 1);
					}
					count2++;
				}

				if (count2 == limit) {
					System.out.println("monthlySumBear today reached limit " + limit + " at dateId " + k);
				}

				// print out sorted results
				Map<String, String> sortedMap = new TreeMap<String, String>(weekSumBullMap);
				System.out.println("With weekly bull stocks..." + sortedMap.size());
				Set sortedStocks = sortedMap.keySet();
				Iterator weeklyBullITSorted = sortedStocks.iterator();
				while (weeklyBullITSorted.hasNext()) {
					String stk = weeklyBullITSorted.next().toString();
					String count = sortedMap.get(stk).toString();
					System.out.println(stk + " count " + count);
					allBullStocksBuffer.append(stk + ",");
				}

				// weeklyBullsWithPrice print out with price
				System.out.println("With price weekly bull stocks..." + weekSumBullPriceMap.size());
				Set weeklyBullsPrice = weekSumBullMap.keySet();
				Iterator weeklyBullPriceIT = weeklyBullsPrice.iterator();
				while (weeklyBullPriceIT.hasNext()) {
					String stk = weeklyBullPriceIT.next().toString();
					String close = weekSumBullPriceMap.get(stk).toString();
					allBullPriceBuffer.append(stk + " " + close + ",");
				}

				Map<String, String> sortedMap2 = new TreeMap<String, String>(monthlySumBearMap);
				System.out.println("With monthly bear stocks..." + sortedMap2.size());
				Set sortedStocks2 = sortedMap2.keySet();
				Iterator monthlyBearITSorted = sortedStocks2.iterator();
				while (monthlyBearITSorted.hasNext()) {
					String stk = monthlyBearITSorted.next().toString();
					String count = sortedMap2.get(stk).toString();
					System.out.println(stk + " count " + count);
					allBearStocksBuffer.append(stk + ",");
				}

				// weeklyBullsWithPrice print out with price
				System.out.println("With price montly bear stocks..." + monthlySumBearPriceMap.size());
				Set monthlyBearPrice = monthlySumBearPriceMap.keySet();
				Iterator monthlyBearPriceIT = monthlyBearPrice.iterator();
				while (monthlyBearPriceIT.hasNext()) {
					String stk = monthlyBearPriceIT.next().toString();
					String close = monthlySumBearPriceMap.get(stk).toString();
					allBearPriceBuffer.append(stk + " " + close + ",");
				}

				String path = "/home/joma/share/test/BBROCK/";
				cDate.setInt(1, k);
				ResultSet rsCDate = cDate.executeQuery();

				String fileName = "";
				if (rsCDate.next()) {
					fileName = rsCDate.getString(1);
				}
				if (fileName.length() < 1) {
					fileName = "Bull" + (int) Math.random() * 1000;
				}
				String fileName1 = fileName + "_WeeklyBull.txt";
				writeToFile(path + fileName1, allBullStocksBuffer);

				String fileName2 = fileName + "_WeeklyBullPrice.txt";
				writeToFile(path + fileName2, allBullPriceBuffer);

				String fileName3 = fileName + "_MonthlyBear.txt";
				writeToFile(path + fileName3, allBearStocksBuffer);

				String fileName4 = fileName + "_MonthlyBearPrice.txt";
				writeToFile(path + fileName4, allBearPriceBuffer);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void writeToFile(String fullPath, StringBuffer content) {
		try {
			File myObj = new File(fullPath);

			if (!myObj.exists())
				myObj.createNewFile();
			// if (myObj.createNewFile()) {
			Thread.sleep(5000);
			FileWriter myWriter = new FileWriter(myObj);
			myWriter.write(content.toString());
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

	public static void printOutBullStocks(int startDateId, int endDateId) {
		PreparedStatement ttaToday = DB.ttaToday();
		PreparedStatement ttaLastTenSum = DB.ttaLastTenSum();
		PreparedStatement fucToday = DB.fucToday();
		PreparedStatement tbkToday = DB.tbkToday();
		PreparedStatement vbiToday = DB.vbiToday();
		PreparedStatement ee8Today = DB.ee8Today();
		PreparedStatement cDate = DB.getCDate();
		PreparedStatement gentleBullToday = DB.gentleBullToday();
		int limitBT9 = 50;
		int limit = 100;

		try {
			for (int k = startDateId; k <= endDateId; k++) {
				HashMap ttaTodayTable = new HashMap();
				HashMap ttaLastTenSumTable = new HashMap();
				HashMap fucTodayTable = new HashMap();
				HashMap tbkTodayTable = new HashMap();
				HashMap vbiTodayTable = new HashMap();
				HashMap ee8TodayTable = new HashMap();
				HashMap gentleBullTodayTable = new HashMap();
				HashMap allStocks = new HashMap();
				HashMap allStocksWithPrice = new HashMap();

				HashMap allStocksExceptGentleBull = new HashMap();
				StringBuffer allStocksBuffer = new StringBuffer();
				StringBuffer allStocksPriceBuffer = new StringBuffer();
				StringBuffer allFreshStocksPriceBuffer = new StringBuffer();

				// ttaToday()
				// String query = "SELECT a.DATEID, a.STOCKID AS SKID, CDATE, b.SYMBOL AS SYM,
				// ROUND(CLOSE,1) AS CLOS, FUC,TBK, VBI,TTA,ROUND(SAY,1) AS SAY,MARKCAP AS
				// CAP,VOLUME,ROUND(BDY,1) AS BDY,PDY,BT9, ROUND(DPC,1) AS DPC, ROUND(UPC,1) AS
				// UPC, ROUND(DM,0) AS DM,ROUND(DD,1) AS DD, ROUND(D9,1) AS D9 FROM BBROCK a,
				// SYMBOLS b, DATES c WHERE a.STOCKID=b.STOCKID and a.DATEID = c.DATEID and
				// a.DATEID=? and TTA>0 AND DD<50 and D9<200 AND MARKCAP>1000 AND CLOSE>20 ORDER
				// by TTA DESC, MARKCAP DESC limit ?";
				ttaToday.setInt(1, k);
				ttaToday.setInt(2, limit);
				ResultSet rs1 = ttaToday.executeQuery();
				int count1 = 0;
				while (rs1.next()) {
					String stock = rs1.getString(4);
					String closePrice = "" + rs1.getFloat(5);
					allStocksWithPrice.put(stock, closePrice);
					// System.out.println("Find TTA stock " + stock);
					if (ttaTodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(ttaTodayTable.get(stock).toString()) + 1;
						ttaTodayTable.put(stock, "" + (newCount + 1));

					} else {
						ttaTodayTable.put(stock, "" + 1);
					}

					count1++;
				}
				if (count1 == limit) {
					System.out.println("TTA today reached limit " + limit);
				}

				// ttaLastTenSum()
				// String query = "select a.STOCKID, b.SYMBOL, SUM(TTA),
				// AVG(DD),AVG(D9),AVG(CLOSE) FROM
				// BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and a.DATEID>=? AND
				// a.DATEID<=? AND MARKCAP>1000 GROUP BY a.STOCKID, b.SYMBOL having SUM(TTA)>400
				// AND AVG(DD)<20 AND AVG(D9)<100 order by SUM(TTA) DESC limit ?";
				ttaLastTenSum.setInt(1, k - 9);
				ttaLastTenSum.setInt(2, k);
				ttaLastTenSum.setInt(3, limit);
				ResultSet rs2 = ttaLastTenSum.executeQuery();
				int count2 = 0;
				while (rs2.next()) {
					String stock = rs2.getString(2);
					// System.out.println("Find TTA last ten sum stock " + stock);
					String closePrice = "" + rs2.getFloat(6);
					allStocksWithPrice.put(stock, closePrice);
					if (ttaLastTenSumTable.containsKey(stock)) {
						int newCount = Integer.parseInt(ttaLastTenSumTable.get(stock).toString()) + 1;
						ttaLastTenSumTable.put(stock, "" + (newCount + 1));

					} else {
						ttaLastTenSumTable.put(stock, "" + 1);
					}
					count2++;
				}
				if (count2 == limit) {
					System.out.println("ttaLastTenSum reached limit " + limit);
				}

				// fucToday
				// String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL AS
				// SYM,RTS,MCP,TBK,TEAL AS T, YELLOW AS Y, PINK AS P,
				// MARKCAP,CLOSE,TTA,TBK,EE8,FUC, VBI,ROUND(DD,1) AS DD, ROUND(D9,0) AS D9, MOR,
				// BT9 FROM BBROCK a, SYMBOLS b,DATES c WHERE a.FUC>=4 AND a.MARKCAP>1000 AND
				// DD<30 and D9<100 AND a.STOCKID = b.STOCKID and a.DATEID=c.DATEID AND
				// a.DATEID= ? ORDER BY MARKCAP DESC limit ?";
				fucToday.setInt(1, k);
				fucToday.setInt(2, limit);
				ResultSet rs3 = fucToday.executeQuery();
				int count3 = 0;
				while (rs3.next()) {
					String stock = rs3.getString(4);
					// System.out.println("Find fuc stock " + stock);
					String closePrice = "" + rs3.getFloat(12);
					allStocksWithPrice.put(stock, closePrice);

					if (fucTodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(fucTodayTable.get(stock).toString()) + 1;
						fucTodayTable.put(stock, "" + (newCount + 1));

					} else {
						fucTodayTable.put(stock, "" + 1);
					}
					count3++;
				}
				if (count3 == limit) {
					System.out.println("fucToday reached limit " + limit);
				}

				// tbkToday
				// String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL AS
				// SYM,RTS,MCP,TBK,TEAL AS T, YELLOW AS Y, PINK AS P,
				// MARKCAP,CLOSE,TTA,TBK,EE8,FUC, VBI,ROUND(DD,1) AS DD, ROUND(D9,0) AS D9, MOR,
				// BT9 FROM BBROCK a, SYMBOLS b,DATES c WHERE a.TBK>=8 AND a.MARKCAP>1000 AND
				// DD<30 and D9<100 AND a.STOCKID = b.STOCKID and a.DATEID=c.DATEID AND
				// a.DATEID= ? ORDER BY MARKCAP DESC limit ?";
				tbkToday.setInt(1, k);
				tbkToday.setInt(2, limit);
				ResultSet rs4 = tbkToday.executeQuery();
				int count4 = 0;
				while (rs4.next()) {
					String stock = rs4.getString(4);
					// System.out.println("Find TBK stock " + stock);
					String closePrice = "" + rs4.getFloat(12);
					allStocksWithPrice.put(stock, closePrice);

					if (tbkTodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(tbkTodayTable.get(stock).toString()) + 1;
						tbkTodayTable.put(stock, "" + (newCount + 1));

					} else {
						tbkTodayTable.put(stock, "" + 1);
					}
					count4++;
				}
				if (count4 == limit) {
					System.out.println("tbkToday reached limit " + limit);
				}

				// vbiToday
				// String query = "SELECT a.DATEID, a.STOCKID AS STKID, CDATE, b.SYMBOL AS SYM,
				// MARKCAP,VOLUME,CLOSE, ROUND(DD,1) AS DD, ROUND(D9,1) AS D9,VBI,FUC, BT9,
				// ROUND(DPC,2) AS DPC, ROUND(UPC,1) AS UPC, ROUND(DM,1) AS DM FROM BBROCK a,
				// SYMBOLS b, DATES c WHERE DD>1 AND a.STOCKID=b.STOCKID and a.DATEID = c.DATEID
				// and a.DATEID =? and VBI>0 AND DD<20 AND D9<100 AND a.MARKCAP>1000 ORDER BY
				// MARKCAP DESC limit ?";
				vbiToday.setInt(1, k);
				vbiToday.setInt(2, limit);
				ResultSet rs5 = vbiToday.executeQuery();
				int count5 = 0;
				while (rs5.next()) {
					String stock = rs5.getString(4);
					// System.out.println("Find vbi stock " + stock);
					String closePrice = "" + rs5.getFloat(7);
					allStocksWithPrice.put(stock, closePrice);

					if (vbiTodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(vbiTodayTable.get(stock).toString()) + 1;
						vbiTodayTable.put(stock, "" + (newCount + 1));

					} else {
						vbiTodayTable.put(stock, "" + 1);
					}
					count5++;
				}
				if (count5 == limit) {
					System.out.println("vbiToday reached limit " + limit);
				}

				// ee8Today
				// String query = "SELECT a.DATEID, a.STOCKID AS STKID, CDATE, b.SYMBOL AS
				// SYM,EE8,VBI,FUC AS UT, MARKCAP AS CAP,VOLUME,ROUND(CLOSE,1) AS CLOS,
				// ROUND(DD,1) AS DD, ROUND(D9,1) AS D9,VBI, ROUND(BDY,1) AS
				// BDY,PDY,ROUND(DPC,1) as DPC, ROUND(UPC,1) AS UPC FROM BBROCK a, SYMBOLS b,
				// DATES c WHERE EE8>1 AND a.STOCKID=b.STOCKID and a.DATEID = c.DATEID and
				// a.DATEID =? AND DD<20 AND D9<100 and a.MARKCAP>1000 ORDER BY MARKCAP DESC
				// limit ?";
				ee8Today.setInt(1, k);
				ee8Today.setInt(2, limit);
				ResultSet rs6 = ee8Today.executeQuery();
				int count6 = 0;
				while (rs6.next()) {
					String stock = rs6.getString(4);
					// System.out.println("Find ee8 stock " + stock);
					String closePrice = "" + rs6.getFloat(10);
					allStocksWithPrice.put(stock, closePrice);

					if (ee8TodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(ee8TodayTable.get(stock).toString()) + 1;
						ee8TodayTable.put(stock, "" + (newCount + 1));

					} else {
						ee8TodayTable.put(stock, "" + 1);
					}
					count6++;
				}
				if (count6 == limit) {
					System.out.println("ee8Today reached limit " + limit);
				}

				// gentleBullToday
				// String query = "SELECT a.DATEID AS DTID, a.STOCKID AS SKID, CDATE, b.SYMBOL
				// AS SYM, BT9,ROUND(MARKCAP,0) AS CAP,VOLUME,ROUND(CLOSE,1) AS
				// CLOS,ROUND(BDY,1) AS BDY,PDY, VBI,ROUND(DD,1) AS DD, ROUND(D9,1) AS D9,
				// ROUND(DPC,1) AS DPC,ROUND(UPC,1) AS UPC, ROUND(DM,0) AS DM FROM BBROCK a,
				// SYMBOLS b, DATES c WHERE a.DATEID=? AND a.STOCKID=b.STOCKID and a.DATEID =
				// c.DATEID and a.BT9>=12 AND a.DD<100 and a.D9<300 and a.MARKCAP>1000 order by
				// a.BT9 DESC limit ?";
				gentleBullToday.setInt(1, k);
				gentleBullToday.setInt(2, limitBT9);
				ResultSet rs7 = gentleBullToday.executeQuery();
				int count7 = 0;
				while (rs7.next()) {
					String stock = rs7.getString(4);
					System.out.println("Find gentle bull stock " + stock);
					String closePrice = "" + rs7.getFloat(8);
					allStocksWithPrice.put(stock, closePrice);

					if (gentleBullTodayTable.containsKey(stock)) {
						int newCount = Integer.parseInt(gentleBullTodayTable.get(stock).toString()) + 1;
						gentleBullTodayTable.put(stock, "" + (newCount + 1));

					} else {
						gentleBullTodayTable.put(stock, "" + 1);
					}
					count7++;
				}
				if (count7 == limitBT9) {
					System.out.println("gentleBullToday reached limit " + limitBT9);
				}

				// combine all stocks
				Set ttaTodayKeys = ttaTodayTable.keySet();
				Set ttaLastTenSumKeys = ttaLastTenSumTable.keySet();
				Set fucTodayKeys = fucTodayTable.keySet();
				Set tbkTodayKeys = tbkTodayTable.keySet();
				Set vbiTodayKeys = vbiTodayTable.keySet();
				Set ee8TodayKeys = ee8TodayTable.keySet();
				Set gentleBullTodayKeys = gentleBullTodayTable.keySet();

				// loop through ttaTodayTable
				Iterator ttaIT = ttaTodayKeys.iterator();
				while (ttaIT.hasNext()) {
					String stk = ttaIT.next().toString();
					String count = ttaTodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through ttaLastTenSumTable
				Iterator ttaLastTenSumKeysIT = ttaLastTenSumKeys.iterator();
				while (ttaLastTenSumKeysIT.hasNext()) {
					String stk = ttaLastTenSumKeysIT.next().toString();
					String count = ttaLastTenSumTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through fucTodayTable
				Iterator fucTodayKeysIT = fucTodayKeys.iterator();
				while (fucTodayKeysIT.hasNext()) {
					String stk = fucTodayKeysIT.next().toString();
					String count = fucTodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through tbkTodayTable
				Iterator tbkTodayKeysIT = tbkTodayKeys.iterator();
				while (tbkTodayKeysIT.hasNext()) {
					String stk = tbkTodayKeysIT.next().toString();
					String count = tbkTodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through vbiTodayKeys
				Iterator vbiTodayKeysIT = vbiTodayKeys.iterator();
				while (vbiTodayKeysIT.hasNext()) {
					String stk = vbiTodayKeysIT.next().toString();
					String count = vbiTodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through ee8TodayTable
				Iterator ee8TodayKeysIT = ee8TodayKeys.iterator();
				while (ee8TodayKeysIT.hasNext()) {
					String stk = ee8TodayKeysIT.next().toString();
					String count = ee8TodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						allStocksExceptGentleBull.put(stk, "" + totalCount);
					}
				}

				// loop through gentleBullTodayTable
				Iterator gentleBullTodayKeysIT = gentleBullTodayKeys.iterator();
				while (ee8TodayKeysIT.hasNext()) {
					String stk = gentleBullTodayKeysIT.next().toString();
					String count = gentleBullTodayTable.get(stk).toString();
					if (!allStocks.containsKey(stk)) {
						allStocks.put(stk, count);
						// allStocksExceptGentleBull.put(stk, count);
					} else {
						String existCount = allStocks.get(stk).toString();
						int totalCount = Integer.parseInt(count) + Integer.parseInt(existCount);
						allStocks.put(stk, "" + totalCount);
						// allStocksExceptGentleBull.put(stk,""+totalCount);
					}

				}

				// print out sorted results
				Map<String, String> sortedMap = new TreeMap<String, String>(allStocks);
				System.out.println("With gentle bull stocks..." + sortedMap.size());
				Set sortedStocks = sortedMap.keySet();
				Iterator ttaITSorted = sortedStocks.iterator();
				while (ttaITSorted.hasNext()) {
					String stk = ttaITSorted.next().toString();
					String count = sortedMap.get(stk).toString();
					System.out.println(stk + " count " + count);
					allStocksBuffer.append(stk + ",");
				}

				// allStocksWithPrice print out with price
				System.out.println("With price bull stocks..." + allStocksWithPrice.size());
				Set stocksPrice = allStocksWithPrice.keySet();
				Iterator stocksPriceIT = stocksPrice.iterator();
				while (stocksPriceIT.hasNext()) {
					String stk = stocksPriceIT.next().toString();
					String close = allStocksWithPrice.get(stk).toString();
					allStocksPriceBuffer.append(stk + " " + close + ",");

					if (freshStock(stk, k)) {
						allFreshStocksPriceBuffer.append(stk + " " + close + ",");
					}
				}

				String path = "/home/joma/share/test/BBROCK/";
				cDate.setInt(1, k);
				ResultSet rsCDate = cDate.executeQuery();

				String fileName = "";
				if (rsCDate.next()) {
					fileName = rsCDate.getString(1);
				}
				if (fileName.length() < 1) {
					fileName = "Bull" + (int) Math.random() * 1000;
				}
				try {
					File myObj = new File(path + fileName + "_Bull.txt");
					System.out.println(".... " + allStocksBuffer.toString());

					if (!myObj.exists())
						myObj.createNewFile();
					// if (myObj.createNewFile()) {
					Thread.sleep(5000);
					FileWriter myWriter = new FileWriter(myObj);
					myWriter.write(allStocksBuffer.toString());
					myWriter.close();
					// System.out.println("File created: " + myObj.getName());
					// } else {
					// System.out.println("File already exists.");
					// }
				} catch (IOException e) {
					System.out.println("An error occurred.");
					e.printStackTrace();
				}

				String fileName2 = fileName + "_BullPrice.txt";
				try {
					File myObj2 = new File(path + fileName2);
					System.out.println(".... " + allStocksPriceBuffer.toString());

					if (!myObj2.exists())
						myObj2.createNewFile();
					// if (myObj.createNewFile()) {
					Thread.sleep(5000);
					FileWriter myWriter = new FileWriter(myObj2);
					myWriter.write(allStocksPriceBuffer.toString());
					myWriter.close();
					// System.out.println("File created: " + myObj.getName());
					// } else {
					// System.out.println("File already exists.");
					// }
				} catch (IOException e) {
					System.out.println("An error occurred.");
					e.printStackTrace();
				}

				String fileName3 = fileName + "_FreshTTABullPrice.txt";
				try {
					File myObj3 = new File(path + fileName3);
					System.out.println(".... " + allFreshStocksPriceBuffer.toString());

					if (!myObj3.exists())
						myObj3.createNewFile();
					// if (myObj.createNewFile()) {
					Thread.sleep(5000);
					FileWriter myWriter = new FileWriter(myObj3);
					myWriter.write(allFreshStocksPriceBuffer.toString());
					myWriter.close();
					// System.out.println("File created: " + myObj.getName());
					// } else {
					// System.out.println("File already exists.");
					// }
				} catch (IOException e) {
					System.out.println("An error occurred.");
					e.printStackTrace();
				}

				Map<String, String> sortedMap2 = new TreeMap<String, String>(allStocksExceptGentleBull);
				System.out.println("Without gentle bull stocks..." + sortedMap2.size());
				Set sortedStocks2 = sortedMap2.keySet();
				Iterator ttaITSorted2 = sortedStocks2.iterator();
				while (ttaITSorted2.hasNext()) {
					String stk8 = ttaITSorted2.next().toString();
					String count = sortedMap2.get(stk8).toString();
					System.out.println(stk8 + " count " + count);

				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	// fresh TTA>100 in 15 days
	public static boolean freshStock(String stock, int dateID) {
		boolean fresh = false;
		boolean cond1 = false;
		boolean cond2 = false;

		try {
			// in last 16 days, if only the latest day has TTA>100, then considered as fresh
			// TTA case
			//// String query = "select count(*) FROM BBROCK a, SYMBOLS b WHERE a.STOCKID =
			//// b.STOCKID and b.SYMBOL= ? and a.DATEID>=? and a.DATEID<=? and TTA>100";
			PreparedStatement ttaCount = DB.getTTACount();
			ttaCount.setString(1, stock);
			ttaCount.setInt(2, dateID);
			ttaCount.setInt(3, dateID);

			ResultSet rs1 = ttaCount.executeQuery();

			if (rs1.next()) {
				int ttaNum = rs1.getInt(1);
				if (ttaNum == 1) {
					cond1 = true;
				}
			}

			if (cond1) {

				ttaCount.setString(1, stock);
				ttaCount.setInt(2, dateID - 16);
				ttaCount.setInt(3, dateID - 1);

				ResultSet rs2 = ttaCount.executeQuery();

				if (rs2.next()) {
					int ttaNum = rs2.getInt(1);
					if (ttaNum == 0) {
						cond2 = true;
						fresh = true;
					}
				}

			}

		} catch (Exception ex) {

		}
		return fresh;
	}

	public static void processStockUpDownHistory(int stockID) {
		try {

			excludeStocks = new Hashtable();

			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement getDateIDByPrice = DB.getDateIDbyPrice();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement resetUpDownToZero = DB.resetUpDownToZero();

			colorRankPriceCheckStmnt = DB.checkRankPriceStmnt();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement priceByDateID = DB.getPriceByDateID();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			for (int k = endDateId; k >= strtDateId; k--) {

				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean exist = false;
				int adjustment = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}

				} while (!exist);

				try {
					priceByDateID.setInt(1, stockID);
					priceByDateID.setInt(2, k);

					System.out.println(stockID + " " + k);
					ResultSet cPriceStmnt = priceByDateID.executeQuery();

					cPriceStmnt.next();
					float cPrice = cPriceStmnt.getFloat(1);

					dateIDStmnt.setInt(1, stockID);
					dateIDStmnt.setInt(2, k - downDays - 20);
					dateIDStmnt.setInt(3, k);

					ResultSet dateIDCount = dateIDStmnt.executeQuery();

					int dateIdStartUp = 0;
					int dateIdStartDown = 0;
					int dateIdStart = 0;
					int count = 0;
					while (dateIDCount.next()) {
						dateIdStart = dateIDCount.getInt(1);
						count++;
						if (count == upDays) {// 30days
							dateIdStartUp = dateIdStart;
						}
						if (count == downDays) { // 250days
							dateIdStartDown = dateIdStart;
							break;
						}
					}

					maxClose.setInt(1, stockID);
					maxClose.setInt(2, dateIdStartDown);
					maxClose.setInt(3, k);
					System.out.println("dateIdStartDown " + dateIdStartDown + " k " + k);
					ResultSet xpRS = maxClose.executeQuery();

					int xDateID = 0;
					float maxDrop = 0.0f;
					int tempDateId = 0;
					float tempDrop = 0.0f;
					float temMaxPrice = 0.0f;
					float maxPrice = 0.0f;
					while (xpRS.next()) {
						tempDateId = xpRS.getInt(1);
						temMaxPrice = xpRS.getFloat(2);
						tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
						if (xDateID == 0) { // get the last year biggest price drop regardless
							xDateID = tempDateId;
							maxDrop = tempDrop;
							maxPrice = temMaxPrice;
						} else if (tempDrop < -60.0f && tempDateId > xDateID) {
							// essentially we want to find the closest point to current
							// which has a 60% price drop
							xDateID = tempDateId;
							maxDrop = tempDrop;
							maxPrice = temMaxPrice;
						}
					}

					minClose.setInt(1, stockID);
					minClose.setInt(2, dateIdStartUp);
					minClose.setInt(3, k);

					ResultSet mpRS = minClose.executeQuery();

					mpRS.next();

					float minPrice = mpRS.getFloat(1);
					int mDateID = mpRS.getInt(2);
					mpRS.close();
					mpRS = null;

					float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
					int maxDistance = k - xDateID;
					if (maxDistance > downDays)
						maxDistance = downDays;
					int minDistsance = k - mDateID;
					if (minDistsance > upDays)
						minDistsance = upDays;

					// this measure what it feels like if you invest fixed amount of money
					// at the top and the bottom average loss
					// let us assume invest $1000 at each point at maxPrice and minPrice
					float shares = 1000.0f / maxPrice + 1000.0f / minPrice;
					float totalWorth = shares * cPrice;
					float changePercentage = 100.0f * (totalWorth - 2000.0f) / 2000.0f;
					int changeDays = maxDistance - minDistsance;
					if (changeDays < 0)
						changeDays = minDistsance - maxDistance;

					// "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?, DM=?,
					// DA=? WHERE STOCKID = ? AND DATEID = ?";

					// String query = "UPDATE BBROCK SET UPC = ?, UDS = ?,
					// DPC = ?, DDS= ? WHERE STOCKID = ? AND DATEID = ?";

					distanceChangeUpdate.setFloat(1, upFromMin);
					if (minDistsance > 255) {
						distanceChangeUpdate.setInt(2, 255);
					} else {
						distanceChangeUpdate.setInt(2, minDistsance);
					}
					distanceChangeUpdate.setFloat(3, maxDrop);
					if (maxDistance > 255) {
						distanceChangeUpdate.setInt(4, 255);
					} else {
						distanceChangeUpdate.setInt(4, maxDistance);
					}
					distanceChangeUpdate.setInt(5, stockID);
					distanceChangeUpdate.setInt(6, k);
					System.out.println("k " + k + " changeDays " + changeDays + " changePercentage " + changePercentage
							+ " upFromMin " + upFromMin + " minDistsance " + minDistsance + " maxDrop " + maxDrop);
					System.out.println("changeDays " + changeDays + " minDistsance" + minDistsance + ", maxDistance  "
							+ maxDistance);

					distanceChangeUpdate.executeUpdate();
				} catch (Exception ex) {
					ex.printStackTrace(System.out);
				}

			}
			System.out.println(stockID + " processed done");
			Thread.sleep(2000);
			long t2 = System.currentTimeMillis();

			long t3 = System.currentTimeMillis();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processF18History(int length) {
		try {
			PreparedStatement f1UpdateStmnt = DB.f1UpdateStmnt();
			PreparedStatement f8UpdateStmnt = DB.f8UpdateStmnt();
			PreparedStatement fucStmnt = DB.getFUCHistoryStmnt();
			// String query = "select a.DATEID,b.CDATE, COUNT(*) FROM BBROCK a, DATES b
			// WHERE a.DATEID=b.DATEID and FUC>? GROUP BY DATEID ORDER BY DATEID DESC limit
			// ?;";

			fucStmnt.setInt(1, 0);
			fucStmnt.setInt(2, length);
			ResultSet rs1 = fucStmnt.executeQuery();
			while (rs1.next()) {
				int dateId = rs1.getInt(1);
				int f1Count = rs1.getInt(3);
				f1UpdateStmnt.setInt(1, f1Count);
				f1UpdateStmnt.setInt(2, dateId);
				f1UpdateStmnt.executeUpdate();
				System.out.println(dateId + " " + f1Count);

			}

			fucStmnt.setInt(1, 4);
			fucStmnt.setInt(2, length);
			ResultSet rs2 = fucStmnt.executeQuery();
			while (rs2.next()) {
				int dateId = rs2.getInt(1);
				int f1Count = rs2.getInt(3);
				f8UpdateStmnt.setInt(1, f1Count);
				f8UpdateStmnt.setInt(2, dateId);
				f8UpdateStmnt.executeUpdate();
				System.out.println(dateId + " " + f1Count);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processF18Today(int dateId) {
		try {
			PreparedStatement f1UpdateStmnt = DB.f1UpdateStmnt();
			PreparedStatement f8UpdateStmnt = DB.f8UpdateStmnt();
			PreparedStatement fucStmnt = DB.getFUCTodayStmnt();
			// String query = "select a.DATEID,b.CDATE, COUNT(*) FROM BBROCK a, DATES b
			// WHERE a.DATEID=b.DATEID and FUC>? GROUP BY DATEID ORDER BY DATEID DESC limit
			// ?;";
			fucStmnt.setInt(1, 0);
			fucStmnt.setInt(2, dateId);
			ResultSet rs1 = fucStmnt.executeQuery();
			while (rs1.next()) {
				// int dateId = rs1.getInt(1);
				int f1Count = rs1.getInt(3);
				f1UpdateStmnt.setInt(1, f1Count);
				f1UpdateStmnt.setInt(2, dateId);
				f1UpdateStmnt.executeUpdate();

			}

			fucStmnt.setInt(1, 4);
			fucStmnt.setInt(2, dateId);
			ResultSet rs2 = fucStmnt.executeQuery();
			while (rs2.next()) {
				// int dateId = rs2.getInt(1);
				int f1Count = rs2.getInt(3);
				f8UpdateStmnt.setInt(1, f1Count);
				f8UpdateStmnt.setInt(2, dateId);
				f8UpdateStmnt.executeUpdate();

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("processF18Today done");
	}

	public static void processUpDownHistory() {
		try {

			excludeStocks = new Hashtable();
			excludeStocks.put("896", "896");
			excludeStocks.put("4680", "4680");
			excludeStocks.put("4418", "4418");
			excludeStocks.put("4692", "4692");
			excludeStocks.put("4113", "4113");
			excludeStocks.put("4704", "4704");
			excludeStocks.put("4062", "4062");
			excludeStocks.put("4034", "4034");
			excludeStocks.put("4702", "4702");
			excludeStocks.put("3116", "3116");
			excludeStocks.put("2631", "2631");// okay, 2631
			excludeStocks.put("4691", "4691");
			excludeStocks.put("4262", "4262");
			excludeStocks.put("4264", "4264");
			excludeStocks.put("4945", "4945");
			excludeStocks.put("4487", "4487");
			excludeStocks.put("4529", "4529");
			excludeStocks.put("4533", "4533");
			excludeStocks.put("4551", "4551");
			excludeStocks.put("4740", "4740");
			excludeStocks.put("5196", "5196");
			excludeStocks.put("4584", "4584");
			excludeStocks.put("4635", "4635");
			excludeStocks.put("4949", "4949");
			excludeStocks.put("4892", "4892");
			excludeStocks.put("4667", "4667");
			excludeStocks.put("5991", "5991");
			excludeStocks.put("4862", "4862");
			excludeStocks.put("4690", "4690");
			excludeStocks.put("5238", "5238");
			excludeStocks.put("5238", "5238");
			excludeStocks.put("5158", "5158");
			excludeStocks.put("4683", "4683");
			excludeStocks.put("4758", "4758");
			excludeStocks.put("4763", "4763");
			excludeStocks.put("5881", "5881");
			excludeStocks.put("5376", "5376");
			excludeStocks.put("5488", "5488");
			// add WIMI, 3775 all zero case to bull, to new to caluculate

			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement getDateIDByPrice = DB.getDateIDbyPrice();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement resetUpDownToZero = DB.resetUpDownToZero();

			colorRankPriceCheckStmnt = DB.checkRankPriceStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement priceByDateID = DB.getPriceByDateID();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				boolean ignore = false;
				if (endDateId < currentDateID) {
					resetUpDownToZero.setInt(1, stockID);
					resetUpDownToZero.executeUpdate();
					ignore = true;
				}
				// if (!ignore||!excludeStocks.containsKey("" + stockID))
				if (!ignore)
					for (int k = currentDateID; k >= strtDateId; k--) {

						// for (int k = currentDateID; k >= currentDateID; k--) {
						boolean exist = false;
						int adjustment = 0;
						do {
							dateIDExistStmnt.setInt(1, stockID);
							dateIDExistStmnt.setInt(2, k);

							ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

							dateIDExist.next();

							int count = dateIDExist.getInt(1);

							if (count > 0) {
								exist = true;
							} else {
								k--;
								adjustment++;
							}

						} while (!exist);

						try {
							priceByDateID.setInt(1, stockID);
							priceByDateID.setInt(2, k);

							System.out.println(stockID + " " + k);
							ResultSet cPriceStmnt = priceByDateID.executeQuery();

							cPriceStmnt.next();
							float cPrice = cPriceStmnt.getFloat(1);

							dateIDStmnt.setInt(1, stockID);
							dateIDStmnt.setInt(2, k - downDays - 20);
							dateIDStmnt.setInt(3, k);

							ResultSet dateIDCount = dateIDStmnt.executeQuery();

							int dateIdStartUp = 0;
							int dateIdStartDown = 0;
							int dateIdStart = 0;
							int count = 0;
							while (dateIDCount.next()) {
								dateIdStart = dateIDCount.getInt(1);
								count++;
								if (count == upDays) {// 30days
									dateIdStartUp = dateIdStart;
								}
								if (count == downDays) { // 250days
									dateIdStartDown = dateIdStart;
									break;
								}
							}

							maxClose.setInt(1, stockID);
							maxClose.setInt(2, dateIdStartDown);
							maxClose.setInt(3, k);
							System.out.println("dateIdStartDown " + dateIdStartDown + " k " + k);
							ResultSet xpRS = maxClose.executeQuery();

							int xDateID = 0;
							float maxDrop = 0.0f;
							int tempDateId = 0;
							float tempDrop = 0.0f;
							float temMaxPrice = 0.0f;
							float maxPrice = 0.0f;
							while (xpRS.next()) {
								tempDateId = xpRS.getInt(1);
								temMaxPrice = xpRS.getFloat(2);
								tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
								if (xDateID == 0) { // get the last year biggest price drop regardless
									xDateID = tempDateId;
									maxDrop = tempDrop;
									maxPrice = temMaxPrice;
								} else if (tempDrop < -60.0f && tempDateId > xDateID) {
									// essentially we want to find the closest point to current
									// which has a 60% price drop
									xDateID = tempDateId;
									maxDrop = tempDrop;
									maxPrice = temMaxPrice;
								}
							}

							minClose.setInt(1, stockID);
							minClose.setInt(2, dateIdStartUp);
							minClose.setInt(3, k);

							ResultSet mpRS = minClose.executeQuery();

							mpRS.next();

							float minPrice = mpRS.getFloat(1);
							int mDateID = mpRS.getInt(2);
							mpRS.close();
							mpRS = null;

							float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
							int maxDistance = k - xDateID;
							if (maxDistance > downDays)
								maxDistance = downDays;
							int minDistsance = k - mDateID;
							if (minDistsance > upDays)
								minDistsance = upDays;

							// this measure what it feels like if you invest fixed amount of money
							// at the top and the bottom average loss
							// let us assume invest $1000 at each point at maxPrice and minPrice
							float shares = 1000.0f / maxPrice + 1000.0f / minPrice;
							float totalWorth = shares * cPrice;
							float changePercentage = 100.0f * (totalWorth - 2000.0f) / 2000.0f;
							int changeDays = maxDistance - minDistsance;
							if (changeDays < 0)
								changeDays = minDistsance - maxDistance;

							// "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?, DM=?,
							// DA=? WHERE STOCKID = ? AND DATEID = ?";
							distanceChangeUpdate.setFloat(1, upFromMin);
							if (minDistsance > 255) {
								distanceChangeUpdate.setInt(2, 255);
							} else {
								distanceChangeUpdate.setInt(2, minDistsance);
							}
							distanceChangeUpdate.setFloat(3, maxDrop);
							if (maxDistance > 255) {
								distanceChangeUpdate.setInt(4, 255);
							} else {
								distanceChangeUpdate.setInt(4, maxDistance);
							}
							distanceChangeUpdate.setInt(5, stockID);
							distanceChangeUpdate.setInt(6, k);
							System.out.println("k " + k + " changeDays " + changeDays + " changePercentage "
									+ changePercentage + " upFromMin " + upFromMin + " minDistsance " + minDistsance
									+ " maxDrop " + maxDrop);
							System.out.println("changeDays " + changeDays + " minDistsance" + minDistsance
									+ ", maxDistance  " + maxDistance);

							distanceChangeUpdate.executeUpdate();
						} catch (Exception ex) {
							ex.printStackTrace(System.out);
						}

					}
				System.out.println(stockID + " processed done");
				Thread.sleep(2000);
				long t2 = System.currentTimeMillis();
				if (sc % 100 == 0)
					System.out.println(sc + " total stocks processed, time cost " + (t2 - t1) / (1000 * 60));
				Thread.sleep(100);
			}
			long t3 = System.currentTimeMillis();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayUpDown(int stockID, int dateId) {
		try {
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement priceByDateID = DB.getPriceByDateID();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;
			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;
				if (count == upDays) {// 30days
					dateIdStartUp = dateIdStart;
				}
				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			priceByDateID.setInt(1, stockID);
			priceByDateID.setInt(2, dateId);

			System.out.println(stockID + " " + dateId);
			ResultSet cPriceStmnt = priceByDateID.executeQuery();

			cPriceStmnt.next();
			float cPrice = cPriceStmnt.getFloat(1);

			maxClose.setInt(1, stockID);
			maxClose.setInt(2, dateIdStartDown);
			maxClose.setInt(3, dateId);
			System.out.println("dateIdStartDown " + dateIdStartDown + " dateId " + dateId);
			ResultSet xpRS = maxClose.executeQuery();

			int xDateID = 0;
			float maxDrop = 0.0f;
			int tempDateId = 0;
			float tempDrop = 0.0f;
			float temMaxPrice = 0.0f;
			float maxPrice = 0.0f;
			while (xpRS.next()) {
				tempDateId = xpRS.getInt(1);
				temMaxPrice = xpRS.getFloat(2);
				tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
				if (xDateID == 0) { // get the last year biggest price drop regardless
					xDateID = tempDateId;
					maxDrop = tempDrop;
					maxPrice = temMaxPrice;
				} else if (tempDrop < -60.0f && tempDateId > xDateID) {
					// essentially we want to find the closest point to current
					// which has a 60% price drop
					xDateID = tempDateId;
					maxDrop = tempDrop;
					maxPrice = temMaxPrice;
				}
			}

			minClose.setInt(1, stockID);
			minClose.setInt(2, dateIdStartUp);
			minClose.setInt(3, dateId);

			ResultSet mpRS = minClose.executeQuery();

			mpRS.next();

			float minPrice = mpRS.getFloat(1);
			int mDateID = mpRS.getInt(2);
			mpRS.close();
			mpRS = null;

			float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
			int maxDistance = dateId - xDateID;
			if (maxDistance > downDays)
				maxDistance = downDays;
			int minDistsance = dateId - mDateID;
			if (minDistsance > upDays)
				minDistsance = upDays;

			// String query = "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?
			// WHERE STOCKID = ? AND DATEID = ?";

			distanceChangeUpdate.setFloat(1, upFromMin);
			if (minDistsance > 255) {
				distanceChangeUpdate.setInt(2, 255);
			} else {
				distanceChangeUpdate.setInt(2, minDistsance);
			}
			distanceChangeUpdate.setFloat(3, maxDrop);
			if (maxDistance > 255) {
				distanceChangeUpdate.setInt(4, 255);
			} else {
				distanceChangeUpdate.setInt(4, maxDistance);
			}

			distanceChangeUpdate.setInt(5, stockID);
			distanceChangeUpdate.setInt(6, dateId);

			distanceChangeUpdate.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void checkYield(PreparedStatement dailyPrice, int dateID, int holdDays, int stockId) {
		try {
			dailyPrice.setInt(1, stockId);
			dailyPrice.setInt(2, dateID);
			dailyPrice.setInt(3, dateID + holdDays);

			ResultSet rs = dailyPrice.executeQuery();

			float startPrice = 0.0f;
			float minPrice = 0.0f;
			float maxPrice = 0.0f;
			int minDays = 0;
			int maxDays = 0;

			int count = 0;
			boolean exist = false;
			while (rs.next()) {
				float price = rs.getFloat(1);
				int dateId = rs.getInt(2);
				if (startPrice < 0.001f) {
					startPrice = price;
					minPrice = price;
					maxPrice = price;
				}

				if (price > maxPrice) {
					maxPrice = price;
					maxDays = count;
					exist = true;
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
					exist = true;
				}

				count++;
			}

			if (exist) {
				float pYield = (maxPrice - startPrice) / startPrice;
				float tYield = (minPrice - startPrice) / startPrice;
				if (pYield > yieldQaulified1) {
					totalHC++;
					totalSC++;
					if (pYield > 3.0f)
						System.out.println("Find suspected high yield " + pYield + " for stock " + stockId);
					if (debug)
						System.out.println("Find up qualified yield " + pYield + " for stock " + stockId);
				}

				if (pYield < yieldQaulified2) {
					totalSC++;
					totalLC++;
					if (pYield < -0.8f)
						System.out.println("Find suspected low yield " + pYield + " for stock " + stockId);

					if (debug)
						System.out.println("Find down qualified yield " + pYield + " for stock " + stockId);
				}

				peakYield = peakYield + pYield;
				troughYield = troughYield + tYield;
				totalCount++;
				if (debug) {
					System.out.println("Average peak yield " + 100.0f * peakYield / totalCount);
					System.out.println(
							"TotalCount " + totalCount + " peakYield " + peakYield + " troughYield " + troughYield);
					System.out.println("Average trough yield " + 100.0f * troughYield / totalCount);
					System.out.println("Success rate " + totalSC + "/" + totalCount);
				}

				int compareStockID = 0;
				do {
					compareStockID = DB.getParallelStock(dateID, stockId);
				} while (excludeStocks.containsKey("" + compareStockID));

				checkRandomYield(dailyPrice, dateID, holdDays, compareStockID);
			}

		} catch (Exception ex) {

		}
	}

	public static void checkRandomYield(PreparedStatement dailyPrice, int dateID, int holdDays, int stockId) {
		try {
			dailyPrice.setInt(1, stockId);
			dailyPrice.setInt(2, dateID);
			dailyPrice.setInt(3, dateID + holdDays);

			ResultSet rs = dailyPrice.executeQuery();

			float startPrice = 0.0f;
			float minPrice = 0.0f;
			float maxPrice = 0.0f;
			int minDays = 0;
			int maxDays = 0;

			int count = 0;
			boolean exist = false;

			while (rs.next()) {
				float price = rs.getFloat(1);
				int dateId = rs.getInt(2);
				if (startPrice < 0.001f) {
					startPrice = price;
					minPrice = price;
					maxPrice = price;
				}

				if (price > maxPrice) {
					maxPrice = price;
					maxDays = count;
					exist = true;
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
					exist = true;
				}

				count++;
			}

			if (exist) {
				float pYield = (maxPrice - startPrice) / startPrice;
				float tYield = (minPrice - startPrice) / startPrice;
				if (pYield > yieldQaulified1) {
					totalSCRandom++;
					totalRandomHC++;
					if (pYield > 3.0f)
						System.out.println("Find suspected random high yield " + pYield + " for stock " + stockId);

				}

				if (pYield < yieldQaulified2) {
					totalSCRandom++;
					totalRandomLC++;
					if (pYield < -0.8f)
						System.out.println("Find suspected random low yield " + pYield + " for stock " + stockId);

				}
				peakYieldRandom = peakYieldRandom + pYield;
				troughYieldRandom = troughYieldRandom + tYield;
				totalCountRandom++;

				if (debug) {
					System.out.println("Average peak random yield " + 100.0f * peakYieldRandom / totalCountRandom);
					System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);

					System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);
				}
			}
		} catch (Exception ex) {

		}

	}

	public static void processTodayFUC(int stockID, int dateId) {
		try {
			if (stockID == 2 && dateId == 8595) {
				System.out.println("Testing...");
			}
			PreparedStatement fud = DB.getFUD();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement updateFUC = DB.updateFUC();
			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - upDays - 10);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == upDays) { // 30days
					dateIdStartUp = dateIdStart;
					break;
				}
			}

			fud.setInt(1, stockID);
			fud.setInt(2, dateIdStartUp);
			fud.setInt(3, dateId);
			ResultSet fuds = fud.executeQuery();

			boolean firstFty = false;
			boolean dmg100 = false;
			// String query = "select UPC,DM,FUC, DATEID FROM BBROCK WHERE STOCKID = ?
			// AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";

			int tc = 0;
			int fucValue = 0;
			while (fuds.next()) {
				float upc = fuds.getFloat(1);
				float dm = fuds.getFloat(2);
				int fuc = fuds.getInt(3);
				int date = fuds.getInt(4);
				tc++;
				if (tc == 1 && dm > 100.0f && upc > 40.0f) {
					fucValue = 8;
				} else if (tc == 1 && upc > 40.0f && dm < 100.0f) {
					fucValue = 4;
				}

				if (((tc > 1 && fuc >= 1) || (tc > 1 && upc >= 40.0f)) && fucValue > 1) {
					if (fuc == 8 && fucValue == 8) {
						fucValue = 1;
					} else if (fuc < 8 && fucValue == 8) {
						fucValue = 8;
					} else if (fuc < 8 && fucValue < 8) {
						fucValue = 1;
					}
				}
			}

			// if else
			updateFUC.setInt(1, fucValue);
			updateFUC.setInt(2, stockID);
			updateFUC.setInt(3, dateId);
			updateFUC.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTodayDMA(int stockID, int dateId) {
		try {
			// String query = "select DPC, DATEID FROM BBROCK WHERE STOCKID = ?
			// AND DATEID>=? AND DATEID<=? ORDER BY DPC ASC LIMIT 1";

			PreparedStatement minDPC = DB.getMinDPC();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement currentUPC = DB.getCurrentUPC();
			PreparedStatement updateMaxUpDown = DB.updateMaxUpDown();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			currentUPC.setInt(1, stockID);
			currentUPC.setInt(2, dateId);

			System.out.println(stockID + " " + dateId);
			ResultSet cUPCStmnt = currentUPC.executeQuery();

			cUPCStmnt.next();
			float cUPC = cUPCStmnt.getFloat(1);

			minDPC.setInt(1, stockID);
			minDPC.setInt(2, dateIdStartDown);
			minDPC.setInt(3, dateId);
			ResultSet xpRS = minDPC.executeQuery();

			xpRS.next();

			float mDPC = xpRS.getFloat(1);
			int mDateID = xpRS.getInt(2);

			float MD = cUPC - mDPC;
			int MA = dateId - mDateID;

			if (dateId == 8454) {
				updateMaxUpDown.setFloat(1, 0);
			} else {
				updateMaxUpDown.setFloat(1, MD);
			}
			if (MA > 250)
				MA = 250;
			updateMaxUpDown.setInt(2, MA);
			updateMaxUpDown.setInt(3, stockID);
			updateMaxUpDown.setInt(4, dateId);
			updateMaxUpDown.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}
	// private static PreparedStatement dmRank = null;
	// private static PreparedStatement avgDM = null;

	public static void processStockDMRankAvgDMHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			System.out.println("-----------Begin---------");

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			for (int k = endDateId; k >= strtDateId + downDays + 20; k--) {
				// for (int k = currentDateID; k >= 8979; k--) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean exist = false;
				int adjustment = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}

				} while (!exist);

				processTodayDMRankAvgDM(stockID, k);
			}

			System.out.println("process TodayDMRankAvgDM done for " + stockID);
			Thread.sleep(2000);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processDMRankAvgDMHistory() {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = endDateId; k >= strtDateId; k--) {
				// for (int k = currentDateID; k >= 8979; k--) {
				for (int k = currentDateID; k >= currentDateID; k--) {
					boolean exist = false;
					int adjustment = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}

					} while (!exist);

					processTodayDMRankAvgDM(stockID, k);
				}

				System.out.println("process TodayDMRankAvgDM done for " + stockID);
				// Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayDMRankAvgDM(int stockID, int dateId) {
		try {
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement avgDM = DB.getAvgDMStmnt();
			PreparedStatement rankDM = DB.getDMRankStmnt();
			PreparedStatement updateDMRankAvg = DB.getUpdateDMRankAVGStmnt();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			avgDM.setInt(1, stockID);
			avgDM.setInt(2, dateIdStartDown);
			avgDM.setInt(3, dateId);
			ResultSet xpRS = avgDM.executeQuery();

			xpRS.next();

			float dmAVG = xpRS.getFloat(1);

			rankDM.setInt(1, stockID);
			rankDM.setInt(2, dateIdStartDown);
			rankDM.setInt(3, dateId);

			ResultSet rankRS = rankDM.executeQuery();

			int rank = 0;
			while (rankRS.next()) {
				rank++;
				if (rankRS.getInt(1) == dateId) {
					break;
				}

			}

			updateDMRankAvg.setFloat(1, dmAVG);
			if (rank > 250)
				rank = 250;
			updateDMRankAvg.setInt(2, rank);
			updateDMRankAvg.setInt(3, stockID);
			updateDMRankAvg.setInt(4, dateId);
			updateDMRankAvg.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockDMAHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// for (int k = endDateId; k >= strtDateId; k--) {
			for (int k = endDateId; k >= strtDateId + downDays + 20; k--) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean exist = false;
				int adjustment = 0;
				int lcMax = 10;
				int lc = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist)
					processTodayDMA(stockID, k);
			}

			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processD2D9History(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// 8923)
			// for (int k = endDateId; k >= strtDateId; k--) {
			// for (int k = endDateId; k >= strtDateId; k--) {
			for (int k = endDateId; k >= 8923; k--) { // we only have info after 8923
				boolean exist = false;
				int adjustment = 0;
				int lcMax = 10;
				int lc = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist)
					processD2Today(stockID, k);
			}

			for (int k = endDateId; k >= strtDateId; k--) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean exist = false;
				int adjustment = 0;
				int lcMax = 10;
				int lc = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist)
					processD9Today(stockID, k);
			}
			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processD2Today(int stockID, int dateId) {
		try {

			// "select CLOSE, DATEID, VOLUME,MARKCAP, D2 from BBROCK WHERE STOCKID=? and
			// DATEID>=?
			// and DATEID<=? ORDER BY DATEID ASC";
			PreparedStatement dailyPrice = DB.getDailyPrice();
			// String query = "UPDATE BBROCK SET D2=? WHERE STOCKID=? and DATEID=?";
			PreparedStatement updateD2 = DB.updateD2();

			dailyPrice.setInt(1, stockID);
			dailyPrice.setInt(2, dateId);
			dailyPrice.setInt(3, dateId);

			ResultSet rs = dailyPrice.executeQuery();

			if (rs.next()) {
				float close = rs.getFloat(1);
				float vol = rs.getFloat(3);
				float markcap = rs.getFloat(4);
				if (markcap > 0.0001 && vol > 1.0f && close > 0.01f) {
					float d2 = 1000000.0f * markcap / (vol * close);
					updateD2.setFloat(1, d2);
					updateD2.setInt(2, stockID);
					updateD2.setInt(3, dateId);
					updateD2.executeUpdate();

				}
			}

		} catch (Exception ex) {

		}
	}

	public static void processD9Today(int stockID, int dateId) {
		try {

			// String query = "select AVG(DD) from BBROCK WHERE
			// STOCKID=? and DATEID>=? and DATEID<=?";

			PreparedStatement avgD2 = DB.getAvgD2();
			// String query = "UPDATE BBROCK SET D9=? WHERE STOCKID=? and DATEID=?";
			PreparedStatement updateD9 = DB.updateD9();

			avgD2.setInt(1, stockID);
			avgD2.setInt(2, dateId - 9);
			avgD2.setInt(3, dateId);

			ResultSet rs = avgD2.executeQuery();

			if (rs.next()) {
				float d9 = rs.getFloat(1);

				updateD9.setFloat(1, d9);
				updateD9.setInt(2, stockID);
				updateD9.setInt(3, dateId);
				updateD9.executeUpdate();

			}

		} catch (Exception ex) {

		}
	}

	public static void processStockVBIHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// 8923 is the starting date with volume and marketcap
			// need 12 bottom to avoid end error
			for (int k = endDateId; k >= 8935; k--) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean exist = false;
				int adjustment = 0;
				int lcMax = 10;
				int lc = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k--;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist) {
					if (k == 8986) {
						System.out.println("K is " + k);
					}
					processVBIToday(stockID, k);
				}
			}

			System.out.println("VBI process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processVBIHistory(boolean lastOnly) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				if (stockID == 48) {
					System.out.println("AMZN");
				}

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// 8923 is the starting date with volume and marketcap
				// need 12 bottom to avoid end error
				for (int k = endDateId; k >= 8935; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist) {
						processVBIToday(stockID, k);
						if (lastOnly)
							break;
					}
				}

				System.out.println("VBI process done for " + stockID);
			}

		} catch (

		Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processVBIToday(int stockID, int dateId) {
		try {

			// String query = "select DD, D9 FROM BBROCK
			// WHERE STOCKID =? and DATEID<=? AND DATEID>=? order by DATEID DESC";

			PreparedStatement ddd9Stmnt = DB.getDDD9Stmnt();
			// String query = "UPDATE BBROCK SET VBI=?
			// WHERE STOCKID =? and DATEID=?";

			PreparedStatement updateVBI = DB.updateVBIStmnt();

			ddd9Stmnt.setInt(1, stockID);
			ddd9Stmnt.setInt(2, dateId);
			ddd9Stmnt.setInt(3, dateId - 5);

			ResultSet rs = ddd9Stmnt.executeQuery();

			int VBI = 0;
			int VB1 = 0;
			int VB2 = 0;
			float d20 = 0.0f;
			float d90 = 0.0f;
			int count = 0;
			while (rs.next()) {
				float d2 = rs.getFloat(1);
				float d9 = rs.getFloat(2);
				if (d20 < 0.01f && d90 < 0.01f) {
					d20 = d2;
					d90 = d9;
				} else {
					if (3 * d20 < d2) { // DD must be reduced more than 2/3
						if (d90 < d9 && d90 >= 0.9f * d9) { // D9 reduced but not more than 10%
							VBI = 118;
						} else if (d90 < d9 && d90 < 0.9f * d9 && d90 > 0.8f * d9) {
							VBI = 108;
						} else if (d90 > d9 && d90 <= 1.1f * d9) { // not sure if details matters
							VBI = 28;
						} else if (d90 > d9 && d90 > 1.1f * d9 && d90 < 1.2f * d9) { // not sure if details matters
							VBI = 18;
						}

						if (VB1 == 0) {
							VB1 = VBI;
							VBI = 0;
						} else {
							VB2 = VBI;
							VBI = 0;
						}
					}
				}
				count++;
				if (count >= 3)
					break;

			}

			if (VB1 >= VB2) {
				VBI = VB1;
			} else {
				VBI = VB2;
			}

			updateVBI.setInt(1, VBI);
			updateVBI.setInt(2, stockID);
			updateVBI.setInt(3, dateId);
			updateVBI.executeUpdate();

		} catch (Exception ex) {

		}
	}

	public static void processOBIHistory(int length) {
		try {
			PreparedStatement OBIHistoryStmnt = DB.getOBIHistoryStmnt();
			PreparedStatement UpdateOBIStmnt = DB.getUpdateOBIStmnt();

			// the last 1 and half data is more reliable
			OBIHistoryStmnt.setInt(1, length);
			ResultSet rs = OBIHistoryStmnt.executeQuery();

			while (rs.next()) {
				int dateId = rs.getInt(1);
				int obi = rs.getInt(3);

				UpdateOBIStmnt.setInt(1, obi);
				UpdateOBIStmnt.setInt(2, dateId);
				UpdateOBIStmnt.executeUpdate();
				System.out.println("processOBIHistory done " + dateId);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayOBI(int dateId) {
		try {
			PreparedStatement todayOBIStmnt = DB.getTodayOBIStmnt();
			PreparedStatement UpdateOBIStmnt = DB.getUpdateOBIStmnt();

			// the last 1 and half data is more reliable
			todayOBIStmnt.setInt(1, dateId);
			ResultSet rs = todayOBIStmnt.executeQuery();

			while (rs.next()) {
				// int dateId = rs.getInt(1);
				int obi = rs.getInt(3);

				UpdateOBIStmnt.setInt(1, obi);
				UpdateOBIStmnt.setInt(2, dateId);
				UpdateOBIStmnt.executeUpdate();
				System.out.println("DateId done " + dateId);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void initCurrentDateID() {
		try {
			PreparedStatement cDateIDStmt = DB.getCurrentDateID();
			ResultSet rs = cDateIDStmt.executeQuery();
			if (rs.next()) {
				currentDateID = rs.getInt(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// currentDateID
	public static void processEE8History() {
		// remember update currentDateID
		initCurrentDateID();
		// 8933 is the 11 th day after we have volume, mark info which
		// are required to calculate VBI info, so start from here
		for (int k = 8933; k <= currentDateID; k++) {
			processTodayEE8(k);
		}
	}

	public static void processStockEE8History(int stockID) {
		// remember update currentDateID
		// 8933 is the 11 th day after we have volume, mark info which
		// are required to calculate VBI info, so start from here
		initCurrentDateID();
		for (int k = 8933; k <= currentDateID; k++) {
			processTodayEE8ForStock(k, stockID);
		}
	}

	public static void processTodayEE8(int dateId) {
		try {
			PreparedStatement todayVBIFXStmnt = DB.getTodayVBIFUCX();

			// String query = "select FUC,VBI,STOCKID,CLOSE, DATEID
			// FROM BBROCK WHERE DATEID=? AND (FUC>? OR VBI>?) ORDER BY STOCKID ASC";

			todayVBIFXStmnt.setInt(1, dateId);
			todayVBIFXStmnt.setInt(2, 1); // FUC=8 or FUC=4
			todayVBIFXStmnt.setInt(3, 100); // VBI=118 or VBI=108
			ResultSet rs = todayVBIFXStmnt.executeQuery();

			while (rs.next()) {
				int stkid = rs.getInt(3);

				processTodayEE8ForStock(dateId, stkid);
				System.out.println("EE8ForStock done for stockid " + stkid);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayEE8ForStock(int dateId, int stockID) {
		try {
			PreparedStatement stockVBIFXStmnt = DB.getStockVBIFUCX();
			PreparedStatement updateEE8 = DB.updateEE8();

			// String query = "select FUC,VBI,STOCKID,CLOSE, DATEID
			// FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=? ORDER BY DATEID
			// DESC";

			stockVBIFXStmnt.setInt(1, stockID);
			stockVBIFXStmnt.setInt(2, dateId - 6);
			stockVBIFXStmnt.setInt(3, dateId);
			ResultSet rs = stockVBIFXStmnt.executeQuery();

			int fucMax = 0;
			int vbiMax = 0;
			int count = 0;
			while (rs.next()) {
				int fuc = rs.getInt(1);
				int vbi = rs.getInt(2);

				if (fuc > fucMax) {
					fucMax = fuc;
				}
				if (vbi > vbiMax) {
					vbiMax = vbi;
				}

				count++;
				if (count >= 3) {
					break;
				}
			}

			// String query = "UPDATE BBROCK SET EE8=? WHERE STOCKID=?
			// AND DATEID=?";
			int EE8 = 0;
			if (fucMax == 8 && vbiMax == 118) {
				EE8 = 88;
			} else if (fucMax == 8 && vbiMax == 108) {
				EE8 = 84;
			} else if (fucMax == 4 && vbiMax == 118) {
				EE8 = 48;
			} else if (fucMax == 4 && vbiMax == 108) {
				EE8 = 44;
			}
			updateEE8.setInt(1, EE8);
			updateEE8.setInt(2, stockID);
			updateEE8.setInt(3, dateId);
			updateEE8.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processD2D9History(boolean lastOnly) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				if (stockID == 48) {
					System.out.println("AMZN");
				}

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// 8923 is the starting date with volume and marketcap
				for (int k = endDateId; k >= 8923; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist) {
						processD2Today(stockID, k);
						if (lastOnly)
							break;
					}
				}

				// 8923 is the starting date with volume and marketcap
				for (int k = endDateId; k >= 8923; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist) {
						processD9Today(stockID, k);
						if (lastOnly)
							break;
					}
				}
				System.out.println("process D2D9 done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// TTA bull indicator, basically FUC, TBK and VBI any two is a combination
	// of long/intermediate signals with bullish potentials
	public static void processTTAHistory(boolean lastOnly) {
		// get all stocks, may be current??
		try {

			long t1 = System.currentTimeMillis();

			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				processStockTTAHistory(stockID, lastOnly);

				System.out.println("process TTA History done for " + stockID);
				if (!lastOnly)
					Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		// loop through stocks and calculate each history of TTA
		// call processStockTTAHistory(int stockId, boolean lastOnly)

	}

	public static void processStockTTAHistory(int stockId, boolean lastOnly) {
		try {
			PreparedStatement dateIdBeginWithTTA = DB.getStockTTADateIDBegin();
			PreparedStatement resetStockTTA = DB.resetStockTTA();
			initCurrentDateID();
			int beginDateId = 0;
			int endDateId = currentDateID;

			if (lastOnly && currentDateID > 0) {
				processStockTTAToday(stockId, currentDateID);
			} else {
				// reset AVI = 0
				resetStockTTA.setInt(1, stockId);
				resetStockTTA.executeUpdate();
				// String query = "select MIN(DATEID) FROM BBROCK
				// WHERE STOCKID = ? AND (FUC>=4 OR VBI>=28 OR TBK>=8)";

				dateIdBeginWithTTA.setInt(1, stockId);
				ResultSet rs = dateIdBeginWithTTA.executeQuery();
				if (rs.next()) {
					beginDateId = rs.getInt(1);
				}

				// we need 30 days for checking TTA combo, but start 15 days after 1st signal
				int begin = beginDateId + 15;

				for (int k = begin; k <= endDateId; k++) {
					processStockTTAToday(stockId, k);
				}
			}
			// loop through stocks and calculate each history of TTA
			// call processStockTTAToday(int stockId, int dateId)
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	/*
	 * VBI calculation logic is based on DD (days to deplete outstand stocks, i.e.,
	 * markcap/close/dailyVolume) and D9 (10 day average of DD). VBI only detects
	 * latest DD compared to last 3 days (4 days data total), if DD drops more than
	 * 2/3 from any of the previous 3 days, any VBI>=8 (18,28,108,118) indicates
	 * this, based on D9 change range 10% or 20%, the rest is determined, not sure
	 * how big the impact is. So VBI>=8 (especially VBI>=108) is an early indicator
	 * of strong buy or sell, very time sensitive (within a week or two). [ detail
	 * logic is in processVBIToday(int stockID, int dateId) of UpDownMeasure] This
	 * metrics maybe should be combined with TBK (30 days outbreak pattern), pure
	 * Teal of latest and close price above the last 90% (>=27 days) of pink+yellow
	 * count of continuous 30 days). So TBK only consider P/Y/T and close pattern
	 * and close price over 30 days with no volume info. TBK+VBI --> considers short
	 * term heavy volume action with long term price breakout pattern. A good
	 * combination. TBK AND VBI SHOULD NOT BE ON THE SAME DAY
	 * 
	 * FUC (U turn pattern) is based on last 250 days (one year) max drop DPC ( drop
	 * percentage from peak) and UPC (Up percentage from bottom of last 30 days),
	 * UTURN is defined UPC>40 and UPC-DPC>100 (FUC=8) or only UPC>40 and
	 * UPC-DPC<100, then FUC=4 So FUC+VBI --> considers short term heavy volume
	 * action with long term price bullish pattern. A good combination. FUC AND VBI
	 * SHOULD NOT BE ON THE SAME DAY
	 * 
	 * TBK + FUC IS ALSO A GOOD COMBINATION AS THE BREAKOUT HAS A YEAR BASE??
	 * 
	 * FUC by itself is significant as it takes both intermediate (30 days) bullish
	 * pattern with long term correction pattern
	 * 
	 * EE8 is a combo of FUC and VBI of last 4 days, maybe this should be more days
	 * in between, like 25 to 30 days?...for FUC+VBI or TBK+VBI patterns
	 * 
	 * Examples of such: PLUG, FUBO,GUSH,
	 * LABU,FB,AAPL,TSLA,SIG,VUZI,JKS,RIG,PTON,SNAP,TCS,LOVE,OCUL,BE,BEEM,BILI,WST,
	 * WDC(DANGER),SHOO, FOSL, NVTA, SOXL,TQQQ,FCEL,ZM,
	 * AMZN,PINS,FSLY,TUP,ROKU,CRSP,SQ,NVTA,Z,PRLB,TDOC,SHOP,EDIT,NSTG,U
	 */
	public static void processStockTTAToday(int stockId, int dateId) {
		try {
			int days = 29; // dateId-days to dateId is 30 days
			PreparedStatement TTAInfo = DB.getStockTTAInfo();
			PreparedStatement updateTTA = DB.updateTTA();
			PreparedStatement existTTA = DB.checkTTAExistence();

			// Check last 30 days TTA info
			// String query = "select DATEID,CLOSE,FUC,TBK,VBI,DD,VOLUME FROM
			// BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?
			// AND (FUC>=4 OR VBI>=28 OR TBK>=8 OR DATEID=? OR DATEID=?)
			// ORDER BY DATEID DESC";

			TTAInfo.setInt(1, stockId);
			TTAInfo.setInt(2, dateId - days);
			TTAInfo.setInt(3, dateId);
			TTAInfo.setInt(4, dateId - days);
			TTAInfo.setInt(5, dateId);

			ResultSet rs1 = TTAInfo.executeQuery();

			boolean fucAlone = false;
			boolean tbkAlone = false;
			boolean vbiAlone = false;
			int fucNum = 0;
			int tbkNum = 0;
			int vbiNum = 0;

			// TTA (TIE SHANG JIAO) THREE TRIANGLE = FUC+TBK+VBI
			// ANY TWO OR THREE WITHIN 30 DAYS IS GOOD
			// IF FUC=4, THEN 1XX, FUC=8, THEN 2XX
			// IF TBK>=8, COUNT HOW MANY WITHIN 30 DAYS, IF C, THEN XCX
			// IF VBI>=18, COUNT HOW MANY WITHIN 30 DAYS, IF D, THEN XXD
			// SO TTA IS 1CD OR 2CD, MOVING FROM DATEID ASC DIRECTION,TTA MUST NOT BE THE
			// SAME VALUE FOR LAST 30 DAYS
			// ALSO, THE LATEID MARK DAY CLOSE PRICE SHOULD BE HIGHER THAN EARLY CLOSE PRICE
			// ALSO WATCH DD=0, NOT CONSIDER VBI. ALSO IF TWO VALUES ON SAME DAY, ONLY ONE
			// CAN BE CHOSEN
			// FUC TAKE 1ST PRIORITY UNLESS THERE IS ONE WITHIN 30 DAYS, THEN TBK, THEN VBI
			// AS FUC IS LONG TERM AND RARE, TBK IS MEDIATE TERM, VBI IS SHORT TERM AND
			// FREQUENT
			if (dateId == 9052) {
				System.out.println("Debugging...");
			}
			float endPrice = 0.0f;// 30 day last close price with/without TTA <>0
			float ttaEndPrice = 0.0f;// last(DATEID max) TTA<>0 tag close price
			float beginPrice = 0.0f;// 30 day begin close price with/without TTA<>0
			float ttaBeginPrice = 0.0f; // first (DATEID min) TTA<>0 tag close price
			while (rs1.next()) { // begin if (rs1.next())
				// DATEID,CLOSE,FUC,TBK,VBI,DD,VOLUME
				int tempDateId = rs1.getInt(1);

				float tempClose = rs1.getFloat(2);
				int tempFUC = rs1.getInt(3);
				int tempTBK = rs1.getInt(4);
				int tempVBI = rs1.getInt(5);
				float tempDD = rs1.getFloat(6);
				float tempVolume = rs1.getFloat(7);

				// only update endPrice once at loop begin
				if (endPrice < 0.001f) {
					endPrice = tempClose;
				}

				// only update ttaEndPrice once if TTA tag <>0, VBI has to take out DD=0 special
				// case
				if (ttaEndPrice < 0.001f
						&& (tempFUC >= 4 || tempTBK >= 8 || (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1))) {
					ttaEndPrice = tempClose;
				}

				// keep updating begin price as DATEID in DESC order
				beginPrice = tempClose;

				// keep updating ttaBeginPrice as DATEID in DESC order if TTA tag <>0, VBI has
				// to take out DD=0 special case
				if (tempFUC >= 4 || tempTBK >= 8 || (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1)) {
					ttaBeginPrice = tempClose;
				}

				// update fucAlone tag if conditions met for a result row
				if (tempFUC >= 4 && tempTBK < 2 && tempVBI < 2) {
					fucAlone = true;
					fucNum++;
				} else if (tempFUC < 2 && tempTBK >= 8 && tempVBI < 2) {
					// update tbkAlone tag if conditions met for a result row
					tbkAlone = true;
					tbkNum++;
				} else if (tempFUC < 2 && tempTBK < 2 && (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1)) {
					// update vbiAlone tag if conditions met for a result row, VBI has to take out
					// DD=0 special case
					vbiAlone = true;
					vbiNum++;
				} else if (tempFUC >= 4 && tempTBK >= 8 && (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1)) {
					if (!fucAlone) { // fuc take priority as it is the longest term indicator (250 days) with rarest
										// frequency
						fucAlone = true;
						fucNum++;
					} else if (!tbkAlone) { // then tbk, it is at least 31 days long indicator
						tbkAlone = true;
						tbkNum++;
					} else if (!vbiAlone) { // VBI is the most frequent with 10 days data only
						vbiAlone = true;
						vbiNum++;
					} else if (fucNum < 2) { // since unsigned tiny int max 255
						fucNum++;
					} else if (tbkNum < 5) { // since unsigned tiny int max 255
						tbkNum++;
					} else if (vbiNum < 5) {
						vbiNum++;
					}
				} else if (tempFUC >= 4 && tempTBK >= 8 && tempVBI < 2) {
					if (!fucAlone) { // fuc take priority as it is the longest term indicator (250 days) with rarest
										// frequency
						fucAlone = true;
						fucNum++;
					} else if (!tbkAlone) { // then tbk, it is at least 31 days long indicator
						tbkAlone = true;
						tbkNum++;
					} else if (fucNum < 2) { // since unsigned tiny int max 255
						fucNum++;
					} else if (tbkNum < 5) { // since unsigned tiny int max 255
						tbkNum++;
					}
				} else if (tempFUC < 2 && tempTBK >= 8 && (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1)) {
					if (!tbkAlone) { // then tbk, it is at least 31 days long indicator
						tbkAlone = true;
						tbkNum++;
					} else if (!vbiAlone) { // VBI is the most frequent with 10 days data only
						vbiAlone = true;
						vbiNum++;
					} else if (tbkNum < 5) {
						tbkNum++;
					} else if (vbiNum < 5) {
						vbiNum++;
					}
				} else if (tempFUC >= 4 && tempTBK < 2 && (tempVBI >= 28 && tempDD > 0.0000001f && tempVolume > 1)) {
					if (!fucAlone) { // fuc take priority as it is the longest term indicator (250 days) with rarest
										// frequency
						fucAlone = true;
						fucNum++;
					} else if (!vbiAlone) { // VBI is the most frequent with 10 days data only
						vbiAlone = true;
						vbiNum++;
					} else if (fucNum < 2) {
						fucNum++;
					} else if (vbiNum < 5) {
						vbiNum++;
					}
				}

			} // end while (rs1.next())

			// only consider if at least two indicators show up within 30 days
			if ((fucNum > 0 && tbkNum > 0) || (fucNum > 0 && vbiNum > 0) || (vbiNum > 0 && tbkNum > 0)) {

				// only consider a bull case if only end close price is higher
				float maxEndClose = endPrice; // 30 day last close price with/without TTA <>0
				if (ttaEndPrice > maxEndClose)
					maxEndClose = ttaEndPrice;// last(DATEID max) TTA<>0 tag close price
				float minBeginClose = beginPrice; // 30 day begin close price with/without TTA<>0
				if (ttaBeginPrice < minBeginClose)
					minBeginClose = ttaBeginPrice; // first (DATEID min) TTA<>0 tag close price

				if (maxEndClose > minBeginClose) { // begin if (maxEndClose > maxBeginClose)
					int ValTTA = 100 * fucNum + 10 * tbkNum + vbiNum;
					// before update TTA with stockId, dateId and ValTTA
					// check if such value exists in the last 30 days to avoid repeat
					// and too much signal to read each day
					// String query = "select COUNT(*) FROM BBROCK WHERE STOCKID = ?
					// AND DATEID>=? AND DATEID<=? AND TTA=?";

					existTTA.setInt(1, stockId);
					existTTA.setInt(2, dateId - days);
					existTTA.setInt(3, dateId);
					existTTA.setInt(4, ValTTA);

					ResultSet rs2 = existTTA.executeQuery();

					if (rs2.next()) { // begin if (rs2.next())
						int exist = rs2.getInt(1);
						if (exist == 0) { // if a new VAlTTA, then update
							// update TTA with stockId, dateId and ValTTA
							// String query = "UPDATE BBROCK SET TTA=? WHERE STOCKID = ?
							// AND DATEID=?";
							updateTTA.setInt(1, ValTTA);
							updateTTA.setInt(2, stockId);
							updateTTA.setInt(3, dateId);
							updateTTA.executeUpdate();
						}
					} // end if (rs2.next())
				} // end if (maxEndClose > maxBeginClose)
			} // only consider if at least two indicators show up within 30 days

		} catch (

		Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processAVIHistory(boolean lastOnly) {
		// get all stocks, may be current??
		try {

			long t1 = System.currentTimeMillis();

			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				processStockAVIHistory(stockID, lastOnly);

				System.out.println("process AVIHistory done for " + stockID);
				if (!lastOnly)
					Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		// loop through stocks and calculate each history of RTS
		// call processStockAVIHistory(int stockId, boolean lastOnly)

	}

	// -- bull pattern within 30 days, 118 ->128->228(optional) then close price
	// above max from 118 bull buy like GE, FB

	public static void processStockAVIHistory(int stockId, boolean lastOnly) {
		try {
			PreparedStatement dateIdRangeWithVolumeInfo = DB.getStockVolumeDateIDRange();
			PreparedStatement resetStockAVI = DB.resetStockAVI();
			initCurrentDateID();
			int beginDateId = 0;
			int endDateId = currentDateID;

			if (lastOnly && currentDateID > 0) {
				processStockAVIToday(stockId, currentDateID);
			} else {
				// reset AVI = 0
				resetStockAVI.setInt(1, stockId);
				resetStockAVI.executeUpdate();
				// String query = "select MIN(DATEID), MAX(DATEID)
				// FROM BBROCK WHERE STOCKID = ? AND VOLUME>1";
				dateIdRangeWithVolumeInfo.setInt(1, stockId);
				ResultSet rs = dateIdRangeWithVolumeInfo.executeQuery();
				if (rs.next()) {
					beginDateId = rs.getInt(1);
					endDateId = rs.getInt(2);
				}

				// we need 10 days for accurate D9 and another 14 days for AVI
				int begin = beginDateId + 25;

				for (int k = begin; k <= endDateId; k++) {
					processStockAVIToday(stockId, k);
				}
			}
			// loop through stocks and calculate each history of RTS
			// call processStockAVIToday(int stockId, int dateId)
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockAVIToday(int stockId, int dateId) {
		try {
			float upLimit = 0.35f;
			float downLimit = -0.35f;
			int days = 14;
			PreparedStatement stkDateId = DB.getStockDateId();
			PreparedStatement checkD9AndClose = DB.checkD9Close();
			PreparedStatement checkAVIExist = DB.checkAVIExist();
			PreparedStatement updateStockAVI = DB.updateStockAVI();

			// String query = "select DATEID FROM BBROCK WHERE STOCKID=?
			// AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";
			stkDateId.setInt(1, stockId);
			stkDateId.setInt(2, dateId - 20);
			stkDateId.setInt(3, dateId);
			ResultSet rs0 = stkDateId.executeQuery();
			int dateIdBegin = dateId - days + 1; // default 14 days ago
			int count1 = 0;
			while (rs0.next()) {
				dateIdBegin = rs0.getInt(1);
				count1++;
				if (count1 >= days) { // need 14 days exact
					break;
				}
			}

			// check if from dateId to dateId-13 (14 days) max(D9),min(D9)
			// to see if +35% or -35% exist, if not exist, skip the rest
			PreparedStatement minMaxD9 = DB.getMinMaxD9();
			// String query = "select MIN(D9),MAX(D9) from BBROCK
			// WHERE STOCKID=? and DATEID>=? and DATEID<=?";
			minMaxD9.setInt(1, stockId);
			minMaxD9.setInt(2, dateIdBegin);
			minMaxD9.setInt(3, dateId);

			ResultSet rs1 = minMaxD9.executeQuery();

			if (rs1.next()) { // begin if (rs1.next())
				float minD9 = rs1.getFloat(1);
				float maxD9 = rs1.getFloat(2);

				// if max(D9),min(D9) +35% or -35% exist, then more intensive calculation
				// select close, d9, dateid from bbrock order by dateid asc dateid range

				if ((1.0f + downLimit) * maxD9 >= minD9 || (1.0f + upLimit) * minD9 <= maxD9) {
					// String query = "SELECT DATEID, CLOSE, D9 from BBROCK
					// WHERE STOCKID=? and DATEID>=? and DATEID<=?
					// ORDER BY DATEID ASC";
					checkD9AndClose.setInt(1, stockId);
					checkD9AndClose.setInt(2, dateIdBegin);
					checkD9AndClose.setInt(3, dateId);

					ResultSet rs2 = checkD9AndClose.executeQuery();
					int[] dateIds = new int[days];
					float[] closes = new float[days];
					float[] d9s = new float[days];

					int count = 0;
					// get last 14 days each day data on close, D9
					while (rs2.next()) {
						dateIds[count] = rs2.getInt(1);
						closes[count] = rs2.getFloat(2);
						d9s[count] = rs2.getFloat(3);
						count++;
						if (count >= days) {// should not happen, just a precaution measurement
							break;
						}
					} // end while

					// loop through if d9 compared to max(d9) or min(d9) if neither +35% nor -35%
					// then move on to next day
					// if verified within range (>+35% or <-35%), then compare the rest days going
					// forward to find the earlies
					// such candidate, then compare close price to identify AVI
					// once AVI value is assigned, then need to back date from this day 14 days to
					// see if
					// any exact AVI value has been assigned, if so skip updating (to avoid too much
					// signals for a given day)
					// if not then update, continue to move this dateid up till all data checked
					for (int day = 0; day < count; day++) { // count is better than days as sometime 13 records
						float earlyD9 = d9s[day];
						int earlyDateId = dateIds[day];
						float earlyClose = closes[day];
						for (int lateDay = day + 1; lateDay < count; lateDay++) {// add one day to start loop
							float lateD9 = d9s[lateDay];
							int lateDateId = dateIds[lateDay];
							float lateClose = closes[lateDay];
							int avi = 0;
							// logic starts
							if (earlyD9 < lateD9 && (earlyD9 * (1.0f + upLimit) <= lateD9)) {// AVI=x28 candidate, early
																								// date much faster pace
																								// buy based on D9
								if (earlyClose <= lateClose) {
									avi = 128; // price increase but buying pace slowed down
								} else if (earlyClose > lateClose) {
									avi = 228; // price decrease and buying pace slowed down
								}

							} else if (earlyD9 > lateD9 && (earlyD9 * (1.0f + downLimit) >= lateD9)) {// AVI=x18
																										// candidate,
																										// early date
																										// much slower
																										// pace buy
																										// based on D9
								if (earlyClose <= lateClose) {
									avi = 118; // price increase but buying pace faster
								} else if (earlyClose > lateClose) {
									avi = 218; // price decrease and buying pace faster
								}

								// so the first digit of AVI indicates price increase (1xx) or decrease(2xx)
								// the second digit of AVI indicates the D9(smaller) faster(x1x) or slower
								// (x2x)(bigger)
							}

							if (avi > 0) {
								// need to check past 14 days if we have the same value or not
								// String query = "SELECT COUNT(*) from BBROCK WHERE
								// STOCKID=? and DATEID>=? and DATEID<=? AND AVI=?";
								checkAVIExist.setInt(1, stockId);
								checkAVIExist.setInt(2, lateDateId - days);
								checkAVIExist.setInt(3, lateDateId + days);// expand as the loop goes back and forth
								checkAVIExist.setInt(4, avi);
								ResultSet rs3 = checkAVIExist.executeQuery();

								if (rs3.next()) {
									int exist = rs3.getInt(1);
									if (exist == 0) {// if not then update
										// String query = "UPDATE BBROCK SET AVI=?
										// WHERE STOCKID=? AND DATEID=?";
										updateStockAVI.setInt(1, avi);
										updateStockAVI.setInt(2, stockId);
										updateStockAVI.setInt(3, lateDateId);
										updateStockAVI.executeUpdate();
									} // end if (exist == 0) {
								} // end if (rs3.next())
							} // end if (avi > 0) {
								// logic ends
						} // end laterDate loop
					} // end early day loop

				} // end if ((1.0f + downLimit) * maxD9 >= minD9 || (1.0f + upLimit) * minD9 <=
					// maxD9) {
			} // end if (rs1.next())

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processRTSHistory(boolean lastOnly) {
		// get all stocks, may be current??
		try {

			long t1 = System.currentTimeMillis();

			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				processStockRTSHistory(stockID, lastOnly);

				System.out.println("process StockRTSHistory done for " + stockID);
				if (!lastOnly)
					Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockRTSHistory(int stockId, boolean lastOnly) {
		try {
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			initCurrentDateID();
			int beginDateId = 0;
			int endDateId = currentDateID;

			if (lastOnly && currentDateID > 0) {
				processStockRTSToday(stockId, currentDateID);
			} else {
				// String query = "select MIN(DATEID), MAX(DATEID)
				// FROM BBROCK WHERE STOCKID = ? ";
				dateIdRange.setInt(1, stockId);
				ResultSet rs = dateIdRange.executeQuery();
				if (rs.next()) {
					beginDateId = rs.getInt(1);
					endDateId = rs.getInt(2);
				}
				int begin = beginDateId + 30;

				if (begin < 8261)// 800 days is enough for now,2017/10/09
					begin = 8261;

				for (int k = endDateId; k >= begin; k--) {
					processStockRTSToday(stockId, k);
				}
			}
			// loop through stocks and calculate each history of RTS
			// call processStockRTSToday(int stockId, int dateId)
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockRTSToday(int stockId, int dateId) {
		try {
			PreparedStatement past30Stmnt = DB.getPast30Stmnt();
			PreparedStatement updateRtsMcp = DB.updateRTS_MCP();
			// String query = "SELECT MAX(CLOSE),SUM(YELLOW),SUM(PINK),
			// AVG(VOLUME) FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=?";
			past30Stmnt.setInt(1, stockId);
			past30Stmnt.setInt(2, dateId - 29);
			past30Stmnt.setInt(3, dateId);

			ResultSet rs = past30Stmnt.executeQuery();
			if (rs.next()) {
				float mcp = rs.getFloat(1);
				int ySum = rs.getInt(2);
				int pSum = rs.getInt(3);
				// String query = "UPDATE BBROCK SET RTS=?, MCP=?
				// WHERE STOCKID=? AND DATEID=?";
				int rts = ySum + pSum;

				if (rts >= 27) { // 90% of 30 days =27
					// update RTS, MCP
					updateRtsMcp.setInt(1, rts);
					updateRtsMcp.setFloat(2, mcp);
					updateRtsMcp.setInt(3, stockId);
					updateRtsMcp.setInt(4, dateId);
					updateRtsMcp.executeUpdate();

				} else {
					// update RTS only
					updateRtsMcp.setInt(1, rts);
					updateRtsMcp.setFloat(2, 0.00000000f);
					updateRtsMcp.setInt(3, stockId);
					updateRtsMcp.setInt(4, dateId);
					updateRtsMcp.executeUpdate();
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processDMAHistory() {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				if (stockID == 48) {
					System.out.println("AMZN");
				}

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = endDateId; k >= strtDateId; k--) {
				for (int k = currentDateID; k >= currentDateID; k--) {
					// for (int k = currentDateID; k >= currentDateID; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist)
						processTodayDMA(stockID, k);
				}

				System.out.println("processDMAHistory done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStockFUCHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// for (int k = strtDateId + upDays; k <= endDateId; k++) {
			for (int k = strtDateId + downDays + 20; k <= endDateId; k++) {
				boolean exist = false;
				int adjustment = 0;
				int lcMax = 10;
				int lc = 0;
				do {
					dateIDExistStmnt.setInt(1, stockID);
					dateIDExistStmnt.setInt(2, k);

					ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

					dateIDExist.next();

					int count = dateIDExist.getInt(1);

					if (count > 0) {
						exist = true;
					} else {
						k++;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist)
					processTodayFUC(stockID, k);
			}

			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processFUCHistory() {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = strtDateId + upDays; k <= endDateId; k++) {
				for (int k = endDateId; k >= currentDateID; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k++;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist)
						processTodayFUC(stockID, k);
				}

				try {
					Thread.sleep(10);
				} catch (Exception ex) {

				}
				System.out.println("processFUCHistory done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void resetAllStocksTBKHistory() {
		try {
			PreparedStatement resetTBK = DB.resetTBKStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);
			ResultSet rs = allStocks.executeQuery();
			System.out.println("-----------Begin reset all stocks TBK to zero---------");
			while (rs.next()) {
				int stockID = rs.getInt(1);
				// reset TBK for this stock to zero as we will recalculate values
				resetTBK.setInt(1, stockID);
				resetTBK.executeUpdate();

			}
			System.out.println("-----------Done reset all stocks TBK to zero---------");
			Thread.sleep(2000);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTBKHistory(boolean lastOnly) {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// process last 200 days for the moment 9058 to 8858
		// begin = 8261;
		// for (int w = currentDateID; w > 8261; w--) {
		// for (int w = tbkStartDateId; w <= currentDateID; w++) { // TBK must start
		// from early to latest
		// System.out.println("Processing TBK at " + w);
		try {
			processTodayTBK(currentDateID, -1);
			// sleep in 2 seconds
			Thread.sleep(2000);

		} catch (Exception ex) {

		}

		// }

	}

	public static void processStockTBKHistory(int stockID) {
		try {
			initCurrentDateID();// init currentDateID value based on DB

			PreparedStatement resetTBK = DB.resetTBKStmnt();
			// reset TBK for this stock to zero as we will recalculate values
			resetTBK.setInt(1, stockID);
			resetTBK.executeUpdate();

			PreparedStatement startStmnt = DB.getDateIDStarttmnt();
			// String query = "SELECT DATEID FROM BBROCK WHERE STOCKID =?
			// ORDER BY DATEID ASC limit 1";
			startStmnt.setInt(1, stockID);
			ResultSet rs1 = startStmnt.executeQuery();

			if (rs1.next()) {
				int startDate = rs1.getInt(1);
				boolean reset = false;
				// 30 is the past days number
				int start = tbkStartDateId;
				if (startDate > tbkStartDateId)
					start = startDate + 30;
				for (int w = start; w <= currentDateID; w++) {

					processTodayTBK(w, stockID);
					// sleep in 2 seconds
					// Thread.sleep(2000);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTodayTBK(int dateID, int stockId) {
		PreparedStatement getPureTeal = DB.getPureTeal();
		PreparedStatement updateTBK = DB.updateTBKStmnt();
		// PreparedStatement past30Stmnt = DB.getPast30Stmnt();
		PreparedStatement closestMCPStmnt = DB.getClosetMCPStmnt();
		PreparedStatement closePriceStmnt = DB.getClosePriceStmnt();
		PreparedStatement tbkStmnt = DB.getTBKStmnt();

		try {

			// String query = "select STOCKID FROM BBROCK WHERE DATEID=?
			// AND STOCKID>=? AND STOCKID<=? AND TEAL=1 AND YELLOW=0 AND
			// PINK=0 ORDER BY STOCKID ASC";
			getPureTeal.setInt(1, dateID);
			if (stockId <= 0) {
				getPureTeal.setInt(2, 0);
				getPureTeal.setInt(3, 1000000);
			} else {
				getPureTeal.setInt(2, stockId);
				getPureTeal.setInt(3, stockId);
			}

			ResultSet rs1 = getPureTeal.executeQuery();

			while (rs1.next()) {
				int nextStockID = rs1.getInt(1);
				try {
					System.out.println("Processing TBK at " + dateID + " for " + nextStockID);
					// 1. reset TBK to zero for this stock
					// may be should take out this logic do it separately
					// as one time operation

					// 2. select the closest MCP and RTS
					// String query = "select DATEID,MCP,RTS FROM BBROCK
					// WHERE MCP>0.001 AND RTS>=? AND STOCKID = ?
					// AND DATEID<? AND DATEID>? ORDER BY DATEID DESC LIMIT 50";
					closestMCPStmnt.setInt(1, 27);// 27 is 90% of 30 days, this could be up further
					closestMCPStmnt.setInt(2, nextStockID);
					closestMCPStmnt.setInt(3, dateID);
					closestMCPStmnt.setInt(4, dateID - 500);// almost two years should be enough long

					ResultSet rs2 = closestMCPStmnt.executeQuery();
					if (rs2.next()) {
						// 3. verify that close price>MCP
						int cDateId = rs2.getInt(1);
						float mcp = rs2.getFloat(2);
						int rts = rs2.getInt(3);

						if (dateID == 8907 || dateID == 8945 || dateID == 8949 || dateID == 8950 || dateID == 8951
								|| dateID == 8952 || dateID == 8953 || dateID == 8954) {
							System.out.println("8907 debug starts...");
						}

						// String query = "SELECT CLOSE,PDY,BDY FROM BBROCK
						// WHERE STOCKID = ? AND DATEID =? ";

						closePriceStmnt.setInt(1, nextStockID);
						closePriceStmnt.setInt(2, dateID);

						ResultSet rs3 = closePriceStmnt.executeQuery();
						if (rs3.next()) {
							float closePrice = rs3.getFloat(1);
							if (closePrice > mcp) {
								// now check between cDateId and dateID, how many TBK>=8
								// String query = "SELECT TBK, DATEID, CLOSE FROM BBROCK
								// WHERE TBK>=? AND STOCKID = ? AND DATEID >=? AND
								// DATEID<=? ORDER BY DATEID DESC";
								tbkStmnt.setInt(1, 8); // only interested TBK>=8 cases
								tbkStmnt.setInt(2, nextStockID);
								tbkStmnt.setInt(3, cDateId + 1);
								tbkStmnt.setInt(4, dateID - 1);

								ResultSet rs4 = tbkStmnt.executeQuery();
								int tbkCount = 0;
								int tbkMax = 0;
								int lastestTBKDateID = 0;
								int lastestTBKVal = 0;
								while (rs4.next()) {
									if (tbkCount == 0) {
										lastestTBKVal = rs4.getInt(1);
										lastestTBKDateID = rs4.getInt(2);
										tbkMax = lastestTBKVal;
									}

									int tempTBK = rs4.getInt(1);
									if (tempTBK > tbkMax) {
										tbkMax = tempTBK;
									}
									tbkCount++;
								} // end while

								// 4. check if there is TBK =18 in the past 30 days
								// and if it is the 3rd days within last 5 >MCP

								// b. if it is the 3rd days within last 5 >MCP since TBK=8, then update TBK =18

								// c. if there is TBK = 8 or TBK=88, but at least there is 20??(SUM(p+y) in
								// between, then new TBK=8 maybe??)
								int tbkFinal = 0;
								if (tbkCount == 0) {
									// a. first ever closePrice>closetMCP then update TBK =8
									tbkFinal = 8;

								} else { // tbkCount>0
									// now we need to check between lastestTBKDateID and dateID
									if (lastestTBKVal == 8) { // a failed breakout, then we need to >30 days
										// before next closePrice>MCP considered to be valid
										if ((dateID - lastestTBKDateID) > 30) {
											tbkFinal = 8;
										} else if ((dateID - lastestTBKDateID) <= 5) {
											// now check if we have three days closePrice>MCP
											// if so tbkFinal = 18; or more based on tbkMax
											// not done yet
											// now check between cDateId and dateID, how many TBK>=8
											// String query = "SELECT TBK, DATEID, CLOSE FROM BBROCK
											// WHERE TBK>=? AND STOCKID = ? AND DATEID >=? AND
											// DATEID<=? ORDER BY DATEID DESC";
											tbkStmnt.setInt(1, 0); // check every day close price, so TBK>=0
											tbkStmnt.setInt(2, nextStockID);
											tbkStmnt.setInt(3, lastestTBKDateID);
											tbkStmnt.setInt(4, dateID);

											ResultSet rs5 = tbkStmnt.executeQuery();

											int aboveCount = 0;
											while (rs5.next()) {
												float close = rs5.getFloat(3);
												if (close > mcp) {
													aboveCount++;
												}
											}

											if (aboveCount == 3) {
												tbkFinal = tbkMax + 10;
											}
										}

									} else { // more than 8
										if ((dateID - lastestTBKDateID) > 30) {
											// more than 30 days gap, new signal
											tbkFinal = 8;
										} else {
											// ignore
										}

									}

								}

								// then update TBK
								// String query = "UPDATE BBROCK SET TBK=?
								// WHERE STOCKID =? and DATEID=?";

								updateTBK.setInt(1, tbkFinal);
								updateTBK.setInt(2, nextStockID);
								updateTBK.setInt(3, dateID);
								updateTBK.executeUpdate();
							} // end if (closePrice > mcp)
						} // if (rs3.next())
					} // if (rs2.next()) {

				} catch (Exception ex) {

				}
			} // while (rs1.next()) {

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	// this is the old logic using immediately previous 30 days'
	// sum(P+Y) and max lose price as reference
	// The new model relax the gap between breakout and rolling 30 days mark
	// Thus is more generic, also we could improve the requirement (bigger sum(Y+P))
	// and thus improving accuracy and catch more positive cases at the same time
	public static void processTodayTBK_OLD(int dateID, int stockId) {
		PreparedStatement getPureTeal = DB.getPureTeal();
		PreparedStatement updateTBK = DB.updateTBKStmnt();
		PreparedStatement past30Stmnt = DB.getPast30Stmnt();
		int pastDays = 30;
		float tier1 = 0.7f;
		float tier2 = 0.8f;
		float tier3 = 0.9f;
		float tier4 = 1.0f;
		float priceMaxLow = 0.01f;

		try {

			// String query = "select STOCKID FROM BBROCK WHERE DATEID=?
			// AND STOCKID>=? AND STOCKID<=? AND TEAL=1 AND YELLOW=0 AND
			// PINK=0 ORDER BY STOCKID ASC";
			getPureTeal.setInt(1, dateID);
			if (stockId <= 0) {
				getPureTeal.setInt(2, 0);
				getPureTeal.setInt(3, 1000000);
			} else {
				getPureTeal.setInt(2, stockId);
				getPureTeal.setInt(3, stockId);
			}

			ResultSet rs1 = getPureTeal.executeQuery();

			while (rs1.next()) {
				int nextStockID = rs1.getInt(1);
				try {
					System.out.println("Processing TBK at " + dateID + " for " + nextStockID);

					// String query = "SELECT MAX(CLOSE),SUM(YELLOW),SUM(PINK),
					// AVG(VOLUME) FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=?";

					past30Stmnt.setInt(1, nextStockID);
					past30Stmnt.setInt(2, dateID - pastDays);
					past30Stmnt.setInt(3, dateID - 1);

					ResultSet rs2 = past30Stmnt.executeQuery();
					if (rs2.next()) {
						float maxClose = rs2.getFloat(1);
						int sumYellow = rs2.getInt(2);
						int sumPink = rs2.getInt(3);
						float avgVol = rs2.getFloat(4);
						int totalBars = sumYellow + sumPink;
						// at least Y+P bar number in past days meets low bar
						if (totalBars >= tier1 * pastDays) {
							// get today
							past30Stmnt.setInt(1, nextStockID);
							past30Stmnt.setInt(2, dateID);
							past30Stmnt.setInt(3, dateID);
							ResultSet rs3 = past30Stmnt.executeQuery();
							if (rs3.next()) {
								float close = rs3.getFloat(1);
								float vol = rs3.getFloat(4);
								// -->TBK(58)/18 (18 if price<>) or 80%(>=24)-->TBK=68/28 or
								// 90%(>=27)-->TBK=78/38 or 100%(>=30)-->TBK=88/48, Teal number not considered
								// the last bar close price>max(previous 30 days) or at least within 1% (then
								// wait for new high)
								int tbk = 0;
								if (totalBars >= tier4 * pastDays) {// 100% Yellow
									if (close > maxClose) {
										tbk = 88;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 48;
									}
								} else if (totalBars >= tier3 * pastDays) {// 90% Yellow
									if (close > maxClose) {
										tbk = 78;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 38;
									}
								} else if (totalBars >= tier2 * pastDays) {// 80% Yellow
									if (close > maxClose) {
										tbk = 68;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 28;
									}
								} else if (totalBars >= tier1 * pastDays) {// 70% Yellow
									if (close > maxClose) {
										tbk = 58;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 18;
									}
								}

								// then update TBK
								// String query = "UPDATE BBROCK SET TBK=?
								// WHERE STOCKID =? and DATEID=?";

								updateTBK.setInt(1, tbk);
								updateTBK.setInt(2, nextStockID);
								updateTBK.setInt(3, dateID);
								updateTBK.executeUpdate();
							}
						}

					}
				} catch (Exception ex) {

				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	// process entire Boduang Delta (Delta of SAY*100+ Delta of IAYD) Alarm history
	public static void processBDAHistory() {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// from the 1st day that we have SAY data 9/25/2020
		// as SAY
		for (int w = 9007; w <= currentDateID; w++) {

			processTodayBDA(w, -1);

		}

	}

	public static void processStockBDAHistory(int stockID) {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// from the 1st day that we have SAY data 9/25/2020
		// as SAY
		for (int w = 9007; w <= currentDateID; w++) {

			processTodayBDA(w, stockID);

		}

	}

	public static void processTodayBDA(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();

			PreparedStatement stockInfoHistory = DB.getStockInfoHistory();
			PreparedStatement DBAdUpdate = DB.updateBDAStmnt();

			System.out.println("Processing BDA dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int currentIndId = 0;
			int currentSubIndId = 0;

			Hashtable IndStocks = new Hashtable();
			float indSumBDY = 0.0f;
			int indSumPDY = 0;
			while (rs.next()) {
				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
				// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
				// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=?
				// and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
				// ORDER BY MARKCAP ASC";

				subIndStockInfo.setInt(1, dateId);
				subIndStockInfo.setInt(2, currentIndId);
				subIndStockInfo.setInt(3, currentSubIndId);

				ResultSet rs0 = subIndStockInfo.executeQuery();

				while (rs0.next()) {
					// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
					// b.SYMBOL, close, BDY,PDY,SAY,IAYD FROM BBROCK a,
					// SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and
					// a.DATEID>=? and a.DATEID<=? and a.STOCKID=b.STOCKID
					// and a.STOCKID=? ORDER BY a.DATEID DESC";

					int stockId = rs0.getInt(3);

					stockInfoHistory.setInt(1, (dateId - 5));
					stockInfoHistory.setInt(2, dateId);
					stockInfoHistory.setInt(3, stockId);

					ResultSet rs1 = stockInfoHistory.executeQuery();

					float say1 = 0.0f;
					float say2 = 0.0f;
					float iayd1 = 0.0f;
					float iayd2 = 0.0f;

					int lcc = 0;
					while (rs1.next()) {
						String symb = rs1.getString(4);
						if (lcc == 0) {
							say1 = rs1.getFloat(8);
							iayd1 = rs1.getFloat(9);
						} else {
							say2 = rs1.getFloat(8);
							iayd2 = rs1.getFloat(9);

							boolean update = false;

							if (stockID > 0 && stockID == (20000 + stockId)) {
								update = true;
							} else if (stockID < 0) {
								update = true;
							}

							if (update) {

								// calculate BDA and update
								float BDA = (say1 - say2) * 100 + (iayd1 - iayd2);
								// String query = "UPDATE BBROCK SET BDA=?
								// WHERE STOCKID =? and DATEID=?";

								DBAdUpdate.setFloat(1, BDA);

								if (stockID > 0) {
									DBAdUpdate.setInt(2, stockID);
								} else {
									DBAdUpdate.setInt(2, stockId);
								}
								DBAdUpdate.setInt(3, dateId);
								DBAdUpdate.executeUpdate();
							}
						}

						lcc++;
						if (lcc >= 2) {
							lcc = 0;
							break;
						}

					}
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("processTodayBDA done");
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is done after individual stock calculation of such
	// this is at Industry sector and sub sector levels
	public static void processIndustryAVGPDYHistory(int buyPoint) {

		long t1 = System.currentTimeMillis();

		initBuyDateIDS(); // init buyDateIds array, currently manual update
		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		for (int w = 0; w < buyDateIds.length; w++) {
			int endDate = currentDateID;
			if ((w + 1) < buyDateIds.length) {
				endDate = buyDateIds[w + 1];
			}
			for (int k = buyDateIds[w] + 1; k <= endDate && buyDateIds[w] >= buyPoint; k++) {
				processTodayIndustryAVGPDY(k, -1);
			}

		}

	}

	public static void processTodayIndustryAVGPDY(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();
			PreparedStatement indAvgYieldUpdate = DB.getIndAvgYieldUpdateStmnt();
			PreparedStatement subIndAvgYieldUpdate = DB.getSubIndAvgYieldUpdateStmnt();

			System.out.println("Processing dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int preIndId = 0;
			int currentIndId = 0;
			int currentSubIndId = 0;

			Hashtable IndStocks = new Hashtable();
			float indSumBDY = 0.0f;
			int indSumPDY = 0;
			while (rs.next()) {
				if (preIndId == 0) {
					preIndId = rs.getInt(2);
				}

				if (currentIndId != preIndId) { // new industry
					// update industry avg for all stocks
					System.out.println("Processing dateId--" + dateId + " for Industry " + currentIndId);

					Enumeration en = IndStocks.keys();

					while (en.hasMoreElements()) {
						String sym = en.nextElement().toString();
						int stockId = Integer.parseInt(IndStocks.get(sym).toString());
						// String query = "UPDATE BBROCK SET IAY = ?, IPY=?
						// WHERE STOCKID = ? AND DATEID =? ";
						boolean update = false;
						if (stockID > 0 && stockID == (20000 + stockId)) {
							update = true;
						} else if (stockID < 0) {
							update = true;
						}

						if (update) {
							float iay = indSumBDY / (IndStocks.size() * 1.0f);
							float ipy = (indSumPDY * 1.0f) / (IndStocks.size() * 1.0f);
							indAvgYieldUpdate.setFloat(1, iay);
							indAvgYieldUpdate.setFloat(2, ipy);
							if (stockID > 0) {
								indAvgYieldUpdate.setInt(3, stockID);
							} else {
								indAvgYieldUpdate.setInt(3, stockId);
							}
							indAvgYieldUpdate.setInt(4, dateId);
							indAvgYieldUpdate.executeUpdate();
						}
					}
					// reset sum
					indSumBDY = 0.0f;
					indSumPDY = 0;
					IndStocks = new Hashtable();
				}

				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
				// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
				// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=?
				// and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
				// ORDER BY MARKCAP ASC";

				subIndStockInfo.setInt(1, dateId);
				subIndStockInfo.setInt(2, currentIndId);
				subIndStockInfo.setInt(3, currentSubIndId);

				ResultSet rs1 = subIndStockInfo.executeQuery();
				Hashtable subIndStocks = new Hashtable();
				float sumBDY = 0.0f;
				int sumPDY = 0;

				while (rs1.next()) {
					String symb = rs1.getString(4);
					int stockId = rs1.getInt(3);
					float bdy = rs1.getFloat(6);
					int pdy = rs1.getInt(7);
					subIndStocks.put(symb, "" + stockId);
					sumBDY = sumBDY + bdy;
					sumPDY = sumPDY + pdy;
				}

				indSumBDY = indSumBDY + sumBDY;
				indSumPDY = indSumPDY + sumPDY;

				float avgBDY = sumBDY / (subIndStocks.size() * 1.0f);
				float avgPDY = (sumPDY * 1.0f) / (subIndStocks.size() * 1.0f);

				Enumeration en = subIndStocks.keys();
				System.out.println("Processing dateId--" + dateId + " for Industry " + currentIndId + " sunInd "
						+ currentSubIndId);
				while (en.hasMoreElements()) {
					String sym = en.nextElement().toString();
					int stockId = Integer.parseInt(subIndStocks.get(sym).toString());
					IndStocks.put(sym, "" + stockId);
					// String query = "UPDATE BBROCK SET SAY = ?, SPY=?
					// WHERE STOCKID = ? AND DATEID =? ";

					boolean update = false;
					if (stockID > 0 && stockID == (20000 + stockId)) {
						update = true;
					} else if (stockID < 0) {
						update = true;
					}

					if (update) {
						subIndAvgYieldUpdate.setFloat(1, avgBDY);
						subIndAvgYieldUpdate.setFloat(2, avgPDY);

						if (stockID > 0) {
							subIndAvgYieldUpdate.setInt(3, stockID);
						} else {
							subIndAvgYieldUpdate.setInt(3, stockId);
						}
						subIndAvgYieldUpdate.setInt(4, dateId);
						subIndAvgYieldUpdate.executeUpdate();
					}
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("processTodayIndustryAVGPDY done");
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is done after individual stock calculation of such
	// this is at Industry sector and sub sector levels
	// compare the half smaller cap stocks performance against the bigger half
	// cap stocks of the same category AvgPDY (Delta)
	public static void processIndustryAVGPDYDeltaHistory(int buyPoint) {

		long t1 = System.currentTimeMillis();

		initBuyDateIDS(); // init buyDateIds array, currently manual update
		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		for (int w = 0; w < buyDateIds.length; w++) {
			int endDate = currentDateID;
			if ((w + 1) < buyDateIds.length) {
				endDate = buyDateIds[w + 1];
			}
			for (int k = buyDateIds[w] + 1; k <= endDate && buyDateIds[w] >= buyPoint; k++) {
				processTodayIndustryAVGPDYDelta(k, -1);
			}

		}

	}

	// calculate IAYD , each day Industry/subindustry category stock AVG BDY
	// for the smaller cap (half of total count) minus the bigger cap portion
	// as we observe a bull sector, smaller cap stocks rocket up harder than bigger
	// one
	// like SOLO, XPEV compared to NIO, TSLA around NOV, 2020
	public static void processTodayIndustryAVGPDYDelta(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();
			PreparedStatement subIndAvgYieldUpdate = DB.updateIndAvgYieldDelta();
			PreparedStatement subStockCount = DB.getSubIndStockCount();

			System.out.println("Processing IndustryAVGPDYDelta dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int preIndId = 0;
			int currentIndId = 0;
			int currentSubIndId = 0;

			while (rs.next()) {

				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT count(*) FROM BBROCK a,
				// SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and
				// a.DATEID=? and a.STOCKID=b.STOCKID and b.INDID=?
				// and b.SUBID=?";

				subStockCount.setInt(1, dateId);
				subStockCount.setInt(2, currentIndId);
				subStockCount.setInt(3, currentSubIndId);
				ResultSet rs0 = subStockCount.executeQuery();

				int totalSubCount = 0;
				if (rs0.next()) {
					totalSubCount = rs0.getInt(1);
				}

				if (totalSubCount > 0) {
					int halfCount = totalSubCount / 2 + totalSubCount % 2;
					// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
					// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
					// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=? and
					// a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
					// ORDER BY MARKCAP ASC";
					subIndStockInfo.setInt(1, dateId);
					subIndStockInfo.setInt(2, currentIndId);
					subIndStockInfo.setInt(3, currentSubIndId);

					Hashtable subIndStocks = new Hashtable();
					ResultSet rs1 = subIndStockInfo.executeQuery();
					Hashtable subIndStocks1 = new Hashtable();
					float sumBDY1 = 0.0f;
					int sumPDY1 = 0;

					Hashtable subIndStocks2 = new Hashtable();
					float sumBDY2 = 0.0f;
					int sumPDY2 = 0;

					if (currentIndId == 20 && currentSubIndId == 1 && dateId == 9047) {
						System.out.println("debugging....");
					}

					int lc = 0;
					while (rs1.next()) {
						lc++;
						String symb = rs1.getString(4);
						int stockId = rs1.getInt(3);
						float bdy = rs1.getFloat(6);
						int pdy = rs1.getInt(7);
						if (lc <= halfCount) {
							subIndStocks1.put(symb, "" + stockId);
							sumBDY1 = sumBDY1 + bdy;
							sumPDY1 = sumPDY1 + pdy;
						} else {
							subIndStocks2.put(symb, "" + stockId);
							sumBDY2 = sumBDY2 + bdy;
							sumPDY2 = sumPDY2 + pdy;
						}

						subIndStocks.put(symb, "" + stockId);
					}

					float avgBDY1 = sumBDY1 / (subIndStocks1.size() * 1.0f);
					float avgBDY2 = sumBDY2 / (subIndStocks2.size() * 1.0f);
					float iayd = avgBDY1 - avgBDY2;

					Enumeration en = subIndStocks.keys();
					System.out.println("Processing IAYD dateId--" + dateId + " for Industry " + currentIndId
							+ " sunInd " + currentSubIndId);
					while (en.hasMoreElements()) {
						String sym = en.nextElement().toString();
						int stockId = Integer.parseInt(subIndStocks.get(sym).toString());
						// String query = "UPDATE BBROCK SET IAYD=?
						// WHERE STOCKID = ? AND DATEID =? ";

						boolean update = false;
						if (stockID > 0 && stockID == (20000 + stockId)) {
							update = true;
						} else if (stockID < 0) {
							update = true;
						}

						if (update) {
							subIndAvgYieldUpdate.setFloat(1, iayd);
							if (stockID > 0) {
								subIndAvgYieldUpdate.setInt(2, stockID);
							} else {
								subIndAvgYieldUpdate.setInt(2, stockId);
							}
							subIndAvgYieldUpdate.setInt(3, dateId);
							try {
								subIndAvgYieldUpdate.executeUpdate();
							} catch (Exception ex) {
								ex.printStackTrace(System.out);
								System.out.println("iayd " + iayd + ", stockId " + stockId + ", dateId " + dateId);
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("processTodayIndustryAVGPDYDelta done");
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is individual stock calculation
	public static void processPDYHistory(int buyPoint) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = strtDateId + upDays; k <= endDateId; k++) {
				// for (int k = endDateId; k >= currentDateID; k--) {
				// 9007 is the latest buying point, hard-coded for now
				// int buyPoint = 9007;
				for (int k = buyPoint; k <= endDateId; k++) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k++;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist)
						processTodayPDY(stockID, k, buyPoint);
				}

				try {
					Thread.sleep(10);
				} catch (Exception ex) {

				}
				System.out.println("process done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayAllPDY(int dateId, int buyDateId, int stockID) {
		try {
			if (stockID < 0) {
				PreparedStatement allStocks = DB.getAllCurrentStockIDs();
				allStocks.setInt(1, dateId);

				ResultSet rs = allStocks.executeQuery();
				int sc = 0;
				System.out.println("-----------Begin---------");
				while (rs.next()) {
					sc++;
					stockID = rs.getInt(1);
					System.out.println("Processing stock " + stockID);
					processTodayPDY(stockID, dateId, buyDateId);
				}
			} else {
				processTodayPDY(stockID, dateId, buyDateId);
			}
			System.out.println("processTodayAllPDY done");
		} catch (Exception ex) {

		}
	}

	public static void processTodayPDY(int stockID, int dateId, int buyDateId) {
		try {
			// String query = "SELECT CLOSE,PDY,BDY FROM BBROCK
			// WHERE STOCKID = ? AND DATEID =? ";

			if (stockID == 5) {
				System.out.println("Testing...");
			}
			PreparedStatement closeStmnt = DB.getClosePriceStmnt();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			// String query = "UPDATE BBROCK SET BDY = ? , PDY=?
			// WHERE STOCKID = ? AND DATEID=?";

			PreparedStatement updateBDYPDY = DB.updateBDYPDY();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - 7);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == 2) { // the next day
					dateIdStartUp = dateIdStart;
					break;
				}
			}

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, dateId);
			ResultSet rs0 = closeStmnt.executeQuery();

			rs0.next();
			float close0 = rs0.getFloat(1);
			int pdy0 = rs0.getInt(2);
			float bdy0 = rs0.getFloat(3);

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, dateIdStartUp);
			ResultSet rs1 = closeStmnt.executeQuery();

			rs1.next();
			float close1 = rs1.getFloat(1);
			int pdy1 = rs1.getInt(2);
			float bdy1 = rs1.getFloat(3);

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, buyDateId);
			ResultSet rs2 = closeStmnt.executeQuery();

			rs2.next();
			float close2 = rs2.getFloat(1);
			int pdy2 = rs2.getInt(2);
			float bdy2 = rs2.getFloat(3);

			float bdy = 100.0f * (close0 - close2) / close2;
			int pdy = pdy1;
			if (close0 > close1) {
				pdy = pdy + 1;
			}

			// fresh buy point start, reset pdy = 1
			if ((dateId - buyDateId) == 1) {
				pdy = 1;
			}

			// String query = "UPDATE BBROCK SET BDY = ? , PDY=?
			// WHERE STOCKID = ? AND DATEID=?";
			updateBDYPDY.setFloat(1, bdy);
			updateBDYPDY.setInt(2, pdy);
			updateBDYPDY.setInt(3, stockID);
			updateBDYPDY.setInt(4, dateId);
			updateBDYPDY.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}
