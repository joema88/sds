package com.sds.db;

import java.sql.*;
import java.util.Calendar;

public class DB {

	/*
	 * //check DB size SELECT table_schema AS 'DB Name', ROUND(SUM(data_length +
	 * index_length) / 1024 / 1024, 1) AS 'DB Size in MB' FROM
	 * information_schema.tables GROUP BY table_schema;
	 */

	// SQL
	// select a.DATEID,CDATE, a.STOCKID, CLOSE, ATR,TEAL, YELLOW, PINK,
	// SC5,BT9,TSC5,DAYS,PTVAL, PTCP, PASS, CX520,CCX, b.SYMBOL FROM BBROCK a,
	// SYMBOLS b, DATES c WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and
	// b.SYMBOL='MAR' order by a.DATEID DESC limit 300;

	// create BBROCK table
	// 3. CREATE TABLE INDUSTRY(INDID SMALLINT, INDUSTRY VARCHAR(100), PRIMARY
	// KEY(INDID));
	// 4. CREATE TABLE SUBINDUSTRY(INDID SMALLINT, SUBID TINYINT UNSIGNED,
	// SUBINDUSTRY VARCHAR(100), PRIMARY KEY (SUBID, INDID),
	// FOREIGN KEY (INDID) REFERENCES INDUSTRY(INDID) ON DELETE CASCADE);
	// 1. CREATE TABLE SYMBOLS(STOCKID SMALLINT, SYMBOL VARCHAR(10), PRIMARY
	// KEY(STOCKID));
	// ALTER TABLE SYMBOLS ADD COLUMN INDID SMALLINT DEFAULT 0;
	// ALTER TABLE SYMBOLS ADD COLUMN SUBID TINYINT UNSIGNED DEFAULT 0;
	// 2. CREATE TABLE DATES(DATEID SMALLINT, CDATE DATE, PRIMARY KEY(DATEID));
	// ALTER TABLE DATES ADD COLUMN OS FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN OB FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN OY FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN AY FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN BAT FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN BOSY TINYINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN BI FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN CBI FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN SALY TINYINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN TOT SMALLINT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN BUY TINYINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN SL1 TINYINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN SL2 TINYINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN UC SMALLINT DEFAULT 0;
	// ALTER TABLE DATES ADD COLUMN AMC FLOAT DEFAULT 0.0;
	// ALTER TABLE DATES ADD COLUMN OBI SMALLINT DEFAULT 0;
	// OBI, an over bought indicator defined by query
	// select a.DATEID, CDATE, COUNT(*) FROM BBROCK a, DATES c WHERE
	// a.DATEID=c.DATEID
	// and PASS>3 AND CCX>0 AND CCX<=14
	// group by a.DATEID, CDATE order by a.DATEID DeSC limit 320;
	// ALTER TABLE DATES ADD COLUMN F1 SMALLINT DEFAULT 0; //FUC>0 NUMBER COUNT OF
	// THE DAY
	// ALTER TABLE DATES ADD COLUMN F8 SMALLINT DEFAULT 0; //FUC=8 NUMBER COUNT OF
	// THE DAY
	
	// 3. CREATE TABLE BBROCK(STOCKID SMALLINT, DATEID SMALLINT, PERCENT FLOAT
	// DEFAULT 0.0, CLOSE FLOAT DEFAULT 0.0,NETCHANGE FLOAT DEFAULT 0.0, ATR FLOAT
	// DEFAULT 0.0, OPEN FLOAT DEFAULT 0.0,HIGH FLOAT DEFAULT 0.0, LOW FLOAT DEFAULT
	// 0.0,LOW52 FLOAT DEFAULT 0.0,HIGH52 FLOAT DEFAULT 0.0, MARKCAP FLOAT DEFAULT
	// 0.0,VOLUME INT DEFAULT 0, YELLOW TINYINT DEFAULT 0,TEAL TINYINT DEFAULT 0,
	// PINK TINYINT DEFAULT 0, SC5 SMALLINT DEFAULT 0,YP10 TINYINT DEFAULT 0, BT9
	// SMALLINT DEFAULT 0,TSC5 SMALLINT DEFAULT 0,DAYS SMALLINT DEFAULT 0, PTVAL
	// FLOAT DEFAULT 0.0, PTCP FLOAT DEFAULT 0.0, PASS TINYINT DEFAULT 0,CX520
	// TINYINT DEFAULT 0, CCX SMALLINT DEFAULT 0 ,BDCX SMALLINT DEFAULT 0, BDW
	// SMALLINT DEFAULT 0,PTCP2 FLOAT DEFAULT 0.0, DAY2 SMALLINT DEFAULT 0, ABT9
	// TINYINT DEFAULT 0, APAS TINYINT DEFAULT 0, PRIMARY KEY (STOCKID, DATEID),APTV
	// FLOAT DEFAULT 0.0,
	// ALTER TABLE BBROCK ADD COLUMN BPY SMALLINT DEFAULT 0;
	// SYPT INT DEFAULT 0, FOREIGN KEY (STOCKID) REFERENCES SYMBOLS(STOCKID) ON
	// DELETE CASCADE, FOREIGN KEY (DATEID) REFERENCES DATES(DATEID));
	// ALTER TABLE BBROCK ADD COLUMN IAY FLOAT DEFAULT 0.0; //Industry sector avg
	// BDY
	// ALTER TABLE BBROCK ADD COLUMN IPY FLOAT DEFAULT 0.0; //Industry sector avg
	// PDY
	// ALTER TABLE BBROCK ADD COLUMN SAY FLOAT DEFAULT 0.0; //Industry sub sector
	// sector avg BDY
	// ALTER TABLE BBROCK ADD COLUMN SPY FLOAT DEFAULT 0.0; //Industry sub sector
	// avg PDY
	// ALTER TABLE BBROCK ADD COLUMN IAYD FLOAT DEFAULT 0.0;
		//-- INDUSTRY AVERAGE YIELD DELTA, EACH DAY, FOR A INDUSTRY/sub-industry GROUP ORDER BY MARKCAP ASC
		//-- AVERAGE THE BDY (SUM/COUNT) THEN DELTA TWO VALUE (SMALL CAP AVERAGE - BIG CAP AVERAGE)
		//-- THIS IS TO INDICATE IF AN INDUSTRY IS IN UPSWING AND COMPETITIVENESS RELATED TO MARKCAP

	// ALTER TABLE BBROCK ADD COLUMN DD FLOAT DEFAULT 0.0; //Days to deplete the
	// stock shares, markcap/(vol*close);
	// ALTER TABLE BBROCK ADD COLUMN D9 FLOAT DEFAULT 0.0; //AVG of last 10 days of
	// D2 to avoid 1 day volume spike
	// TRACK DD WITHIN 3 DAYS REDUCE 2/3 OR MORE WHILE D9 REDUCED LESS THAN 10%
	// BASED ON D9 UP/DOWN AND SCALE, WE MAY ASSIGN MINOR VALUE
	// THIS IS IS AN EXTREMELY SHORT TERM BULLISH SIGNAL WEEKS TO ONE MONTH
	// ALTER TABLE BBROCK ADD COLUMN VBI TINYINT DEFAULT 0;
	// ALTER TABLE BBROCK ADD COLUMN EE8 TINYINT DEFAULT 0; //VBI=118 and FUC=8 within 3 days, then EE8=8
    // ALTER TABLE BBROCK ADD COLUMN BDA SMALLINT DEFAULT 0; //Boduang Delta (of SAY) Delta (of IAYD) Sum
	//This field indicates the delta values of two fields SAY (sub-industry accumlated yield of mini-bull boduang)
	//and delta of IAYD (Industry(sub) accumlated yield delta(bottom substract top half marketcap stocks yield)
	//formula: [SAY at n+1 (dateid) - SAY at n (dateId)]*100+[IAYD at n+1 (dateid) - IAYD at n (dateid)]
	/*
	 * ALTER TABLE BBROCK ADD COLUMN TSM SMALLINT DEFAULT 0; //Teal color total
	 * summary between two days ALTER TABLE BBROCK ADD COLUMN TOM TINYINT DEFAULT 0;
	 * //Teal color moving one month summary ALTER TABLE BBROCK ADD COLUMN PSM
	 * SMALLINT DEFAULT 0; //Pink color total summary between two days ALTER TABLE
	 * BBROCK ADD COLUMN POM TINYINT DEFAULT 0; //Pink color moving one month
	 * summary ALTER TABLE BBROCK ADD COLUMN YSM SMALLINT DEFAULT 0; //Yellow color
	 * total summary between two days ALTER TABLE BBROCK ADD COLUMN YOM TINYINT
	 * DEFAULT 0; //Yellow color moving one month summary ALTER TABLE BBROCK ADD
	 * COLUMN NSM SMALLINT DEFAULT 0; //No color total summary between two days
	 * ALTER TABLE BBROCK ADD COLUMN NOM TINYINT DEFAULT 0; //No color moving one
	 * month summary ALTER TABLE BBROCK ADD COLUMN DSM SMALLINT DEFAULT 0; //Total
	 * days ALTER TABLE BBROCK ADD COLUMN MOR FLOAT DEFAULT 0.0; //the current month
	 * color rank measurement ALTER TABLE BBROCK ADD COLUMN YOR FLOAT DEFAULT 0.0;
	 * // 1 year color rank measurement ALTER TABLE BBROCK ADD COLUMN TRK FLOAT
	 * DEFAULT 0.0; //Total color rank since having data ALTER TABLE BBROCK ADD
	 * COLUMN DM FLOAT DEFAULT 0.0; //MAX DELTA OF CURRENT UPC AND PAST 250 DAYS'
	 * DPC ALTER TABLE BBROCK ADD COLUMN DA TINYINT UNSIGNED DEFAULT 0; //DM DAY
	 * DIFFERENCE ALTER TABLE BBROCK ADD COLUMN ADM FLOAT DEFAULT 0.0; //AVG DM OF
	 * THE PAST 250 DAYS ALTER TABLE BBROCK ADD COLUMN RK TINYINT UNSIGNED DEFAULT
	 * 0; //RANK OF CURRENT DM COMPARED TO LAST 250 DAYS, HIGHEST RK = 1, LOWEST
	 * =250 ALTER TABLE BBROCK ADD COLUMN FUC TINYINT UNSIGNED DEFAULT 0; //UPC 1st
	 * 40 occurance within 30(or 40?) days, if accompanied by DM>100, then 8 //
	 * ALTER TABLE BBROCK ADD COLUMN BDY FLOAT DEFAULT 0.0; //from buy to sell point
	 * every day total yield // ALTER TABLE BBROCK ADD COLUMN PDY TINYINT UNSIGNED
	 * DEFAULT 0; //positive gain days to achieve this yield
	 * 
	 * //else then 4, check sequential of 4s to see if the second 4 price>1st 4
	 * price like DOCU,ZM etc. for 8, then one 8 is enough. The rest of UPC>40 //is
	 * denoted by 1, so it is convenient to calculate if another UPC>40 is within
	 * the reach of 30 days //dateId - 252 ( 1 year ) //mor = 1.0f * (tom - yom - 2
	 * * pom) / 25.0f; //float trk = 1.0f * (tSum - ySum - 2 * pSum) / (1.0f *
	 * dSUM); //yor = 1.0f * ((tSum - tSum1) - (ySum - ySum1) - 2 * (pSum - pSum1))
	 * / (1.0f * (dSUM - dSUM1)); ALTER TABLE BBROCK ADD COLUMN UPC FLOAT DEFAULT
	 * 0.0; //up percentage from the lowest (after highest) within last 30 days
	 * ALTER TABLE BBROCK ADD COLUMN UDS TINYINT UNSIGNED DEFAULT 0; //days between
	 * current and lowest after (after highest) within last 30 days ALTER TABLE
	 * BBROCK ADD COLUMN DPC FLOAT DEFAULT 0.0; //down percentage from the highest
	 * within last 30 days ALTER TABLE BBROCK ADD COLUMN DDS TINYINT UNSIGNED
	 * DEFAULT 0; //days between current and highest within last 30 days ALTER TABLE
	 * BBROCK ADD COLUMN FUC TINYINT UNSIGNED DEFAULT 0; //UPC>40 and DM>100 first
	 * occurence past 30 days 8, only UPC>40, then 4, otherwise, if not first
	 * occurence, then assign value 1
	 */
	// ALTER TABLE BBROCK ADD COLUMN TBK TINYINT UNSIGNED DEFAULT 0;
	// TBK means 30 days breakout bullish pattern based on Teal, Yellow, Pink color number
	// The last bar must be pure Teal, and previous 30 days the sum of Yellow and Pink>=70%(21)
	// -->TBK(58)/18 (18 if price<>) or 80%(>=24)-->TBK=68/28 or 90%(>=27)-->TBK=78/38 or 100%(>=30)-->TBK=88/48, Teal number not considered
	//the last bar close price>max(previous 30 days) or at least within 1% (then wait for new high)
	//Currently it is implemented as immediately 30 days before, but in reality there may be a gap
	//between qualified Yellow concentrated area and breakout like NKE, so a better
	//implementation would be keep track 30 Y+P saturation, then find breakout between>=95% saturation
	//and breakout point, regardless the gap in between situation.
	//basically --consolidation peroid + plus gap with no breakout and no clear pattern --> breakout
	
	//TBK is a special case of a more general 30 breakout pattern: any previous 30 days
	// if SUM(Yellow+Pink)>=27 (90%), then it is a consolidating phase, if a later day
	//breakout the highest of that 30 days' highest, then it is likely a bull buy entry point
	// In this scenario, the gap between the breakout and consolidation 30 days doesn't have to be
	//fixed (immediately previous as last case). However, the breakout day must be pure Teal color
	// Also, the breakout must only check the immediately closest consolidation and its max price.
	//To avoid data duplication, no new breakout signal in the next 30 days after given first
	//Also no more new signal at least new consolidation phase appears or certain number of sum (Y+P) of rolling 30 days (>=15??? example, to be determined).
	//Calculation method, rolling 30 days from old to new, sum(Y+P), if sum>=27(90%), recording max(Close) along with it
	//Otherwise just take the sum. RTS (Rolling 30 days' sum), MCP (max close price)
	//Based on RTS, MCP and current day close price, pure Teal, we decide TBK (We need to get rid of old implementation logic)
	//ALTER TABLE BBROCK ADD COLUMN RTS TINYINT UNSIGNED DEFAULT 0;
	//ALTER TABLE BBROCK ADD COLUMN MCP FLOAT DEFAULT 0.0;
	private static Connection dbcon = null;
	private static PreparedStatement symbolStmnt = null;
	private static PreparedStatement symbolDateIDQuery = null;
	private static PreparedStatement dateStmnt = null;
	private static PreparedStatement rockStmnt = null;
	private static PreparedStatement backTestStmnt = null;
	private static PreparedStatement backTestTealUpdateStmnt = null;
	private static PreparedStatement backTestYellowUpdateStmnt = null;
	private static PreparedStatement backTestPinkUpdateStmnt = null;
	private static PreparedStatement typSumQueryStmnt = null;
	private static PreparedStatement scUpdateStmnt = null;
	private static PreparedStatement queryTealStmnt = null;
	private static PreparedStatement updateT9Stmnt = null;
	private static PreparedStatement update520CXStmnt = null;
	private static PreparedStatement queryStockIDStmnt = null;
	private static PreparedStatement typSumbyStockIDStmnt = null;
	private static Statement stmnt = null;
	private static PreparedStatement yp10SumCalStmnt = null;
	private static PreparedStatement yp10SumUpdateStmnt = null;
	private static PreparedStatement update520CXRange = null;
	private static PreparedStatement closePriceStmnt = null;
	private static PreparedStatement lastCloseStmnt = null;
	private static PreparedStatement cx520Stmnt = null;
	private static PreparedStatement SYPTStmnt = null;
	private static PreparedStatement SYPTUpdate = null;
	private static PreparedStatement checkHistoryExists = null;
	private static PreparedStatement stockIds = null;
	private static PreparedStatement dBullStmnt = null;
	private static PreparedStatement queryPYStmnt = null;
	private static PreparedStatement updateBPYStmnt = null;
	private static PreparedStatement stocks = null;
	private static PreparedStatement tealSumStmnt = null;
	private static PreparedStatement dailyPriceStmnt = null;
	private static PreparedStatement stockIDStmnt = null;
	private static PreparedStatement startDateIdStmnt = null;
	private static PreparedStatement colorSumUpdateStmnt = null;
	private static PreparedStatement colorOMUpdateStmnt = null;
	private static PreparedStatement colorSumStmnt = null;
	private static PreparedStatement noColorSumStmnt = null;
	private static PreparedStatement colorRankingUpdateStmnt = null;
	private static PreparedStatement colorCalSumStmnt = null;
	private static PreparedStatement colorRankSumStmnt = null;
	private static PreparedStatement dateIDRange = null;
	private static PreparedStatement dateIdByPrice = null;
	private static PreparedStatement minClose = null;
	private static PreparedStatement maxClose = null;
	private static PreparedStatement priceByDateID = null;
	private static PreparedStatement distanceChangeUpdate = null;
	private static PreparedStatement dateIDStmnt = null;
	private static PreparedStatement dateIDExistStmnt = null;
	private static PreparedStatement oldStocks = null;
	private static PreparedStatement dateIDStartStmnt = null;
	private static PreparedStatement qualifiedLowStmnt = null;
	private static PreparedStatement dateIdByUPC = null;
	private static PreparedStatement dateIdByDPC = null;
	private static PreparedStatement updateMaxUpDown = null;
	private static PreparedStatement upcStmnt = null;
	private static PreparedStatement resetUpDownToZero = null;
	private static PreparedStatement currentUPC = null;
	private static PreparedStatement dmRank = null;
	private static PreparedStatement avgDM = null;
	private static PreparedStatement updateDMRankAvg = null;
	private static PreparedStatement fucf = null;
	private static PreparedStatement fud = null;
	private static PreparedStatement updateFUC = null;
	private static PreparedStatement markCapStmnt = null;
	private static PreparedStatement BosyUpdate = null;
	private static PreparedStatement BOYSStmnt = null;
	private static PreparedStatement CBIUpdate = null;
	private static PreparedStatement BuySellStmnt = null;
	private static PreparedStatement BuySellUpdate = null;
	private static PreparedStatement currentUTurnStocks = null;
	private static PreparedStatement UMACUpdate = null;
	private static PreparedStatement updateBDYPDY = null;
	private static PreparedStatement UTurnStmnt = null;
	private static PreparedStatement insertIndStmnt = null;
	private static PreparedStatement IndStmnt = null;
	private static PreparedStatement insertSubIndStmnt = null;
	private static PreparedStatement subIndStmnt = null;
	private static PreparedStatement subUnderIndStmnt = null;
	private static PreparedStatement updateStockIndustryCode = null;
	private static PreparedStatement IndCodeStmnt = null;
	private static PreparedStatement indIdStmnt = null;
	private static PreparedStatement indAvgYieldUpdate = null;
	private static PreparedStatement subIndAvgYieldUpdate = null;
	private static PreparedStatement subIndStockInfo = null;
	private static PreparedStatement updateStockSectorStmnt = null;
	private static PreparedStatement OBIHistoryStmnt = null;
	private static PreparedStatement UpdateOBIStmnt = null;
	private static PreparedStatement f1UpdateStmnt = null;
	private static PreparedStatement f8UpdateStmnt = null;
	private static PreparedStatement fucStmnt = null;
	private static PreparedStatement originalDataStmnt = null;
	private static PreparedStatement insertDataStmnt = null;
	private static PreparedStatement stockSymbol = null;
	private static PreparedStatement deleteStockRecord = null;
	private static PreparedStatement todayOBIStmnt = null;
	private static PreparedStatement fucTodayStmnt = null;
	private static PreparedStatement minDPC = null;
	private static PreparedStatement updatePDYToOne = null;
	private static PreparedStatement StockDateIDAsc = null;
	private static PreparedStatement updateD2 = null;
	private static PreparedStatement updateD9 = null;
	private static PreparedStatement avgD2 = null;
	private static PreparedStatement updateVBIStmnt= null;
	private static PreparedStatement DDD9Stmnt = null;
	private static PreparedStatement deleteSymb = null;
	private static PreparedStatement updateStockID = null;
	private static PreparedStatement updateEE8 = null;
	private static PreparedStatement stkvbifx = null;
	private static PreparedStatement vbifx = null;
	private static PreparedStatement cDateId= null;
	private static PreparedStatement subIndStockCount = null;
	private static PreparedStatement updateIndAvgYieldDelta = null;
	private static PreparedStatement updateBDAStmnt = null;
	private static PreparedStatement stockInfoHistory = null;
	private static PreparedStatement pureTeal = null;
	private static PreparedStatement past30Stmnt = null;
	private static PreparedStatement updateTBKStmnt = null;
	private static PreparedStatement updateRtsMcp = null;
	private static PreparedStatement resetTBKStmnt = null;
	private static PreparedStatement closestMCPStmnt = null;
	private static PreparedStatement tbkStmnt = null;
	
	public static void closeConnection() {
		try {
			if( tbkStmnt != null) {
				tbkStmnt.close();
				tbkStmnt = null;
			}
			if( closestMCPStmnt != null) {
				closestMCPStmnt.close();
				closestMCPStmnt = null;
			}
			if( resetTBKStmnt != null) {
				resetTBKStmnt.close();
				resetTBKStmnt = null;
			}
			if( updateRtsMcp != null) {
				updateRtsMcp.close();
				updateRtsMcp = null;
			}
			if( updateTBKStmnt != null) {
				updateTBKStmnt.close();
				updateTBKStmnt = null;
			}
			if( past30Stmnt != null) {
				past30Stmnt.close();
				past30Stmnt = null;
			}
			if(pureTeal != null) {
				pureTeal.close();
				pureTeal = null;
			}
			if( stockInfoHistory != null) {
				stockInfoHistory.close();
				stockInfoHistory = null;
			}
			if( updateBDAStmnt != null) {
				updateBDAStmnt.close();
				updateBDAStmnt = null;
			}
			if(updateIndAvgYieldDelta != null) {
				updateIndAvgYieldDelta.close();
				updateIndAvgYieldDelta = null;
			}
			if(subIndStockCount != null) {
				subIndStockCount.close();
				subIndStockCount = null;
			}
			if(cDateId != null) {
				cDateId.close();
				cDateId = null;
			}
			if(vbifx != null) {
				vbifx.close();
				vbifx  = null;
			}
			if( stkvbifx != null) {
				stkvbifx.close();
				stkvbifx = null;
			}
			if( updateEE8 != null) {
				updateEE8.close();
				updateEE8 = null;
			}
			if(updateStockID != null) {
				updateStockID.close();
				updateStockID = null;
			}
			if(deleteSymb !=  null) {
				deleteSymb.close();
				deleteSymb = null;
			}
			if(DDD9Stmnt != null) {
				DDD9Stmnt.close();
				DDD9Stmnt = null;
			}
			if( updateVBIStmnt != null ) {
				updateVBIStmnt.close();
				updateVBIStmnt = null;
			}
			if (avgD2 != null) {
				avgD2.close();
				avgD2 = null;
			}
			if (updateD2 != null) {
				updateD2.close();
				updateD2 = null;
			}
			if (updateD9 != null) {
				updateD9.close();
				updateD9 = null;
			}
			if (StockDateIDAsc != null) {
				StockDateIDAsc.close();
				StockDateIDAsc = null;
			}
			if (updatePDYToOne != null) {
				updatePDYToOne.close();
				updatePDYToOne = null;
			}
			if (minDPC != null) {
				minDPC.close();
				minDPC = null;
			}
			if (fucTodayStmnt != null) {
				fucTodayStmnt.close();
				fucTodayStmnt = null;
			}
			if (todayOBIStmnt != null) {
				todayOBIStmnt.close();
				todayOBIStmnt = null;
			}
			if (deleteStockRecord != null) {
				deleteStockRecord.close();
				deleteStockRecord = null;
			}
			if (stockSymbol != null) {
				stockSymbol.close();
				stockSymbol = null;
			}
			if (insertDataStmnt != null) {
				insertDataStmnt.close();
				insertDataStmnt = null;
			}
			if (originalDataStmnt != null) {
				originalDataStmnt.close();
				originalDataStmnt = null;
			}
			if (fucStmnt != null) {
				fucStmnt.close();
				fucStmnt = null;
			}
			if (f1UpdateStmnt != null) {
				f1UpdateStmnt.close();
				f1UpdateStmnt = null;
			}
			if (f8UpdateStmnt != null) {
				f8UpdateStmnt.close();
				f8UpdateStmnt = null;
			}
			if (OBIHistoryStmnt != null) {
				OBIHistoryStmnt.close();
				OBIHistoryStmnt = null;
			}
			if (UpdateOBIStmnt != null) {
				UpdateOBIStmnt.close();
				UpdateOBIStmnt = null;
			}
			if (updateStockSectorStmnt != null) {
				updateStockSectorStmnt.close();
				updateStockSectorStmnt = null;
			}
			if (subIndStockInfo != null) {
				subIndStockInfo.close();
				subIndStockInfo = null;
			}
			if (subIndAvgYieldUpdate != null) {
				subIndAvgYieldUpdate.close();
				subIndAvgYieldUpdate = null;
			}
			if (indAvgYieldUpdate != null) {
				indAvgYieldUpdate.close();
				indAvgYieldUpdate = null;
			}
			if (indIdStmnt != null) {
				indIdStmnt.close();
				indIdStmnt = null;
			}
			if (IndCodeStmnt != null) {
				IndCodeStmnt.close();
				IndCodeStmnt = null;
			}
			if (updateStockIndustryCode != null) {
				updateStockIndustryCode.close();
				updateStockIndustryCode = null;
			}
			if (subUnderIndStmnt != null) {
				subUnderIndStmnt.close();
				subUnderIndStmnt = null;
			}
			if (insertSubIndStmnt != null) {
				insertSubIndStmnt.close();
				insertSubIndStmnt = null;
			}
			if (subIndStmnt != null) {
				subIndStmnt.close();
				subIndStmnt = null;
			}
			if (insertIndStmnt != null) {
				insertIndStmnt.close();
				insertIndStmnt = null;
			}
			if (IndStmnt != null) {
				IndStmnt.close();
				IndStmnt = null;
			}
			if (UTurnStmnt != null) {
				UTurnStmnt.close();
				UTurnStmnt = null;
			}
			if (UMACUpdate != null) {
				UMACUpdate.close();
				UMACUpdate = null;
			}
			if (currentUTurnStocks != null) {
				currentUTurnStocks.close();
				currentUTurnStocks = null;
			}
			if (BuySellUpdate != null) {
				BuySellUpdate.close();
				BuySellUpdate = null;
			}
			if (BuySellStmnt != null) {
				BuySellStmnt.close();
				BuySellStmnt = null;
			}
			if (CBIUpdate != null) {
				CBIUpdate.close();
				CBIUpdate = null;
			}
			if (BosyUpdate != null) {
				BosyUpdate.close();
				BosyUpdate = null;
			}
			if (BOYSStmnt != null) {
				BOYSStmnt.close();
				BOYSStmnt = null;
			}
			if (fucf != null) {
				fucf.close();
				fucf = null;
			}
			if (fud != null) {
				fud.close();
				fud = null;
			}
			if (updateFUC != null) {
				updateFUC.close();
				updateFUC = null;
			}
			if (updateDMRankAvg != null) {
				updateDMRankAvg.close();
				updateDMRankAvg = null;
			}
			if (avgDM != null) {
				avgDM.close();
				avgDM = null;
			}
			if (dmRank != null) {
				dmRank.close();
				dmRank = null;
			}
			if (currentUPC != null) {
				currentUPC.close();
				currentUPC = null;
			}
			if (resetUpDownToZero != null) {
				resetUpDownToZero.close();
				resetUpDownToZero = null;
			}
			if (upcStmnt != null) {
				upcStmnt.close();
				upcStmnt = null;
			}

			if (updateMaxUpDown != null) {
				updateMaxUpDown.close();
				updateMaxUpDown = null;
			}
			if (dateIdByDPC != null) {
				dateIdByDPC.close();
				dateIdByDPC = null;
			}
			if (dateIdByUPC != null) {
				dateIdByUPC.close();
				dateIdByUPC = null;
			}
			if (qualifiedLowStmnt != null) {
				qualifiedLowStmnt.close();
				qualifiedLowStmnt = null;
			}
			if (dateIDStartStmnt != null) {
				dateIDStartStmnt.close();
				dateIDStartStmnt = null;
			}
			if (oldStocks != null) {
				oldStocks.close();
				oldStocks = null;
			}
			if (dateIDStmnt != null) {
				dateIDStmnt.close();
				dateIDStmnt = null;
			}
			if (distanceChangeUpdate != null) {
				distanceChangeUpdate.close();
				distanceChangeUpdate = null;
			}
			if (priceByDateID != null) {
				priceByDateID.close();
				priceByDateID = null;
			}
			if (dateIDRange != null) {
				dateIDRange.close();
				dateIDRange = null;
			}
			if (minClose != null) {
				minClose.close();
				minClose = null;
			}
			if (maxClose != null) {
				maxClose.close();
				maxClose = null;
			}
			if (dateIDRange != null) {
				dateIDRange.close();
				dateIDRange = null;
			}
			if (colorCalSumStmnt != null) {
				colorCalSumStmnt.close();
				colorCalSumStmnt = null;
			}
			if (colorRankingUpdateStmnt != null) {
				colorRankingUpdateStmnt.close();
				colorRankingUpdateStmnt = null;
			}
			if (noColorSumStmnt != null) {
				noColorSumStmnt.close();
				noColorSumStmnt = null;
			}
			if (colorSumStmnt != null) {
				colorSumStmnt.close();
				colorSumStmnt = null;
			}
			if (colorOMUpdateStmnt != null) {
				colorOMUpdateStmnt.close();
				colorOMUpdateStmnt = null;
			}
			if (colorSumUpdateStmnt != null) {
				colorSumUpdateStmnt.close();
				colorSumUpdateStmnt = null;
			}
			if (startDateIdStmnt != null) {
				startDateIdStmnt.close();
				startDateIdStmnt = null;
			}
			if (stockIDStmnt != null) {
				stockIDStmnt.close();
				stockIDStmnt = null;
			}
			if (dailyPriceStmnt != null) {
				dailyPriceStmnt.close();
				dailyPriceStmnt = null;
			}
			if (tealSumStmnt != null) {
				tealSumStmnt.close();
				tealSumStmnt = null;
			}
			if (stocks != null) {
				stocks.close();
				stocks = null;
			}
			if (updateBPYStmnt != null) {
				updateBPYStmnt.close();
				updateBPYStmnt = null;
			}
			if (queryPYStmnt != null) {
				queryPYStmnt.close();
				queryPYStmnt = null;
			}
			if (stockIds != null) {
				stockIds.close();
				stockIds = null;
			}
			if (dBullStmnt != null) {
				dBullStmnt.close();
				dBullStmnt = null;
			}
			if (checkHistoryExists != null) {
				checkHistoryExists.close();
				checkHistoryExists = null;
			}
			if (SYPTUpdate != null) {
				SYPTUpdate.close();
				SYPTUpdate = null;
			}
			if (SYPTStmnt != null) {
				SYPTStmnt.close();
				SYPTStmnt = null;
			}
			if (cx520Stmnt != null) {
				cx520Stmnt.close();
				cx520Stmnt = null;
			}
			if (lastCloseStmnt != null) {
				lastCloseStmnt.close();
				lastCloseStmnt = null;
			}
			if (closePriceStmnt != null) {
				closePriceStmnt.close();
				closePriceStmnt = null;
			}
			if (update520CXStmnt != null) {
				update520CXStmnt.close();
				update520CXStmnt = null;
			}
			if (update520CXRange != null) {
				update520CXRange.close();
				update520CXRange = null;
			}
			if (yp10SumUpdateStmnt != null) {
				yp10SumUpdateStmnt.close();
				yp10SumUpdateStmnt = null;
			}
			if (yp10SumCalStmnt != null) {
				yp10SumCalStmnt.close();
				yp10SumCalStmnt = null;
			}
			if (typSumbyStockIDStmnt != null) {
				typSumbyStockIDStmnt.close();
				typSumbyStockIDStmnt = null;
			}
			if (queryStockIDStmnt != null) {
				queryStockIDStmnt.close();
				queryStockIDStmnt = null;
			}
			if (queryTealStmnt != null) {
				queryTealStmnt.close();
				queryTealStmnt = null;
			}
			if (scUpdateStmnt != null) {
				scUpdateStmnt.close();
				scUpdateStmnt = null;
			}
			if (typSumQueryStmnt != null) {
				typSumQueryStmnt.close();
				typSumQueryStmnt = null;
			}
			if (symbolDateIDQuery != null) {
				symbolDateIDQuery.close();
				symbolDateIDQuery = null;
			}
			if (backTestYellowUpdateStmnt != null) {
				backTestYellowUpdateStmnt.close();
				backTestYellowUpdateStmnt = null;
			}
			if (backTestPinkUpdateStmnt != null) {
				backTestPinkUpdateStmnt.close();
				backTestPinkUpdateStmnt = null;
			}
			if (backTestTealUpdateStmnt != null) {
				backTestTealUpdateStmnt.close();
				backTestTealUpdateStmnt = null;
			}
			if (backTestStmnt != null) {
				backTestStmnt.close();
				backTestStmnt = null;
			}
			if (stmnt != null) {
				stmnt.close();
				stmnt = null;
			}
			if (symbolStmnt != null) {
				symbolStmnt.close();
				symbolStmnt = null;
			}
			if (dateStmnt != null) {
				dateStmnt.close();
				dateStmnt = null;
			}
			if (rockStmnt != null) {
				rockStmnt.close();
				rockStmnt = null;
			}

			if (dbcon != null) {
				dbcon.close();
				dbcon = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() {

		try {
			if (dbcon == null) {
				dbcon = DriverManager.getConnection(
						"jdbc:mysql://127.0.0.1:3306/SDS?allowPublicKeyRetrieval=true&useSSL=false", "root",
						"Goldfish@3224");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return dbcon;
	}

	// GET OLD STOCKS
	public static PreparedStatement getOldStockIDs() {
		if (oldStocks == null) {
			try {
				String query = "SELECT DISTINCT(STOCKID) FROM  BBROCK WHERE DATEID=?";
				oldStocks = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return oldStocks;
	}

	// SYPTStmnt
	public static PreparedStatement getAllStockIDs() {
		if (stockIds == null) {
			try {
				String query = "SELECT DISTINCT(STOCKID) FROM  BBROCK WHERE STOCKID>=?";
				stockIds = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stockIds;
	}

	public static PreparedStatement getAllCurrentStockIDs() {
		if (stockIds == null) {
			try {
				String query = "SELECT DISTINCT(STOCKID) FROM  BBROCK WHERE DATEID = ? ";
				stockIds = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stockIds;
	}

	public static PreparedStatement getAllStocks() {
		if (stocks == null) {
			try {
				String query = "SELECT SYMBOL FROM  SYMBOLS WHERE STOCKID>= ?";
				stocks = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stocks;
	}

	public static PreparedStatement getStockSymbol() {
		if (stockSymbol == null) {
			try {
				String query = "SELECT SYMBOL FROM  SYMBOLS WHERE STOCKID = ?";
				stockSymbol = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stockSymbol;
	}

	public static PreparedStatement getStockDateIDRange() {
		if (dateIDRange == null) {
			try {
				String query = "select MIN(DATEID), MAX(DATEID) FROM BBROCK WHERE STOCKID = ? ";
				dateIDRange = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dateIDRange;
	}

	// SELECT DATEID, CLOSE, DPC, UPC FROM BBROCK WHERE STOCKID=2626
	// AND DATEID<=8720 AND DATEID>8470 ORDER BY CLOSE DESC, DATEID DESC LIMIT 20;
	public static PreparedStatement getMaxClose() {
		if (maxClose == null) {
			try {
				String query = "select DATEID, CLOSE FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?  ORDER BY CLOSE DESC, DATEID DESC LIMIT 20";
				maxClose = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return maxClose;
	}

	public static PreparedStatement getMaxUPC() {
		if (maxClose == null) {
			try {
				String query = "select MAX(UPC) FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?";
				maxClose = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return maxClose;
	}

	public static PreparedStatement getCurrentUTurnStocks() {
		if (currentUTurnStocks == null) {
			try {
				String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, MARKCAP,CLOSE, MOR, "
						+ " YOR, TRK, PASS, APAS,FLOOR(DPC), FLOOR(UPC), FLOOR(DM), FUC AS UTURN FROM BBROCK a, "
						+ " SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID "
						+ " and  a.DATEID=? AND a.FUC>0 ORDER BY MARKCAP DESC ";
				currentUTurnStocks = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return currentUTurnStocks;
	}

	public static PreparedStatement getCurrentUPC() {
		if (currentUPC == null) {
			try {
				String query = "select UPC, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID=? ";
				currentUPC = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return currentUPC;
	}

	public static PreparedStatement getCurrentDateID() {
		if (cDateId== null) {
			try {
				String query = "select MAX(DATEID) FROM BBROCK";
				cDateId = getConnection().prepareStatement(query);
				System.out.println(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return cDateId;
	}
	public static PreparedStatement getMinDPC() {
		if (minDPC == null) {
			try {
				String query = "select DPC, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? ORDER BY DPC ASC LIMIT 1";
				minDPC = getConnection().prepareStatement(query);
				System.out.println(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return minDPC;
	}

	public static PreparedStatement getFUD() {
		if (fud == null) {
			try {
				String query = "select UPC,DM,FUC, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";
				fud = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return fud;
	}

	public static PreparedStatement getTodayVBIFUCX() {
		if (vbifx == null) {
			try {
				String query = "select FUC,VBI,STOCKID,CLOSE, DATEID FROM BBROCK WHERE DATEID=? AND ( FUC>? OR VBI>? ) ORDER BY STOCKID ASC";
				vbifx  = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return vbifx ;
	}
	
	public static PreparedStatement getStockVBIFUCX() {
		if (stkvbifx == null) {
			try {
				String query = "select FUC,VBI,STOCKID,CLOSE, DATEID FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";
				stkvbifx  = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stkvbifx ;
	}
	
	
	public static PreparedStatement updateEE8() {
		if (updateEE8 == null) {
			try {
				String query = "UPDATE BBROCK SET EE8=? WHERE STOCKID=? AND DATEID=?";
				updateEE8  = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateEE8;
	}
	
	
	public static PreparedStatement getFUCF() {
		if (fucf == null) {
			try {
				String query = "select FUC,CLOSE, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";
				fucf = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return fucf;
	}

	public static PreparedStatement updateFUC() {
		if (updateFUC == null) {
			try {
				String query = "UPDATE BBROCK SET FUC = ?  WHERE  STOCKID = ? AND DATEID=?";
				updateFUC = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateFUC;
	}

	public static PreparedStatement updateBDYPDY() {
		if (updateBDYPDY == null) {
			try {
				String query = "UPDATE BBROCK SET BDY = ? , PDY=? WHERE  STOCKID = ? AND DATEID=?";
				updateBDYPDY = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateBDYPDY;
	}

	public static PreparedStatement getMinClose() {
		if (minClose == null) {
			try {
				String query = "select CLOSE, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? ORDER BY CLOSE ASC LIMIT 1";
				minClose = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return minClose;
	}

	public static PreparedStatement getDateIDbyUPC() {
		if (dateIdByUPC == null) {
			try {
				String query = "select DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? AND UPC>? AND UPC<? ORDER BY DATEID DESC";
				dateIdByUPC = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dateIdByUPC;
	}

	public static PreparedStatement getDateIDbyDPC() {
		if (dateIdByDPC == null) {
			try {
				String query = "select DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? AND DPC>? AND DPC<? ORDER BY DATEID DESC";
				dateIdByDPC = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dateIdByDPC;
	}

	public static PreparedStatement getDateIDbyPrice() {
		if (dateIdByPrice == null) {
			try {
				String query = "select DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=? AND CLOSE>? AND CLOSE<? ORDER BY DATEID DESC";
				dateIdByPrice = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dateIdByPrice;
	}

	public static PreparedStatement getPriceByDateID() {
		if (priceByDateID == null) {
			try {
				// String query = "select CLOSE FROM BBROCK WHERE STOCKID = ? AND DATEID=? ";
				String query = "select CLOSE, a.DATEID,a.STOCKID, CDATE, b.SYMBOL FROM BBROCK  a, SYMBOLS b,DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and a.STOCKID = ? AND a.DATEID=?";
				priceByDateID = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return priceByDateID;
	}

	public static int getStartDateId(int stockId) {
		int startDateId = 0;
		try {
			getStartDateIdStmnt();
			startDateIdStmnt.setInt(1, stockId);
			ResultSet rs = startDateIdStmnt.executeQuery();
			if (rs.next()) {
				startDateId = rs.getInt(1);
			}
		} catch (Exception ex) {

		}
		return startDateId;

	}

	// startDateIdStmnt
	public static PreparedStatement getStartDateIdStmnt() {
		if (startDateIdStmnt == null) {
			try {
				String query = "select  DATEID from BBROCK WHERE STOCKID=? ORDER BY DATEID ASC limit 1";

				startDateIdStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return startDateIdStmnt;
	}

	public static PreparedStatement getAvgD2() {
		if (avgD2 == null) {
			try {
				String query = "select AVG(DD) from BBROCK WHERE STOCKID=? and DATEID>=? and DATEID<=?";

				avgD2 = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return avgD2;
	}

	public static PreparedStatement getDailyPrice() {
		if (dailyPriceStmnt == null) {
			try {
				String query = "select CLOSE, DATEID, VOLUME,MARKCAP, DD from BBROCK WHERE STOCKID=? and DATEID>=? and DATEID<=? ORDER BY DATEID ASC";

				dailyPriceStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dailyPriceStmnt;
	}

	public static PreparedStatement updateAliasStockID() {
		if (updateStockID == null) {
			try {
				String query = "UPDATE BBROCK SET STOCKID=? WHERE STOCKID=?";

				updateStockID  = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateStockID ;
	}
	
	
	public static PreparedStatement updateRTS_MCP() {
		if (updateRtsMcp== null) {
			try {
				String query = "UPDATE BBROCK SET RTS=?, MCP=? WHERE STOCKID=? AND DATEID=?";

				updateRtsMcp  = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateRtsMcp ;
	}
	
	public static PreparedStatement updateD2() {
		if (updateD2 == null) {
			try {
				String query = "UPDATE BBROCK SET DD=?  WHERE STOCKID=? and DATEID=?";

				updateD2 = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateD2;
	}

	public static PreparedStatement updateD9() {
		if (updateD9 == null) {
			try {
				String query = "UPDATE BBROCK SET D9=?  WHERE STOCKID=? and DATEID=?";

				updateD9 = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateD9;
	}

	public static PreparedStatement getSumTeal() {
		if (tealSumStmnt == null) {
			try {
				String query = "select SUM(TEAL), SUM(PINK), SUM(YELLOW) from BBROCK WHERE STOCKID=? and DATEID>=? and DATEID<=?";

				tealSumStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return tealSumStmnt;
	}

	public static PreparedStatement getDBulls() {
		if (dBullStmnt == null) {
			try {
				// String query = "select a.DATEID,a.STOCKID, CDATE, PASS,APAS,b.SYMBOL, CLOSE,
				// MARKCAP FROM BBROCK a, SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and
				// a.DATEID=c.DATEID and ((PASS>=1 and APAS>=1) OR a.DATEID =?) AND a.STOCKID= ?
				// AND a.DATEID>=8454 ORDER BY a.DATEID ASC";
				String query = "select a.DATEID,a.STOCKID, CDATE, PASS,APAS,b.SYMBOL, CLOSE, MARKCAP FROM BBROCK a, SYMBOLS b,DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID AND a.STOCKID= ? AND a.DATEID>=8454 ORDER BY a.DATEID ASC";
				// String query = "select a.DATEID,a.STOCKID, CDATE, PASS,APAS,b.SYMBOL, CLOSE,
				// MARKCAP FROM BBROCK a, SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and
				// a.DATEID=c.DATEID AND a.STOCKID= ? AND a.DATEID>=1 ORDER BY a.DATEID ASC";

				dBullStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return dBullStmnt;
	}

	public static PreparedStatement getBosyUpdateStmnt() {
		if (BosyUpdate == null) {
			try {
				String query = "UPDATE  DATES  SET BOSY= ?,AY=?, BAT=?, BI=? WHERE DATEID = ?";
				BosyUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return BosyUpdate;
	}

	public static PreparedStatement getCBIUpdateStmnt() {
		if (CBIUpdate == null) {
			try {
				String query = "UPDATE  DATES  SET CBI=?, SALY=? WHERE DATEID = ?";
				CBIUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return CBIUpdate;
	}

	public static PreparedStatement getBuySellUpdate() {
		if (BuySellUpdate == null) {
			try {
				String query = "UPDATE  DATES  SET Sl1=?,SL2=?, BUY = ? WHERE DATEID = ?";
				BuySellUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return BuySellUpdate;
	}

	public static PreparedStatement getSubIndustryStmnt() {
		if (subIndStmnt == null) {
			try {
				String query = "select a.INDID , SUBID,INDUSTRY, SUBINDUSTRY FROM INDUSTRY a, SUBINDUSTRY b where a.INDID=b.INDID ORDER BY a.INDUSTRY ASC, b.SUBINDUSTRY ASC;";
				subIndStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return subIndStmnt;
	}

	public static PreparedStatement getAllSubUnderIndustryStmnt() {
		if (subUnderIndStmnt == null) {
			try {
				String query = "select INDID , SUBID,SUBINDUSTRY FROM  SUBINDUSTRY  where INDID = ?";
				subUnderIndStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return subUnderIndStmnt;
	}

	public static PreparedStatement getSubIndInsertStatement() {
		getConnection();

		if (insertSubIndStmnt == null) {
			try {
				String query = "insert into SUBINDUSTRY (INDID ,SUBID, SUBINDUSTRY) values (?, ?, ?)";

				insertSubIndStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return insertSubIndStmnt;
	}

	public static PreparedStatement getIndustryCodeStmnt() {
		if (IndCodeStmnt == null) {
			try {
				String query = "select a.INDID , SUBID,INDUSTRY, SUBINDUSTRY FROM INDUSTRY a, SUBINDUSTRY b where a.INDID=b.INDID AND a.INDUSTRY=? AND b.SUBINDUSTRY=?";
				IndCodeStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return IndCodeStmnt;
	}

	public static PreparedStatement updateStockIndustryCode() {
		if (updateStockIndustryCode == null) {
			try {
				String query = "UPDATE SYMBOLS SET INDID = ?, SUBID = ? WHERE SYMBOL = ?";
				updateStockIndustryCode = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateStockIndustryCode;
	}

	// select INDID , INDUSTRY FROM INDUSTRY;
	public static PreparedStatement getIndustryStmnt() {
		if (IndStmnt == null) {
			try {
				String query = "select INDID , INDUSTRY FROM INDUSTRY ORDER BY INDUSTRY ASC";
				IndStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return IndStmnt;
	}

	public static PreparedStatement getIndInsertStatement() {
		getConnection();

		if (insertIndStmnt == null) {
			try {
				String query = "insert into INDUSTRY (INDID , INDUSTRY) values (?, ?)";

				insertIndStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return insertIndStmnt;
	}

	public static PreparedStatement getOriginalData() {
		if (originalDataStmnt == null) {
			try {
				String query = "SELECT STOCKID,DATEID,PERCENT,CLOSE,NETCHANGE,ATR,OPEN,HIGH,LOW,LOW52,HIGH52,MARKCAP,VOLUME,YELLOW,TEAL,PINK,CX520 FROM BBROCK WHERE STOCKID=? ORDER BY DATEID ASC";
				originalDataStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return originalDataStmnt;
	}

	public static PreparedStatement insertOriginalData() {
		if (insertDataStmnt == null) {
			try {
				String query = "INSERT INTO BBROCK (STOCKID,DATEID,PERCENT,CLOSE,NETCHANGE,ATR,OPEN,HIGH,LOW,LOW52,HIGH52,MARKCAP,VOLUME,YELLOW,TEAL,PINK,CX520) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				insertDataStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return insertDataStmnt;
	}

	public static PreparedStatement getBOYSStmnt() {
		if (BOYSStmnt == null) {
			try {
				String query = "SELECT OS,OB,OY,BI,SALY,BOSY FROM  DATES  WHERE DATEID > ? AND DATEID<= ? ORDER BY DATEID DESC";
				BOYSStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return BOYSStmnt;
	}

	// select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, MARKCAP,CLOSE,DPC, UPC, DM, FUC
	// AS UTURN
	// FROM BBROCK a, SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and
	// a.DATEID=c.DATEID
	// and a.DATEID=9023 AND a.FUC>4 ORDER BY MARKCAP DESC;
	public static PreparedStatement getUTurnStmnt() {
		if (UTurnStmnt == null) {
			try {
				String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, MARKCAP,CLOSE,DPC, UPC, DM, FUC AS UTURN "
						+ " FROM BBROCK a, SYMBOLS b,DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID "
						+ " and  a.DATEID=? AND a.FUC>4 AND MARKCAP>1000 and a.DATEID<9010 ORDER BY MARKCAP DESC";
				UTurnStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return UTurnStmnt;
	}

	// SYPTStmnt
	public static PreparedStatement getSYPTStmnt() {
		if (SYPTStmnt == null) {
			try {
				String query = "SELECT SUM(TEAL),SUM(YELLOW),SUM(PINK), COUNT(*) FROM  BBROCK  WHERE DATEID = ?";
				SYPTStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return SYPTStmnt;
	}

	public static PreparedStatement getMarkcapStmnt() {
		if (markCapStmnt == null) {
			try {
				String query = "SELECT DATEID,STOCKID,CLOSE,MARKCAP FROM  BBROCK  WHERE STOCKID=? AND DATEID <= ? ORDER BY DATEID DESC LIMIT 3";
				markCapStmnt = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return markCapStmnt;
	}

	// select COUNT(*) FROM BBROCK WHERE DATEID=8900;
	// checkHistoryExists
	public static PreparedStatement checkHistoryExists() {
		if (checkHistoryExists == null) {
			try {
				String query = "select COUNT(*) FROM BBROCK WHERE DATEID=8900 and STOCKID = ?";
				checkHistoryExists = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return checkHistoryExists;
	}

	// SYPTUpdate
	public static PreparedStatement getSYPTUpdate() {
		if (SYPTUpdate == null) {
			try {
				String query = "UPDATE DATES SET OS = ?, OB=?, OY=?, TOT= ?  WHERE DATEID = ?";
				SYPTUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return SYPTUpdate;
	}

	public static PreparedStatement updatePDYToOne() {
		if (updatePDYToOne == null) {
			try {
				String query = "UPDATE BBROCK SET PDY= 1  WHERE  STOCKID=? AND DATEID = ?";
				updatePDYToOne = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updatePDYToOne;
	}

	public static PreparedStatement getUMACUpdate() {
		if (UMACUpdate == null) {
			try {
				String query = "UPDATE DATES SET UC = ?,AMC = ?  WHERE DATEID = ?";
				UMACUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return UMACUpdate;
	}

	public static PreparedStatement updateMaxUpDown() {
		if (updateMaxUpDown == null) {
			try {
				String query = "UPDATE BBROCK SET DM = ?, DA = ? WHERE STOCKID = ? AND DATEID = ?";
				updateMaxUpDown = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return updateMaxUpDown;
	}

	public static PreparedStatement getUpdateUpDownDistance() {
		if (distanceChangeUpdate == null) {
			try {
				String query = "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?  WHERE STOCKID = ? AND DATEID = ?";
				distanceChangeUpdate = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return distanceChangeUpdate;
	}

	public static PreparedStatement resetUpDownToZero() {
		if (resetUpDownToZero == null) {
			try {
				String query = "UPDATE BBROCK SET UPC = 0.0, UDS = 0, DPC = 0.0, DDS= 0, DM=0.0, DA=0  WHERE STOCKID = ? ";
				resetUpDownToZero = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return resetUpDownToZero;
	}

	public static Statement getStatement() {
		getConnection();
		try {
			if (stmnt == null) {
				stmnt = dbcon.createStatement();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return stmnt;
	}

	public static int getNextDateID() {
		int nextID = 1;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM DATES";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				nextID = rs.getInt(1) + 1;
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return nextID;
	}

	public static String getDateStr(int dateID) {
		String dateStr = "";
		getStatement();
		try {
			String query = "SELECT DATE_FORMAT(CDATE, '%Y-%m-%d') FROM DATES WHERE DATEID = " + dateID;

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				dateStr = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return dateStr;
	}

	public static int getDateID(String date) {
		int dateID = 0;
		getStatement();
		try {
			String query = " SELECT DATEID FROM DATES WHERE date(CDATE)='" + date + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				dateID = rs.getInt(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return dateID;
	}

	public static int getSymbolID(String symbol) {
		int stockID = 0;
		getStatement();
		try {
			String query = " SELECT STOCKID FROM SYMBOLS WHERE SYMBOL='" + symbol + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				stockID = rs.getInt(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return stockID;
	}

	public static int getNextSymbolID() {
		int nextID = 1;
		getStatement();
		try {
			String query = " SELECT MAX(STOCKID) FROM SYMBOLS WHERE STOCKID<10000";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				nextID = rs.getInt(1) + 1;
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return nextID;
	}

	// SELECT our_date FROM our_table WHERE idate >= '1997-05-05';
	public static int getParallelStock(int dateID, int stockID) {
		int stockId = 0;
		try {
			getStockIDsStmnt();
			stockIDStmnt.setInt(1, dateID);

			ResultSet rs = stockIDStmnt.executeQuery();

			int[] stocks = new int[7000];
			int count = 0;
			while (rs.next()) {
				stocks[count] = rs.getInt(1);
				count++;
			}

			double a1 = Math.random();

			int stockIdRandom = (int) (10000 * a1);

			while (count > 1 && (stockId < 1 || stockId == stockID)) {
				stockId = stocks[stockIdRandom % count];
				// System.out.println("Generate stockId "+stockId+" for "+stockID);
				a1 = Math.random();
				stockIdRandom = (int) (10000 * a1);
			}

			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return stockId;
	}

	public static PreparedStatement checkDateIDExistsStmnt() {
		getConnection();

		if (dateIDExistStmnt == null) {
			try {

				String query = "SELECT COUNT(*) FROM BBROCK  WHERE STOCKID = ? AND DATEID =? ";

				dateIDExistStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateIDExistStmnt;
	}

	public static PreparedStatement getStockIDsStmnt() {
		getConnection();

		if (stockIDStmnt == null) {
			try {

				String query = "SELECT STOCKID FROM BBROCK  WHERE  DATEID =? ";

				stockIDStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return stockIDStmnt;
	}

	// SELECT our_date FROM our_table WHERE idate >= '1997-05-05';
	public static boolean checkDateExist(String date) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM DATES WHERE date(CDATE)='" + date + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	public static boolean checkSymbolExist(String symbol) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM SYMBOLS WHERE SYMBOL='" + symbol + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	public static boolean checkBBRecordExist(int stockID, int dateID) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM BBROCK WHERE STOCKID = " + stockID + " and DATEID = " + dateID;

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	public static PreparedStatement get520CXRangeUpdate() {
		getConnection();

		if (update520CXRange == null) {
			try {

				String query = "UPDATE BBROCK SET CX520 = ? WHERE STOCKID = ? AND DATEID >=? ";

				update520CXRange = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return update520CXRange;
	}

	// update520CXStmnt
	public static PreparedStatement get520CXUpdateStmnt() {
		getConnection();

		if (update520CXStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET CX520 = ? WHERE STOCKID = ? AND DATEID =? ";

				update520CXStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return update520CXStmnt;
	}

	public static PreparedStatement getQualifiedLowStmnt() {
		getConnection();

		if (qualifiedLowStmnt == null) {
			try {

				String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, CLOSE, UPC, UDS, DPC, DDS, DM FROM BBROCK a, SYMBOLS b,DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and a.STOCKID=?  and  a.DATEID = ? ";

				qualifiedLowStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return qualifiedLowStmnt;
	}

	// select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, CLOSE, MOR, YOR,
	// TRK,(MOR+YOR+TRK) as TTR, PASS, APAS, BT9,TSM,YSM,PSM,DSM FROM BBROCK a,
	// SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and
	// a.STOCKID=509 order by a.DATEID DeSC limit 550;
	public static PreparedStatement checkRankPriceStmnt() {
		getConnection();

		if (closePriceStmnt == null) {
			try {

				String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, CLOSE, MOR, YOR, TRK,(MOR+YOR+TRK) as TTR, PASS, APAS, BT9,TSM,YSM,PSM,DSM FROM BBROCK a, SYMBOLS b,DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and a.STOCKID=?  and  a.DATEID >= ? and a.DATEID <= ? ";

				closePriceStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return closePriceStmnt;
	}

	// getClosePrice
	public static PreparedStatement getClosePriceStmnt() {
		getConnection();

		if (closePriceStmnt == null) {
			try {

				String query = "SELECT CLOSE,PDY,BDY FROM BBROCK  WHERE STOCKID = ? AND DATEID =? ";

				closePriceStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return closePriceStmnt;
	}

	
	public static PreparedStatement getTBKStmnt() {
		getConnection();

		if (tbkStmnt == null) {
			try {

				String query = "SELECT TBK, DATEID, CLOSE FROM BBROCK  WHERE TBK>=? AND STOCKID = ? AND DATEID >=? AND DATEID<=? ORDER BY DATEID DESC";

				tbkStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return tbkStmnt;
	}
	
	public static PreparedStatement getUPCStmnt() {
		getConnection();

		if (upcStmnt == null) {
			try {

				String query = "SELECT UPC,DPC, CLOSE, DM, DA, ADM, RK, FUC FROM BBROCK  WHERE STOCKID = ? AND DATEID =? ";

				upcStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return upcStmnt;
	}

	public static PreparedStatement getFUCHistoryStmnt() {
		getConnection();

		if (fucStmnt == null) {
			try {

				String query = "select a.DATEID,b.CDATE,  COUNT(*) FROM BBROCK a, DATES b WHERE a.DATEID=b.DATEID and FUC>? GROUP BY DATEID ORDER BY DATEID DESC limit ?;";

				fucStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return fucStmnt;
	}

	
	
	public static PreparedStatement getFUCTodayStmnt() {
		getConnection();

		if (fucTodayStmnt == null) {
			try {

				String query = "select a.DATEID,b.CDATE,  COUNT(*) FROM BBROCK a, DATES b WHERE a.DATEID=b.DATEID and FUC>? AND a.DATEID=? GROUP BY DATEID";

				fucTodayStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return fucTodayStmnt;
	}

	public static PreparedStatement f1UpdateStmnt() {
		getConnection();

		if (f1UpdateStmnt == null) {
			try {

				String query = "UPDATE DATES SET F1 = ?  WHERE  DATEID =? ";

				f1UpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return f1UpdateStmnt;
	}

	public static PreparedStatement f8UpdateStmnt() {
		getConnection();

		if (f8UpdateStmnt == null) {
			try {

				String query = "UPDATE DATES SET F8 = ?  WHERE  DATEID =? ";

				f8UpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return f8UpdateStmnt;
	}

	public static PreparedStatement getSubIndStockCount() {
		if (subIndStockCount== null) {
			try {
				String query = "SELECT count(*) FROM  BBROCK a, SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and a.DATEID=? and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?";
				subIndStockCount = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return subIndStockCount;
	}
	
	public static PreparedStatement getAllSubIndStockInfo() {
		if (subIndStockInfo == null) {
			try {
				String query = "SELECT a.DATEID, CDATE, a.STOCKID,b.SYMBOL, close, BDY,PDY FROM  BBROCK a, SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and a.DATEID=?  and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=? ORDER BY MARKCAP ASC";
				subIndStockInfo = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return subIndStockInfo;
	}

	public static PreparedStatement getStockInfoHistory() {
		if (stockInfoHistory == null) {
			try {
				String query = "SELECT a.DATEID, CDATE, a.STOCKID,b.SYMBOL, close, BDY,PDY,SAY,IAYD FROM  BBROCK a, SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and a.DATEID>=? and a.DATEID<=? and a.STOCKID=b.STOCKID and a.STOCKID=?  ORDER BY a.DATEID DESC";
				stockInfoHistory = getConnection().prepareStatement(query);
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}

		}

		return stockInfoHistory;
	}

	// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
	// b,DATES c, INDUSTRY d, SUBINDUSTRY e WHERE a.STOCKID = b.STOCKID and
	// a.DATEID=c.DATEID and b.INDID=d.INDID and b.INDID=e.INDID and b.SUBID=e.SUBID
	// and a.DATEID =9026
	// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC;
	public static PreparedStatement getIndIDStmnt() {
		getConnection();

		if (indIdStmnt == null) {
			try {

				String query = "select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS b,DATES c, INDUSTRY d, SUBINDUSTRY e  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?  GROUP BY b.INDID,b.SUBID ORDER BY  b.INDID ASC,b.SUBID ASC";

				indIdStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return indIdStmnt;
	}

	public static PreparedStatement getIndAvgYieldUpdateStmnt() {
		getConnection();

		if (indAvgYieldUpdate == null) {
			try {

				String query = "UPDATE BBROCK SET IAY = ?, IPY=? WHERE STOCKID = ? AND DATEID =? ";

				indAvgYieldUpdate = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return indAvgYieldUpdate;
	}

	public static PreparedStatement getSubIndAvgYieldUpdateStmnt() {
		getConnection();

		if (subIndAvgYieldUpdate == null) {
			try {

				String query = "UPDATE BBROCK SET SAY = ?, SPY=? WHERE STOCKID = ? AND DATEID =? ";

				subIndAvgYieldUpdate = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return subIndAvgYieldUpdate;
	}

	public static PreparedStatement updateIndAvgYieldDelta() {
		getConnection();

		if (updateIndAvgYieldDelta == null) {
			try {

				String query = "UPDATE BBROCK SET IAYD=? WHERE STOCKID = ? AND DATEID =? ";

				updateIndAvgYieldDelta = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateIndAvgYieldDelta;
	}
	
	public static PreparedStatement updateStockSectorStmnt() {
		getConnection();

		if (updateStockSectorStmnt == null) {
			try {

				String query = "UPDATE SYMBOLS SET INDID = ?, SUBID =? WHERE SYMBOL= ? ";
				updateStockSectorStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateStockSectorStmnt;
	}

	public static PreparedStatement getCX520Stmnt() {
		getConnection();

		if (cx520Stmnt == null) {
			try {

				String query = "SELECT CX520 FROM BBROCK  WHERE STOCKID = ? AND DATEID =? ";

				cx520Stmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return cx520Stmnt;
	}

	public static PreparedStatement getLastCloseStmnt() {
		getConnection();

		if (lastCloseStmnt == null) {
			try {

				String query = "SELECT CLOSE FROM BBROCK  WHERE STOCKID = ? ORDER BY DATEID DESC LIMIT 1 ";

				lastCloseStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return lastCloseStmnt;
	}

	// scUpdateStmnt
	public static PreparedStatement getSCUpdateStmnt() {
		getConnection();

		if (scUpdateStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET SC5 = ? WHERE STOCKID = ? AND DATEID =? ";

				scUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return scUpdateStmnt;
	}

	// queryStockIDStmnt
	public static PreparedStatement getDateIDStarttmnt() {
		getConnection();

		if (dateIDStartStmnt == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK WHERE  STOCKID =? ORDER BY DATEID ASC limit 1";
				dateIDStartStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateIDStartStmnt;
	}

	// queryStockIDStmnt
	public static PreparedStatement getStockIDQueryStmnt() {
		getConnection();

		if (queryStockIDStmnt == null) {
			try {

				String query = "SELECT STOCKID FROM BBROCK WHERE  DATEID =? ORDER BY STOCKID ASC";
				queryStockIDStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryStockIDStmnt;
	}

	public static PreparedStatement getUpdateColorSumStmnt() {
		getConnection();

		if (colorSumUpdateStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET TSM = ?, NSM = ?, YSM = ?, PSM = ?, DSM = ?  WHERE STOCKID =  ? AND DATEID =?";
				colorSumUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorSumUpdateStmnt;
	}

	public static PreparedStatement getNoColorSumStmnt() {
		getConnection();

		if (noColorSumStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "SELECT COUNT(*) FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=? AND TEAL=0 AND YELLOW=0 AND PINK=0";
				noColorSumStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return noColorSumStmnt;
	}

	public static PreparedStatement getColorCalSumStmnt() {
		getConnection();

		if (colorCalSumStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "SELECT TSM, YSM, PSM, DSM, TOM, POM,YOM FROM BBROCK  WHERE STOCKID =  ? AND DATEID =?";
				colorCalSumStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorCalSumStmnt;
	}

	public static PreparedStatement updateColorRankingStmnt() {
		getConnection();

		if (colorRankingUpdateStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET MOR = ?, YOR = ?, TRK = ?  WHERE STOCKID =  ? AND DATEID =?";
				colorRankingUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorRankingUpdateStmnt;
	}

	public static PreparedStatement getColorSumStmnt() {
		getConnection();

		if (colorSumStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "SELECT SUM(TEAL), SUM(YELLOW), SUM(PINK), COUNT(*) FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=?";
				colorSumStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorSumStmnt;
	}

	public static PreparedStatement getColorRankSumStmnt() {
		getConnection();

		if (colorRankSumStmnt == null) {
			try {

				String query = "SELECT SUM(MOR), SUM(YOR), SUM(TRK), COUNT(*) FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=?";
				colorRankSumStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorRankSumStmnt;
	}

//	select a.DATEID, CDATE, COUNT(*)  FROM BBROCK a, DATES c  WHERE a.DATEID=c.DATEID and PASS>3 AND CCX>0 AND CCX<=14 
//			group by a.DATEID, CDATE order by a.DATEID DeSC limit 320;

	public static PreparedStatement getOBIHistoryStmnt() {
		getConnection();

		if (OBIHistoryStmnt == null) {
			try {

				String query = "select a.DATEID, CDATE, COUNT(*)  FROM BBROCK a, DATES c  WHERE a.DATEID=c.DATEID and PASS>3 AND CCX>0 AND CCX<=14 group by a.DATEID, CDATE order by a.DATEID DeSC limit ?; ";
				OBIHistoryStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return OBIHistoryStmnt;
	}

	public static PreparedStatement getTodayOBIStmnt() {
		getConnection();

		if (todayOBIStmnt == null) {
			try {

				String query = "select a.DATEID, CDATE, COUNT(*)  FROM BBROCK a, DATES c  WHERE a.DATEID=c.DATEID and PASS>3 AND CCX>0 AND CCX<=14 and a.DATEID=? group by a.DATEID, CDATE ";
				todayOBIStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return todayOBIStmnt;
	}

	public static PreparedStatement getUpdateOBIStmnt() {
		getConnection();

		if (UpdateOBIStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE DATES SET OBI = ? WHERE DATEID =?";
				UpdateOBIStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return UpdateOBIStmnt;
	}

	public static PreparedStatement getAvgDMStmnt() {
		getConnection();

		if (avgDM == null) {
			try {

				String query = "SELECT AVG(DM) FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=?";
				avgDM = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return avgDM;
	}

	public static PreparedStatement getDMRankStmnt() {
		getConnection();

		if (dmRank == null) {
			try {

				String query = "SELECT DATEID, DM FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=? ORDER BY DM DESC";
				dmRank = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dmRank;
	}

	public static PreparedStatement deleteStockRecord() {
		getConnection();

		if (deleteStockRecord == null) {
			try {

				String query = "DELETE FROM BBROCK  WHERE STOCKID =  ? ";
				deleteStockRecord = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return deleteStockRecord;
	}

	public static PreparedStatement getStockDateIDAsc() {
		getConnection();

		if (StockDateIDAsc == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK  WHERE STOCKID =  ? ORDER BY DATEID ASC";
				StockDateIDAsc = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return StockDateIDAsc;
	}

	public static PreparedStatement getDateIDStmnt() {
		getConnection();

		if (dateIDStmnt == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK  WHERE STOCKID =  ? AND DATEID>=? AND DATEID <=? ORDER BY DATEID DESC";
				dateIDStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateIDStmnt;
	}

	public static PreparedStatement getOMColorUpdateStmnt() {
		getConnection();

		if (colorOMUpdateStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET TOM = ?, NOM = ?, YOM = ?, POM = ?  WHERE STOCKID =  ? AND DATEID =?";
				colorOMUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return colorOMUpdateStmnt;
	}

	public static PreparedStatement getUpdateDMRankAVGStmnt() {
		getConnection();

		if (updateDMRankAvg == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET ADM = ?, RK = ? WHERE STOCKID =  ? AND DATEID =?";
				updateDMRankAvg = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateDMRankAvg;
	}

	// updateBPYStmnt = null;
	public static PreparedStatement getUpdateBPYStmnt() {
		getConnection();

		if (updateBPYStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET BPY = ? WHERE STOCKID =  ? AND DATEID =?";
				updateBPYStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateBPYStmnt;
	}

	// updateT9Stmnt = null;
	public static PreparedStatement getUpdateT9Stmnt() {
		getConnection();

		if (updateT9Stmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET BT9 = ? WHERE STOCKID =  ? AND DATEID =?";
				updateT9Stmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateT9Stmnt;
	}

	// queryPYStmnt
	public static PreparedStatement getPYQueryStmnt() {
		getConnection();

		if (queryPYStmnt == null) {
			try {

				String query = "select PINK,YELLOW, DATEID, BPY FROM BBROCK WHERE STOCKID = ? AND DATEID>=?  ORDER BY DATEID ASC";
				queryPYStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryPYStmnt;
	}
	
	
	public static PreparedStatement getClosetMCPStmnt() {
		getConnection();

		if (closestMCPStmnt == null) {
			try {

				String query = "select DATEID,MCP,RTS FROM BBROCK WHERE MCP>0.001 AND RTS>=? AND STOCKID = ? AND DATEID<? AND DATEID>? ORDER BY DATEID DESC LIMIT 50";
				closestMCPStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return closestMCPStmnt;
	}

	// queryTealStmnt
	public static PreparedStatement getTealQueryStmnt() {
		getConnection();

		if (queryTealStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select TEAL, DATEID, BT9 FROM BBROCK WHERE STOCKID = ? AND DATEID>=?  ORDER BY DATEID ASC";
				queryTealStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryTealStmnt;
	}

	// select a.DATEID, CDATE, b.SYMBOL, CLOSE,BI,CBI,SALY,BOSY,BAT,TOT,OS, OB, OY,
	// AY
	// FROM BBROCK a, SYMBOLS b, DATES c WHERE a.STOCKID = b.STOCKID and
	// a.DATEID=c.DATEID and ( b.SYMBOL='SPY') order by a.DATEID DeSC limit 550;

	public static PreparedStatement getBuySellStmnt() {
		getConnection();

		if (BuySellStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select a.DATEID, CDATE, b.SYMBOL, CLOSE,BI,CBI,SALY,BOSY,BAT,TOT,OS, OB, OY, AY, SL1, SL2, BUY, c.CDATE "
						+ "FROM BBROCK a, SYMBOLS b, DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and ( b.SYMBOL=?)  AND a.DATEID>?   order by a.DATEID ASC";

				BuySellStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return BuySellStmnt;
	}

	//
	public static PreparedStatement getPast30Stmnt() {
		getConnection();

		if (past30Stmnt == null) {
			try {
				String query = "SELECT MAX(CLOSE),SUM(YELLOW),SUM(PINK), AVG(VOLUME) FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=?";

				past30Stmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return past30Stmnt;
	}
	
	
	
	public static PreparedStatement updateTBKStmnt() {
		getConnection();

		if (updateTBKStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET TBK=?   WHERE STOCKID =? and DATEID=?";

				updateTBKStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateTBKStmnt;
	}
	
	public static PreparedStatement getPureTeal() {
		getConnection();

		if (pureTeal == null) {
			try {
				String query = "select STOCKID FROM BBROCK   WHERE DATEID=? AND STOCKID>=? AND STOCKID<=? AND TEAL=1 AND YELLOW=0 AND PINK=0 ORDER BY STOCKID ASC";

				pureTeal = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return pureTeal;
	}

	
	public static PreparedStatement getDDD9Stmnt() {
		getConnection();

		if (DDD9Stmnt == null) {
			try {
				String query = "select DD, D9 FROM BBROCK   WHERE STOCKID =? and DATEID<=? AND DATEID>=? order by DATEID DESC";

				DDD9Stmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return DDD9Stmnt;
	}

	
	public static PreparedStatement updateVBIStmnt() {
		getConnection();

		if (updateVBIStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET VBI=?   WHERE STOCKID =? and DATEID=?";

				updateVBIStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateVBIStmnt;
	}
	
	
	public static PreparedStatement resetTBKStmnt() {
		getConnection();

		if (resetTBKStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET TBK=0   WHERE STOCKID =?";

				resetTBKStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return resetTBKStmnt;
	}
	
	
	public static PreparedStatement updateBDAStmnt() {
		getConnection();

		if (updateBDAStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET BDA=?   WHERE STOCKID =? and DATEID=?";

				updateBDAStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateBDAStmnt;
	}
	
	
	public static PreparedStatement getTYPDSumByStockIDStmnt() {
		getConnection();

		if (typSumbyStockIDStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?";

				typSumbyStockIDStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return typSumbyStockIDStmnt;
	}

	// yp10SumUpdateStmnt
	public static PreparedStatement getYP10SumUpdateStmnt() {
		getConnection();

		if (yp10SumUpdateStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET YP10 = ?  WHERE STOCKID = ? AND DATEID = ? ";

				yp10SumUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return yp10SumUpdateStmnt;
	}

	// yp10SumCalStmnt
	public static PreparedStatement getYP10SumCalStmnt() {
		getConnection();

		if (yp10SumCalStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select SUM(YELLOW), SUM(PINK) FROM BBROCK  WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?";

				yp10SumCalStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return yp10SumCalStmnt;
	}

	// typSumQueryStmnt
	public static PreparedStatement getTYPDSumQueryStmnt() {
		getConnection();

		if (typSumQueryStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?";

				typSumQueryStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return typSumQueryStmnt;
	}

	// symbolDateIDQuery
	public static PreparedStatement getSymbolDateIDQueryStmnt() {
		getConnection();

		if (symbolDateIDQuery == null) {
			try {
				String query = "SELECT DATEID FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and b.SYMBOL = ? ORDER BY a.DATEID DESC";

				symbolDateIDQuery = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return symbolDateIDQuery;
	}

	public static PreparedStatement getBackTestTealUpdateStmnt() {
		getConnection();

		if (backTestTealUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET TEAL = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestTealUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestTealUpdateStmnt;
	}

	public static PreparedStatement getBackTestYellowUpdateStmnt() {
		getConnection();

		if (backTestYellowUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET YELLOW = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestYellowUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestYellowUpdateStmnt;
	}

	public static PreparedStatement getBackTestPinkUpdateStmnt() {
		getConnection();

		if (backTestPinkUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET PINK = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestPinkUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestPinkUpdateStmnt;
	}

	public static PreparedStatement getBackTestInsertStatement() {
		getConnection();

		if (backTestStmnt == null) {
			try {
				String query = "insert into BBROCK (STOCKID,DATEID, CLOSE) values (?, ?,?)";

				backTestStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestStmnt;
	}

	public static PreparedStatement getDateInsertStatement() {
		getConnection();

		if (dateStmnt == null) {
			try {
				String query = " insert into DATES (DATEID, CDATE) values (?, ?)";

				dateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateStmnt;
	}

	public static PreparedStatement getSymbolInsertStatement() {
		getConnection();

		if (symbolStmnt == null) {
			try {
				String query = " insert into SYMBOLS (STOCKID, SYMBOL) values (?, ?)";

				symbolStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return symbolStmnt;
	}

	public static PreparedStatement deleteSymbol() {
		getConnection();

		if (deleteSymb == null) {
			try {
				String query = " DELETE FROM SYMBOLS WHERE STOCKID = ?";

				deleteSymb = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return deleteSymb;
	}
	
	public static PreparedStatement getRockInsertStatement() {
		getConnection();

		if (rockStmnt == null) {
			try {
				String query = " insert into BBROCK(STOCKID,DATEID,PERCENT,CLOSE,NETCHANGE,ATR,OPEN,HIGH,LOW,LOW52,HIGH52,MARKCAP,VOLUME,YELLOW,TEAL,PINK)"
						+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

				rockStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return rockStmnt;
	}

	public static void main(String[] args) {

		// https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html#package.description
		// auto java.sql.Driver discovery -- no longer need to load a java.sql.Driver
		// class via Class.forName

		// register JDBC driver, optional since java 1.6
		/*
		 * try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException
		 * e) { e.printStackTrace(); }
		 */

		// $$$$$$$$ SQL
		/*
		 * CREATE TABLE BBROCK(STOCKID SMALLINT, DATEID SMALLINT, PERCENT FLOAT DEFAULT
		 * 0.0, CLOSE FLOAT DEFAULT 0.0,NETCHANGE FLOAT DEFAULT 0.0, ATR FLOAT DEFAULT
		 * 0.0, OPEN FLOAT DEFAULT 0.0,HIGH FLOAT DEFAULT 0.0, LOW FLOAT DEFAULT
		 * 0.0,LOW52 FLOAT DEFAULT 0.0,HIGH52 FLOAT DEFAULT 0.0, MARKCAP FLOAT DEFAULT
		 * 0.0,VOLUME INT DEFAULT 0, YELLOW TINYINT DEFAULT 0,TEAL TINYINT DEFAULT 0,
		 * PINK TINYINT DEFAULT 0, SC5 SMALLINT DEFAULT 0, SC10C SMALLINT DEFAULT 0,
		 * SC15 SMALLINT DEFAULT 0, TSC INT DEFAULT 0,BT9 SMALLINT DEFAULT 0, PTVAL
		 * FLOAT DEFAULT 0.0, PTCP FLOAT DEFAULT 0.0, PASS TINYINT DEFAULT 0, PRIMARY
		 * KEY (STOCKID, DATEID), FOREIGN KEY (STOCKID) REFERENCES SYMBOLS(STOCKID) ON
		 * DELETE CASCADE, FOREIGN KEY (DATEID) REFERENCES DATES(DATEID)); //select
		 * a.DATEID,CDATE, a.STOCKID, CLOSE, ATR,TEAL, YELLOW, PINK,
		 * BTC,BYC,BPC,BTS,BYS,BPS,BBS,PTVAL, PTCP,BT9, MERGE, MARKCAP, VOLUME FROM
		 * BBROCK a, SYMBOLS b, DATES c WHERE a.STOCKID = b.STOCKID and
		 * a.DATEID=c.DATEID and b.SYMBOL='LYG';
		 */
		// $$$$$$$$$ SQL

		// auto close connection
		try {
			Connection conn = DriverManager.getConnection(
					"jdbc:mysql://127.0.0.1:3306/SDS?allowPublicKeyRetrieval=true&useSSL=false", "root",
					"Goldfish@3224");

			if (conn != null) {
				System.out.println("Connected to the database!");

				/*
				 * create table users ( id int unsigned auto_increment not null, first_name
				 * varchar(32) not null, last_name varchar(32) not null, date_created timestamp
				 * default now(), is_admin boolean, num_points int, primary key (id) );
				 */

				// create a sql date object so we can use it in our INSERT statement
				Calendar calendar = Calendar.getInstance();
				java.sql.Date startDate = new java.sql.Date(calendar.getTime().getTime());

				/*
				 * // the mysql insert statement String query =
				 * " insert into users (first_name, last_name, date_created, is_admin, num_points)"
				 * + " values (?, ?, ?, ?, ?)";
				 * 
				 * // create the mysql insert preparedstatement PreparedStatement preparedStmt =
				 * conn.prepareStatement(query); preparedStmt.setString (1, "Barney");
				 * preparedStmt.setString (2, "Rubble"); preparedStmt.setDate (3, startDate);
				 * preparedStmt.setBoolean(4, false); preparedStmt.setInt (5, 5000);
				 * 
				 * // execute the preparedstatement preparedStmt.execute();
				 * 
				 * conn.close();
				 */
				String query = " insert into HISTORY (STOCKID, DATEID, OPEN, YELLOW)" + " values (?, ?, ?, ?)";

				// create the mysql insert preparedstatement
				PreparedStatement preparedStmt = conn.prepareStatement(query);
				preparedStmt.setInt(1, 3);
				preparedStmt.setInt(2, 1);
				preparedStmt.setFloat(3, 9.80f);
				preparedStmt.setInt(4, 3);

				// execute the preparedstatement
				// preparedStmt.execute();

				conn.close();
			} else {
				System.out.println("Failed to make connection!");
			}

		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
