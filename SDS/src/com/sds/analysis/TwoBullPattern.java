package com.sds.analysis;

import com.sds.db.DB;
import com.sds.db.OneBullDB;

import java.sql.*;
import com.sds.db.TwoBullDB;

/*
 *  So Bull Pattern deals is another less bull case of Bull Pattern One.
 *  Strong Bull Pattern One, the BT9 and 10 pass points usually overlapped and one after 
 *  another without much break period. Most stocks could not behave this strongly.
 *  What usually happens is that after T9, with or without 10 passing points  or sometimes
 *  even with a couple around 10 passing points, the stock enters into correction without
 *  another leg up T9. The correct usually last above 25 days and above 15% drop, during 
 *  correction period, AVG5 < AVG 20 (>=25 days), then comes with AVG5>AVG20 time.
 *  If it last longer than >12 days, then an relative safe up trend is set up. 
 *  The above 12 days AVG5>AVG20 may take one short above or several such before appears
 *  Since most stocks fall into this category, this may be a more important case than 
 *  bull pattern one. Bull pattern one usually find exceptional bull stocks, especially
 *  BT9 points are back to back closely.
 *  Pattern Two is usually good for pick up relative good stock on cheap and wait for up leg.
 *  Example: BIG, GNUS, TNDM,
 *  IF there is BT9 appears, then we could buy right after BT9 without +5% increase
 *  especially the SUM (TEAL+PINK+Yellow) for the BT9 period is >=5, even part of the BT9
 *  days is AVG5 <AVG20 period. That way you get much cheap entry buy point. (see FBIO)
 */
/*
 * ONE TRICK IS TO MERGE SMALL FRAGMENTS (LESS THAN 13 DAYS) OF AVG5<AVG20 OR AVG5>AVG20 
 * INTO BIG CHUNK OF TREND, SMALL FRAGMENT AVG5>AVG20 PEAK (HIGHEST CLOSE) COULD BE USED
 * AS REFERENCE TO SEE IF THE FRAGMENT SHOULD BE ALIGNED BACKWARDS OR FORWARDS, UP OR DOWN
 */

//QUERY TO UNDERSTAND LOGIC
//select a.DATEID,CDATE, a.STOCKID, CLOSE, ATR,TEAL, YELLOW, PINK,YP10, SC5,BT9,DAYS,PTVAL, PTCP, PASS, CX520,CCX, BDCX, b.SYMBOL FROM BBROCK a, SYMBOLS b, DATES c  WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.SYMBOL='TENX' order by a.DATEID DESC limit 300;
public class TwoBullPattern {

	private static PreparedStatement queryBDCXStmnt = null;
	private static PreparedStatement queryBeginPrice = null;
	private static PreparedStatement queryEndPrice = null;
	private static PreparedStatement queryHighLowPrice = null;
	private static PreparedStatement pdateBDCXZero = null;
	private static PreparedStatement updateBDW = null;
	private static PreparedStatement updateBDWZero = null;
	private static PreparedStatement bdwQueryStmnt = null;
	private static PreparedStatement minCloseQuery = null;
	private static PreparedStatement maxCloseQuery = null;
	private static PreparedStatement updatePTCP2 = null;
	
	public static void init() {
		queryBDCXStmnt = TwoBullDB.getBDCXQuery();
		queryBeginPrice = TwoBullDB.getQueryBeginPrice();
		queryEndPrice = TwoBullDB.getQueryEndPrice();
		queryHighLowPrice = TwoBullDB.getHighLowPrice();
		pdateBDCXZero = TwoBullDB.getBDCXUpdateZero();
		updateBDW = TwoBullDB.getBDWUpdateStmnt();
		updateBDWZero = TwoBullDB.getBDWUpdateZero();
		bdwQueryStmnt = TwoBullDB.getBDWQuery();
		minCloseQuery = OneBullDB.getMinCloseFindStmnt();
		maxCloseQuery = OneBullDB.getMaxCloseFindStmnt();
		updatePTCP2 = TwoBullDB.getPTCP2UpdateStmnt();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String symbol ="ZSAN";
		mergeBDCXHistory(symbol, -1,false);
		updatePTCP2History(symbol, -1,false);

	}

	// update peak trough change percentage based on merged boundary BDW
	public static void updatePTCP2History(String symbol, int stockID, boolean lastDayOnly) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			bdwQueryStmnt.setInt(1, stockID);
			ResultSet rs1 = bdwQueryStmnt.executeQuery();
			boolean start = false;
			int lc = 0;
			int endDate1 = 0;
			int endDate2 = 0;
			int startDate1 = 0;
			int startDate2 = 0;
			int bdw1 = 0;
			int bdw2 = 0;
			float minClose = 0.0f;
			float maxClose = 0.0f;
			int minDate = 0;
			int maxDate = 0;
			float ptcp2 = 0.0f;
			int days2 = 0;
			boolean lastProcessed = false;

			while (rs1.next()) {
				int bdw = rs1.getInt(1);
				int dateId = rs1.getInt(2);
				if (start) {
					if (endDate1 == 0) {
						endDate1 = dateId;
						bdw1 = bdw;
					} else if (startDate1 == 0) {
						startDate1 = dateId;
					} else if (endDate2 == 0) {
						endDate2 = dateId;
						bdw2 = bdw;
					} else if (startDate2 == 0) {
						startDate2 = dateId;
					} 
					
					if(endDate1>0 && endDate2>0 && startDate1>0 && startDate2>0) {
						lastProcessed = true;
						//we have full house, we could start query max, min close price
						if(bdw1<0 && bdw2>0) {
							//get the min for the first period
							minCloseQuery.setInt(1, stockID);
							minCloseQuery.setInt(2, startDate1);
							minCloseQuery.setInt(3, endDate1);
							ResultSet rs2 = minCloseQuery.executeQuery();
							
							if(rs2.next()) {
								minClose = rs2.getFloat(1);
								minDate = rs2.getInt(2);
							}
							
							
							//get the max for the second period
							maxCloseQuery.setInt(1, stockID);
							maxCloseQuery.setInt(2, startDate2);
							maxCloseQuery.setInt(3, endDate2);
							ResultSet rs3 = maxCloseQuery.executeQuery();
							
							if(rs3.next()) {
								maxClose = rs3.getFloat(1);
								maxDate = rs3.getInt(2);
							}
							
							ptcp2 = 100.0f*(minClose - maxClose)/maxClose;
							days2 = minDate - maxDate;
							
						}else if(bdw1>0 && bdw2<0) {
							//get the max for the first period
							maxCloseQuery.setInt(1, stockID);
							maxCloseQuery.setInt(2, startDate1);
							maxCloseQuery.setInt(3, endDate1);
							ResultSet rs4 = maxCloseQuery.executeQuery();
							
							if(rs4.next()) {
								maxClose = rs4.getFloat(1);
								maxDate = rs4.getInt(2);
							}
							
							
							//get the min for the second period
							minCloseQuery.setInt(1, stockID);
							minCloseQuery.setInt(2, startDate2);
							minCloseQuery.setInt(3, endDate2);
							ResultSet rs5 = minCloseQuery.executeQuery();
							
							if(rs5.next()) {
								minClose = rs5.getFloat(1);
								minDate = rs5.getInt(2);
							}
							
							ptcp2 = 100.0f*(maxClose - minClose)/minClose;
							days2 = maxDate - minDate;
							
						}
						
						//update 
						updatePTCP2.setFloat(1, ptcp2);
						updatePTCP2.setInt(2,days2);
						updatePTCP2.setInt(3, stockID);
						updatePTCP2.setInt(4, endDate1);
						updatePTCP2.executeUpdate();
						
						//reassign
						endDate1 = endDate2;
						startDate1 = startDate2;
						bdw1 = bdw2;
						endDate2 = 0;
						startDate2 = 0;
						bdw2 = 0;
						days2 = 0;
						ptcp2 = 0.0f;
					}

				} else if (!start) {
					// if -1 or +1, then we could start next, otherwise wait one more next
					if ((bdw == -1 || bdw == 1) || lc == 2) {
						start = true;
					}
				}
				lc++;

				//optimized for daily process, we only need to process last attempt
				if(lastDayOnly&&lastProcessed) {
					break;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);

		}

	}

	public static void mergeBDCXHistory(String symbol, int stockID, boolean lastDayOnly) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}

		int bdcx1 = 0;
		float closeHigh1 = 0.0f;
		float closeLow1 = 0.0f;
		float endPrice1 = 0.0f;
		float beginPrice1 = 0.0f;
		int endDate1 = 0;
		int beginDate1 = 0;

		int bdcx2 = 0;
		float endPrice2 = 0.0f;
		float beginPrice2 = 0.0f;
		float closeHigh2 = 0.0f;
		float closeLow2 = 0.0f;
		int endDate2 = 0;
		int beginDate2 = 0;

		int bdcx3 = 0;
		float endPrice3 = 0.0f;
		float beginPrice3 = 0.0f;
		float closeHigh3 = 0.0f;
		float closeLow3 = 0.0f;
		int endDate3 = 0;
		int beginDate3 = 0;
		boolean attempted = false;
		
		try {
			
			
			queryBDCXStmnt.setInt(1, stockID);
			ResultSet rs1 = queryBDCXStmnt.executeQuery();
			int loopCount = 0;
			int targetCount = 2;
			int beginBDCX = 0;
			boolean firstLoop = true;
			while (rs1.next()) {
				int bdcx = rs1.getInt(1);
				int dateID = rs1.getInt(2);

				if (firstLoop && (bdcx == 1 || bdcx == -1)) {
					// we should just ignore the first bdcx
					targetCount = 1;
				}

				if (loopCount >= targetCount) {
					if (endDate1 == 0 && beginDate1 == 0) {
						endDate1 = dateID;
						bdcx1 = bdcx;
						beginBDCX = bdcx;
						queryEndPrice.setInt(1, stockID);
						queryEndPrice.setInt(2, endDate1);
						ResultSet rs2 = queryEndPrice.executeQuery();
						if (rs2.next()) {
							endPrice1 = rs2.getFloat(1);
						}
						if (rs1.next()) {
							// bdcx = rs1.getInt(1);//should be +1 or -1
							beginDate1 = rs1.getInt(2);

							queryBeginPrice.setInt(1, stockID);
							queryBeginPrice.setInt(2, beginDate1);
							ResultSet rs3 = queryBeginPrice.executeQuery();
							if (rs3.next()) {
								beginPrice1 = rs3.getFloat(1);
							}

							queryHighLowPrice.setInt(1, stockID);
							queryHighLowPrice.setInt(2, beginDate1);
							queryHighLowPrice.setInt(3, endDate1);
							ResultSet rs4 = queryHighLowPrice.executeQuery();
							if (rs4.next()) {
								closeHigh1 = rs4.getFloat(1);
								closeLow1 = rs4.getFloat(2);
							}
						}

					} else if (endDate2 == 0 && beginDate2 == 0) {
						endDate2 = dateID;
						bdcx2 = bdcx;
						queryEndPrice.setInt(1, stockID);
						queryEndPrice.setInt(2, endDate2);
						ResultSet rs5 = queryEndPrice.executeQuery();
						if (rs5.next()) {
							endPrice2 = rs5.getFloat(1);
						}
						if (rs1.next()) {
							// bdcx = rs1.getInt(1);//should be +1 or -1
							beginDate2 = rs1.getInt(2);

							queryBeginPrice.setInt(1, stockID);
							queryBeginPrice.setInt(2, beginDate2);
							ResultSet rs6 = queryBeginPrice.executeQuery();
							if (rs6.next()) {
								beginPrice2 = rs6.getFloat(1);
							}

							queryHighLowPrice.setInt(1, stockID);
							queryHighLowPrice.setInt(2, beginDate2);
							queryHighLowPrice.setInt(3, endDate2);
							ResultSet rs7 = queryHighLowPrice.executeQuery();
							if (rs7.next()) {
								closeHigh2 = rs7.getFloat(1);
								closeLow2 = rs7.getFloat(2);
							}
						}

					} else if (endDate3 == 0 && beginDate3 == 0) {
						endDate3 = dateID;
						bdcx3 = bdcx;
						queryEndPrice.setInt(1, stockID);
						queryEndPrice.setInt(2, endDate3);
						ResultSet rs8 = queryEndPrice.executeQuery();
						if (rs8.next()) {
							endPrice3 = rs8.getFloat(1);
						}
						if (rs1.next()) {
							// bdcx = rs1.getInt(1);//should be +1 or -1
							beginDate3 = rs1.getInt(2);

							queryBeginPrice.setInt(1, stockID);
							queryBeginPrice.setInt(2, beginDate3);
							ResultSet rs9 = queryBeginPrice.executeQuery();
							if (rs9.next()) {
								beginPrice3 = rs9.getFloat(1);
							}

							queryHighLowPrice.setInt(1, stockID);
							queryHighLowPrice.setInt(2, beginDate3);
							queryHighLowPrice.setInt(3, endDate3);
							ResultSet rs10 = queryHighLowPrice.executeQuery();
							if (rs10.next()) {
								closeHigh3 = rs10.getFloat(1);
								closeLow3 = rs10.getFloat(2);
							}
						}

						attempted = true;
						boolean merged = false;
						// merge logic
						if ((bdcx1 < 0 && bdcx2 < 0) || (bdcx1 > 0 && bdcx2 > 0)) { // same sign merge
							// we need to adjust the new end bdcx value
							// then updateBDW
							int bdw = Math.abs(bdcx1) + Math.abs(bdcx2);
							if (bdcx1 < 0) {
								bdcx1 = -bdw;
								updateBDW.setInt(1, -bdw);
							} else {
								bdcx1 = bdw;
								updateBDW.setInt(1, bdw);
							}
							updateBDW.setInt(2, stockID);
							updateBDW.setInt(3, endDate1);
							updateBDW.executeUpdate();

							// skip the next few points

							// update the last
							if (bdcx1 < 0) {
								updateBDW.setInt(1, -1);
							} else {
								updateBDW.setInt(1, 1);
							}
							updateBDW.setInt(2, stockID);
							updateBDW.setInt(3, beginDate2);
							updateBDW.executeUpdate();

							// set previous -1 to zero
							updateBDWZero.setInt(1, stockID);
							updateBDWZero.setInt(2, beginDate2);
							updateBDWZero.setInt(3, endDate1);
							if (bdcx1 < 0) {
								updateBDWZero.setInt(4, -1);
							} else {
								updateBDWZero.setInt(4, 1);
							}
							updateBDWZero.executeUpdate();

							// reassign logic, through reassign logic, we skip points
							// of beginDate1, endDate2, beginDate2, enbdDate3 update again BDW column
							if (bdcx1 < 0) {
								bdcx1 = -bdw;
							} else {
								bdcx1 = bdw;
							}
							// endPrice1 = endPrice1;
							// endDate1 = endDate1 ;
							beginDate1 = beginDate2;
							beginPrice1 = beginPrice2;

							// need to get new closeHigh1 and closeLow1
							queryHighLowPrice.setInt(1, stockID);
							queryHighLowPrice.setInt(2, beginDate1);
							queryHighLowPrice.setInt(3, endDate1);
							ResultSet rs11 = queryHighLowPrice.executeQuery();
							if (rs11.next()) {
								closeHigh1 = rs11.getFloat(1);
								closeLow1 = rs11.getFloat(2);
							}

							// need to reset zero to make loop logic effective again

							bdcx2 = bdcx3;
							closeHigh2 = closeHigh3;
							closeLow2 = closeLow3;
							endPrice2 = endPrice3;
							beginPrice2 = beginPrice3;
							endDate2 = endDate3;
							beginDate2 = beginDate3;

							endDate3 = 0;
							beginDate3 = 0;

							merged = true;
							// beginBDCX = -1;
							// end same sign merge
						} else if (bdcx1 < 0 && bdcx2 > 0) {

							// so we have -bdcx, then +bdcx, then -bdcx
							// the goal is that if +bdcx is <13, and part of continue drop
							// then we could merge this as a big drop
							// auto merge if bdcx2<10 and >0 (implied)
							if (bdcx2 < 10 || (closeHigh3 > closeHigh2 && bdcx2 < 13 && bdcx2 > 0)) { // we can merge
								// we need to adjust the new end bdcx value
								// then updateBDW
								int bdw = -(Math.abs(bdcx1) + Math.abs(bdcx2) + Math.abs(bdcx3));
								updateBDW.setInt(1, bdw);
								updateBDW.setInt(2, stockID);
								updateBDW.setInt(3, endDate1);
								updateBDW.executeUpdate();

								// skip the next few points

								// update the last

								updateBDW.setInt(1, -1);
								updateBDW.setInt(2, stockID);
								updateBDW.setInt(3, beginDate3);
								updateBDW.executeUpdate();

								// set previous -1 to zero
								updateBDWZero.setInt(1, stockID);
								updateBDWZero.setInt(2, beginDate3);
								updateBDWZero.setInt(3, endDate1);
								updateBDWZero.setInt(4, -1);
								updateBDWZero.executeUpdate();

								// reassign logic, through reassign logic, we skip points
								// of beginDate1, endDate2, beginDate2, enbdDate3 update again BDW column
								bdcx1 = bdw;
								// endPrice1 = endPrice1;
								// endDate1 = endDate1 ;
								beginDate1 = beginDate3;
								beginPrice1 = beginPrice3;

								// need to get new closeHigh1 and closeLow1
								queryHighLowPrice.setInt(1, stockID);
								queryHighLowPrice.setInt(2, beginDate1);
								queryHighLowPrice.setInt(3, endDate1);
								ResultSet rs11 = queryHighLowPrice.executeQuery();
								if (rs11.next()) {
									closeHigh1 = rs11.getFloat(1);
									closeLow1 = rs11.getFloat(2);
								}

								// need to reset zero to make loop logic effective again
								endDate2 = 0;
								beginDate2 = 0;
								endDate3 = 0;
								beginDate3 = 0;
								merged = true;
								// beginBDCX = -1;

							}
							// end if (closeHigh3 > closeHigh2 && bdcx2 < 13 && bdcx2>0) { // we can merge

						} else if (bdcx1 > 0 && bdcx2 < 0) { // beginBDCX>0
							// so we have +bdcx, then -bdcx, then +bdcx
							// the goal is that if -bdcx is <13, and part of shallow drop in big up trend
							// then we could merge this as a big up trend
							// auto merge if bdcx2>-10
							if (bdcx2 > -10
									|| (endPrice3 < endPrice1 && closeHigh3 < closeHigh1 && -bdcx2 < 13 && bdcx2 < 0)) { // we
																															// can
																															// merge
								// we need to adjust the new end bdcx value
								// then updateBDW
								int bdw = Math.abs(bdcx1) + Math.abs(bdcx2) + Math.abs(bdcx3);
								updateBDW.setInt(1, bdw);
								updateBDW.setInt(2, stockID);
								updateBDW.setInt(3, endDate1);
								updateBDW.executeUpdate();

								// skip the next few points

								// update the last
								updateBDW.setInt(1, 1);
								updateBDW.setInt(2, stockID);
								updateBDW.setInt(3, beginDate3);
								updateBDW.executeUpdate();

								// set previous 11 to zero
								updateBDWZero.setInt(1, stockID);
								updateBDWZero.setInt(2, beginDate3);
								updateBDWZero.setInt(3, endDate1);
								updateBDWZero.setInt(4, 1);
								updateBDWZero.executeUpdate();

								// reassign logic, through reassign logic, we skip points
								// of beginDate1, endDate2, beginDate2, enbdDate3 update again BDW column
								bdcx1 = bdw;
								// endPrice1 = endPrice1;
								// endDate1 = endDate1 ;
								beginDate1 = beginDate3;
								beginPrice1 = beginPrice3;

								// need to get new closeHigh1 and closeLow1
								queryHighLowPrice.setInt(1, stockID);
								queryHighLowPrice.setInt(2, beginDate1);
								queryHighLowPrice.setInt(3, endDate1);
								ResultSet rs12 = queryHighLowPrice.executeQuery();
								if (rs12.next()) {
									closeHigh1 = rs12.getFloat(1);
									closeLow1 = rs12.getFloat(2);
								}

								// need to reset zero to make loop logic effective again
								endDate2 = 0;
								beginDate2 = 0;
								endDate3 = 0;
								beginDate3 = 0;
								merged = true;
								// beginBDCX = 1;

							}
							// end if (closeHigh3 > closeHigh2 && bdcx2 < 13 && bdcx2>0) { // we can merge

						} // end bginBDCX>0

						// if we could not merge then update BDW
						// reassign logic
						if (!merged) {
							updateBDW.setInt(1, bdcx1);
							updateBDW.setInt(2, stockID);
							updateBDW.setInt(3, endDate1);
							updateBDW.executeUpdate();

							if (bdcx1 > 0) {
								updateBDW.setInt(1, 1);
							} else {
								updateBDW.setInt(1, -1);
							}
							updateBDW.setInt(2, stockID);
							updateBDW.setInt(3, beginDate1);
							updateBDW.executeUpdate();

							bdcx1 = bdcx2;
							bdcx2 = bdcx3;
							closeHigh1 = closeHigh2;
							closeHigh2 = closeHigh3;
							closeLow1 = closeLow2;
							closeLow2 = closeLow3;
							endPrice1 = endPrice2;
							endPrice2 = endPrice3;
							beginPrice1 = beginPrice2;
							beginPrice2 = beginPrice3;
							endDate1 = endDate2;
							endDate2 = endDate3;
							beginDate1 = beginDate2;
							beginDate2 = beginDate3;
							endDate3 = 0;
							beginDate3 = 0;
							// beginBDCX = -beginBDCX;
						}

					}
				} else { // if (loopCount >= targetCount) {
					// we need move the first 1 or 2 records over
					updateBDW.setInt(1, bdcx);
					updateBDW.setInt(2, stockID);
					updateBDW.setInt(3, dateID);
					updateBDW.executeUpdate();
					
				    if(bdcx>2 && loopCount == 0) {
				    	//for the last day only
				    	//we need to reset previous day BDW to zero for daily case
				    	//but we don't want to erase bdcx=1
				    	updateBDW.setInt(1, 0);
						updateBDW.setInt(2, stockID);
						updateBDW.setInt(3, dateID-1);
						updateBDW.executeUpdate();
				    	
				    }
					loopCount++;
					firstLoop = false;
				}

				//we only need to process last merge, optimized for daily process
				//once attempted, even not merged we should stop as all previous
				//records have already been attempted anyway
				//if total count less than 6, loop through all of them not that 
				//expensive anyway
				if(lastDayOnly && attempted) {
					break;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

}
