package com.sds.analysis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.db.DB;
import com.sds.db.OneBullDB;

public class OneBullPattern {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String stock = "MRNA";
		processStock(stock, -1, -1);
		findPassPoints(stock, -1, false);

	}

	private static PreparedStatement dayLengthStmnt = null;
	private static PreparedStatement querySC5SumStmnt = null;
	private static PreparedStatement pt9BullStmnt = null;
	private static PreparedStatement maxCloseFindStmnt = null;
	private static PreparedStatement minCloseFindStmnt = null;
	private static PreparedStatement updateMaxStmnt = null;
	private static PreparedStatement boundaryQueryStmnt = null;
	private static PreparedStatement findPreviousYP10ZeroStmnt = null;
	private static PreparedStatement updateBullPointStmnt = null;
	private static PreparedStatement ptvalQueryStmnt = null;
	private static PreparedStatement closeAboveQueryStmnt = null;
	private static PreparedStatement updatePassPointStmnt = null;

	// find days that passes qualified BT9 points +5%, continue log for 5 days, if
	// it stays above +4% or 5% above
	public static void findPassPoints(String symbol, int stockID, boolean lastOnly) {
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			int preBullDateID = 0;
			int ppDateID = 0;
			updatePassPointStmnt = OneBullDB.getUpdatePassPointStmnt();
			closeAboveQueryStmnt = OneBullDB.getCloseAboveStmnt();
			ptvalQueryStmnt = OneBullDB.getPTVALQueryStmnt();
			ptvalQueryStmnt.setInt(1, stockID);
			ResultSet rs = ptvalQueryStmnt.executeQuery();

			int lc = 0;
			while (rs.next()) {
				int dateId = rs.getInt(1);
				float val = rs.getFloat(2);
				lc++;

				closeAboveQueryStmnt.setInt(1, stockID);
				closeAboveQueryStmnt.setInt(2, dateId);
				closeAboveQueryStmnt.setFloat(3, val);

				ResultSet rs2 = closeAboveQueryStmnt.executeQuery();
				int pval = 1;
				while (rs2.next()) {
					int pDateId = rs2.getInt(1);

					//modify to allow to overrun the next bullpoint passpoint
					//or allow enough space to log 10 days continuous passes
					if (pDateId < preBullDateID + 10 || preBullDateID == 0) {

						if (ppDateID == 0) {
							pval = 1;
						} else if ((pDateId - ppDateID) == 1) {
							pval++;
						} else {
							pval = 1;
						}

						if(pval<12) { //we only need 10 anyway
						updatePassPointStmnt.setInt(1, pval);
						updatePassPointStmnt.setInt(2, stockID);
						updatePassPointStmnt.setInt(3, pDateId);
						updatePassPointStmnt.executeUpdate();
						}
					}

					ppDateID = pDateId;
				}

				preBullDateID = dateId;

				// for daily process, only need last two processed
				//this is because the 10 continuous pass could run into the next
				//bull point, we need to override the first bull point pass point
				//low values, so we don't miss the 10 cont pass bull points
				if (lastOnly&&lc>=2) {
					break;
				}
			}

			
		} catch (Exception ex) {

		}
	}

	public static void processStock(String symbol, int stockID, int stockDateId) {
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		pt9BullStmnt = OneBullDB.getPT9BullStmnt(stockID, -1, -1);
		if (stockDateId > 0) {
			pt9BullStmnt = OneBullDB.getPT9BullStmnt(stockID, stockDateId, -1);
		}

		findPreviousYP10ZeroStmnt = OneBullDB.getPreviousYP10OneStmnt();
		querySC5SumStmnt = OneBullDB.getSC5SumStmnt();
		dayLengthStmnt = OneBullDB.getBoundaryLengthStmnt();

		try {
			// the SC5>=8 condition could be SC5>=0 for big cap like MSFT?
			pt9BullStmnt.setInt(1, stockID);
			ResultSet rs = pt9BullStmnt.executeQuery();

			while (rs.next()) {
				stockID = rs.getInt(1);
				int dateID = rs.getInt(2);
				System.out.println("BT9 Bull found at " + DB.getDateStr(dateID));
				int SC5 = rs.getInt(3);

				maxCloseFindStmnt = OneBullDB.getMaxCloseFindStmnt();
				maxCloseFindStmnt.setInt(1, stockID);
				maxCloseFindStmnt.setInt(2, dateID - 4);
				maxCloseFindStmnt.setInt(3, dateID);

				ResultSet rs1 = maxCloseFindStmnt.executeQuery();
				float close = 0.0f;

				if (rs1.next()) {
					close = rs1.getFloat(1);
					int dateC = rs1.getInt(2);
					System.out.println("Find max " + close + " at " + DB.getDateStr(dateC));
					updateMaxStmnt = OneBullDB.getUpdateMaxStmnt();
					// above 1.05*(max)Close of last 4 days will be a trigger to buy watch
					// some stock failed right the day after big!!, need to be cautious
					updateMaxStmnt.setFloat(1, close * 1.05f);
					updateMaxStmnt.setInt(2, stockID);
					updateMaxStmnt.setInt(3, dateID);
					updateMaxStmnt.executeUpdate();

				} else {
					System.out.println("No max found");
				}

				boundaryQueryStmnt = OneBullDB.getNextBoundaryStmnt(stockID, SC5, dateID);

				boundaryQueryStmnt.setInt(1, stockID);
				boundaryQueryStmnt.setInt(2, dateID);

				ResultSet rs2 = boundaryQueryStmnt.executeQuery();

				if (rs2.next()) {
					// int SC5d2 = rs2.getInt(1);

					int dateId2 = rs2.getInt(2);
					System.out.println("boundary date at " + DB.getDateStr(dateId2));
					findPreviousYP10ZeroStmnt.setInt(1, stockID);
					findPreviousYP10ZeroStmnt.setInt(2, dateId2);

					ResultSet rs3 = findPreviousYP10ZeroStmnt.executeQuery();

					if (rs3.next()) {
						// the previous start day should be 10 days with no yellow/pink
						// so we need to set up a YP10 field to tally count, done!
						// push back 4 more days to get max high
						int startDateId = rs3.getInt(1) - 4;
						System.out.println("startDateId " + DB.getDateStr(startDateId));
						System.out.println("End DateId " + DB.getDateStr(dateId2));
						System.out.println("Real startDateId " + DB.getDateStr(startDateId));
						querySC5SumStmnt.setInt(1, stockID);
						querySC5SumStmnt.setInt(2, dateId2);
						// skip the next 5 days after BT9
						querySC5SumStmnt.setInt(3, startDateId);

						ResultSet rs4 = querySC5SumStmnt.executeQuery();
						int sc5Sum = 0;
						if (rs4.next()) {
							sc5Sum = rs4.getInt(1);
						}

						maxCloseFindStmnt = OneBullDB.getMaxCloseFindStmnt();
						maxCloseFindStmnt.setInt(1, stockID);
						maxCloseFindStmnt.setInt(2, startDateId);
						maxCloseFindStmnt.setInt(3, dateID);

						ResultSet rs8 = maxCloseFindStmnt.executeQuery();
						float closePreMax = 0.0f;
						int maxDateId = 0;
						if (rs8.next()) {
							closePreMax = rs8.getFloat(1);
							maxDateId = rs8.getInt(2);
						}
						System.out.println("Find preMax " + closePreMax + " at " + DB.getDateStr(maxDateId));

						// the down leg is defined from peak close price to SC5 change into positive
						// before the 9 Teals
						// this is better than sql it tells direction of max/min
						int days = dateId2 - maxDateId;
						System.out.println("Calculation " + days);
						minCloseFindStmnt = OneBullDB.getMinCloseFindStmnt();
						minCloseFindStmnt.setInt(1, stockID);
						minCloseFindStmnt.setInt(2, startDateId);
						minCloseFindStmnt.setInt(3, dateID);

						ResultSet rs9 = minCloseFindStmnt.executeQuery();
						float closePreMin = 0.0f;
						int minDateId = 0;
						if (rs9.next()) {
							closePreMin = rs9.getFloat(1);
							minDateId = rs9.getInt(2);
						}
						System.out.println("Find preMin " + closePreMin + " at " + DB.getDateStr(minDateId));
						// String query = "UPDATE BBROCK SET PTCP = ?, TSC5 = ?,
						// DAYS = ?, GT10 = ?, GT6 = ? WHERE STOCKID = ? AND DATEID =? ";

						updateBullPointStmnt = OneBullDB.getUpdateBullPointStmnt();
						float PTCP = 100.0f * (closePreMin - closePreMax) / closePreMax;
						// max happens later after min
						if (days < 0)
							PTCP = 100.0f * (closePreMax - closePreMin) / closePreMin;
						System.out.println("PTCP is " + PTCP);
						updateBullPointStmnt.setFloat(1, PTCP);
						updateBullPointStmnt.setInt(2, sc5Sum);
						updateBullPointStmnt.setInt(3, days);
						updateBullPointStmnt.setInt(4, stockID);
						updateBullPointStmnt.setInt(5, dateID);
						updateBullPointStmnt.executeUpdate();
						System.out.println("One bull point processed at..." + dateID);
						// break;

					} else { // start from begin of stock record history)

					}

				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

}
