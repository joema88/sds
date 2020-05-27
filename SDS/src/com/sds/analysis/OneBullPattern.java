package com.sds.analysis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.db.DB;
import com.sds.db.OneBullDB;

public class OneBullPattern {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		processStock("MSFT",-1);

	}

	private static PreparedStatement dayLengthStmnt = null;
	private static PreparedStatement querySC5SumStmnt = null;
	private static PreparedStatement aboveSC5CountStmnt = null;
	private static PreparedStatement pt9BullStmnt = null;
	private static PreparedStatement maxCloseFindStmnt = null;
	private static PreparedStatement minCloseFindStmnt = null;
	private static PreparedStatement updateMaxStmnt = null;
	private static PreparedStatement boundaryQueryStmnt = null;
	private static PreparedStatement findPreviousYP10ZeroStmnt = null;
	private static PreparedStatement updateBullPointStmnt = null;


	
	public static void processStock(String symbol, int stockID) {
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		pt9BullStmnt = OneBullDB.getPT9BullStmnt(stockID, -1, -1);
		findPreviousYP10ZeroStmnt = OneBullDB.getPreviousYP10OneStmnt();
		querySC5SumStmnt = OneBullDB.getSC5SumStmnt();
		aboveSC5CountStmnt = OneBullDB.getAboveSC5CountStmnt();
		dayLengthStmnt = OneBullDB.getBoundaryLengthStmnt();

		try {
			//the SC5>=8 condition could be SC5>=0 for big cap like MSFT?
			pt9BullStmnt.setInt(1, stockID);
			ResultSet rs = pt9BullStmnt.executeQuery();

			while (rs.next()) {
				stockID = rs.getInt(1);
				int dateID = rs.getInt(2);
				System.out.println("BT9 Bull found at "+DB.getDateStr(dateID));
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
					System.out.println("Find max "+close+" at "+DB.getDateStr(dateC));
					updateMaxStmnt = OneBullDB.getUpdateMaxStmnt();
					updateMaxStmnt.setFloat(1, close);
					updateMaxStmnt.setInt(2, stockID);
					updateMaxStmnt.setInt(3, dateID);

				}else {
					System.out.println("No max found");
				}

				boundaryQueryStmnt = OneBullDB.getNextBoundaryStmnt(stockID, SC5, dateID);
				
				boundaryQueryStmnt.setInt(1, stockID);
				boundaryQueryStmnt.setInt(2,dateID);
				
				ResultSet rs2 = boundaryQueryStmnt.executeQuery();
				
				if(rs2.next()) {
					//int SC5d2 = rs2.getInt(1);
					
					int dateId2 = rs2.getInt(2);
					System.out.println("boundary date at "+DB.getDateStr(dateId2));
					findPreviousYP10ZeroStmnt.setInt(1,stockID);
					findPreviousYP10ZeroStmnt.setInt(2, dateId2);
					
					ResultSet rs3 = findPreviousYP10ZeroStmnt.executeQuery();
					
					if(rs3.next()) {
						//the previous start day should be 10 days with no yellow/pink
						//so we need to set up a YP10 field to tally count, done!
						int startDateId = rs3.getInt(1);
						System.out.println("startDateId "+DB.getDateStr(startDateId));
						System.out.println("End DateId "+DB.getDateStr(dateId2));
						System.out.println("Real startDateId "+DB.getDateStr(startDateId));
						querySC5SumStmnt.setInt(1, stockID);
						querySC5SumStmnt.setInt(2, dateId2);
						//skip the next 5 days after BT9
						querySC5SumStmnt.setInt(3,startDateId);
						
						ResultSet rs4 = querySC5SumStmnt.executeQuery();
						int sc5Sum = 0;
						if(rs4.next()) {
							sc5Sum = rs4.getInt(1);
						}
						
						aboveSC5CountStmnt.setInt(1,stockID);
						aboveSC5CountStmnt.setInt(2, dateId2);
						aboveSC5CountStmnt.setInt(3,startDateId);
						aboveSC5CountStmnt.setInt(4,10);
						
						ResultSet rs5 = aboveSC5CountStmnt.executeQuery();
						int sc5Above10 = 0;
						if(rs5.next()) {
							sc5Above10 = rs5.getInt(1);
						}
						
						aboveSC5CountStmnt.setInt(1,stockID);
						aboveSC5CountStmnt.setInt(2, dateId2);
						aboveSC5CountStmnt.setInt(3,startDateId);
						aboveSC5CountStmnt.setInt(4,6);
						
						ResultSet rs6= aboveSC5CountStmnt.executeQuery();
						int sc5Above6 = 0;
						if(rs6.next()) {
							sc5Above6 = rs6.getInt(1);
						}
						
						
						
						maxCloseFindStmnt = OneBullDB.getMaxCloseFindStmnt();
						maxCloseFindStmnt.setInt(1, stockID);
						maxCloseFindStmnt.setInt(2, startDateId);
						maxCloseFindStmnt.setInt(3, dateID);
						
						ResultSet rs8 = maxCloseFindStmnt.executeQuery();
						float closePreMax = 0.0f;
						int maxDateId = 0;
						if(rs8.next()) {
							closePreMax = rs8.getFloat(1);
							maxDateId = rs8.getInt(2);
						}
						System.out.println("Find preMax "+closePreMax+" at "+DB.getDateStr(maxDateId));
						
						//the down leg is defined from peak close price to SC5 change into positive before the 9 Teals
						//this is better than sql it tells direction of max/min
						int days = dateId2 - maxDateId;
						System.out.println("Calculation "+days);
						minCloseFindStmnt = OneBullDB.getMinCloseFindStmnt();
						minCloseFindStmnt.setInt(1, stockID);
						minCloseFindStmnt.setInt(2, startDateId);
						minCloseFindStmnt.setInt(3, dateID);
						
						ResultSet rs9 = minCloseFindStmnt.executeQuery();
						float closePreMin = 0.0f;
						int minDateId = 0;
						if(rs9.next()) {
							closePreMin = rs9.getFloat(1);
							minDateId = rs9.getInt(2);
						}
						System.out.println("Find preMin "+closePreMin+" at "+DB.getDateStr(minDateId));
						//	String query = "UPDATE BBROCK SET PTCP = ?, TSC5 = ?, 
						//DAYS = ?, GT10 = ?, GT6 = ? WHERE STOCKID = ? AND DATEID =? ";
						
						updateBullPointStmnt = OneBullDB.getUpdateBullPointStmnt();
						float PTCP = 100.0f*(closePreMin - closePreMax )/closePreMax;
						//max happens later after min
						if(days<0)
							PTCP = 100.0f*(closePreMax - closePreMin )/closePreMin;
						System.out.println("PTCP is "+PTCP);
						updateBullPointStmnt.setFloat(1, PTCP);
						updateBullPointStmnt.setInt(2,sc5Sum);
						updateBullPointStmnt.setInt(3, days);
						updateBullPointStmnt.setInt(4,sc5Above10);
						updateBullPointStmnt.setInt(5,sc5Above6);
						updateBullPointStmnt.setInt(6,stockID);
						updateBullPointStmnt.setInt(7, dateID);
						updateBullPointStmnt.executeUpdate();
						System.out.println("One bull point processed at..."+dateID);
						//break;
						
					}else { //start from begin of stock record history)
						
					}
					
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

}
