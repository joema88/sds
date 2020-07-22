package com.backtest.driver;

import java.sql.PreparedStatement;
import com.sds.db.*;
import java.sql.*;

import com.sds.db.DB;

public class DoubleBullBuy {

	public static void main(String[] args) {
		try {
			float totalInput = 0.0f;
			float totalNow = 0.0f;

			long t1 = System.currentTimeMillis();
			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dBulls = DB.getDBulls();
			allStocks.setInt(1, 6171);

			ResultSet rs = allStocks.executeQuery();

			while (rs.next()) {
				int stockID = rs.getInt(1);
				// 7/17/2020
				//dBulls.setInt(1, 8958);
				dBulls.setInt(1, stockID);

				ResultSet rs2 = dBulls.executeQuery();

				float totalInput1 = 0.0f;
				float totalNow1 = 0.0f;
				int totalShares = 0;
				int preDateId = 0;
				while (rs2.next()) {
					int dateId = rs2.getInt(1);
					int pass = rs2.getInt(4);
					int apas = rs2.getInt(5);
					String symbol = rs2.getString(6);
					float price = rs2.getFloat(7);
					if (dateId < 8957) {
						if((dateId-preDateId)>10&&(pass>=1||apas>=1)) {
						totalInput1 = totalInput1 + 100.0f * price;
						totalShares = totalShares + 100;
						//System.out.println("bUY... ");
						//totalNow1 = totalNow1 + 100 * 1.0f * price;
						preDateId = dateId;
						}
						
						if((2.0f*totalInput1<(totalShares * 1.0f * price))||(totalInput1>1.2f*(totalShares * 1.0f * price))) {
							totalNow1 = totalNow1+totalShares * 1.0f * price;
							totalShares = 0;
						}
					} else if (dateId == 8957) {
						//System.out.println("sTOCK ID "+symbol+" dateId "+dateId+" total "+totalInput1 );
						
						totalNow1 = totalNow1 + totalShares * 1.0f * price;
						if (totalInput1 > 1.0f) {
							System.out.println(
									"For " + symbol + " total input " + totalInput1 + " total now " + totalNow1);
							totalInput = totalInput + totalInput1;
							totalNow = totalNow + totalNow1;
						}

					}
				}

			}

			System.out.println("For all stocks total input " + totalInput + " total now " + totalNow);

			DB.closeConnection();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

}
