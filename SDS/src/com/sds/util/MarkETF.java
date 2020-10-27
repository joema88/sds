package com.sds.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.PreparedStatement;

import com.sds.db.DB;

public class MarkETF {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			PreparedStatement updateStockSectorStmnt = DB.updateStockSectorStmnt();
			String file = "/home/joma/share/test/ETF.txt";
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = br.readLine()) != null) {
				String symbol = line.strip();
				System.out.println("ETF " + symbol);
				//String query = "UPDATE SYMBOLS SET INDID = ?, SUBID =? WHERE SYMBOL= ? ";
				updateStockSectorStmnt.setInt(1, 69);
				updateStockSectorStmnt.setInt(2, 1);
				updateStockSectorStmnt.setString(3, symbol);
				updateStockSectorStmnt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);

		}
	}

}
