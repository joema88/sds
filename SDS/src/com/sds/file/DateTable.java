package com.sds.file;

import com.sds.db.*;
import java.sql.*;

public class DateTable {
	
	public static int getDateID(String dateStr) {
		int dateID = 0;
		try {
		dateID = DB.getDateID(dateStr);
		if(dateID==0) {
			int nextID = DB.getNextDateID();
			PreparedStatement stmnt = DB.getDateInsertStatement();
			stmnt.setInt(1,nextID);
			stmnt.setString(2, dateStr);
			stmnt.execute();
		}
		}catch(Exception ex) {
			System.out.println("Date insertion failed...");
			ex.printStackTrace(System.out);
		}
		
		return dateID;
	}

}
