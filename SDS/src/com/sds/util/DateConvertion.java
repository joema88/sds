package com.sds.util;

import java.text.SimpleDateFormat;
import java.util.*;

public class DateConvertion {

	public static String convertDateFormat(String strDate) {
		String date1 = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("M/dd/yy");
			Date date = sdf.parse(strDate);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
			date1 = format1.format(date);
			//System.out.println("Converted date string is " + date1);
		} catch (Exception ex) {
			System.out.println("Date convertion error");
			ex.printStackTrace(System.out);
		}

		return date1;

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String strDate = "12/22/20";

		try {
			SimpleDateFormat sdf = new SimpleDateFormat("M/dd/yy");
			Date date = sdf.parse(strDate);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
			String date1 = format1.format(date);
			System.out.println("Converted date string is " + date1);
		} catch (Exception ex) {
			System.out.println("Date convertion error");
			ex.printStackTrace(System.out);
		}

	}

}
