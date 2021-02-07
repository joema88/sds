package com.sds.util;

import java.util.ArrayList;
import java.util.List;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import java.util.Calendar;

public class FinVizDownload {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			downLoadFinViz("BIDU");
			//downLoadStockHistory("SPY");
		}catch(Exception ex){
			ex.printStackTrace(System.out);
		}

	}
	
public static String  downLoadFinViz(String stock) throws Exception{
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int month = Calendar.getInstance().get(Calendar.MONTH)+1;
		int day = Calendar.getInstance().get(Calendar.DAY_OF_MONTH)-1;
		System.out.println(year+" "+month+" "+day);
		//String url = "http://real-chart.finance.yahoo.com/table.csv?s="+symbol+"&d="+month+"&e="+day+"&f="+year+"&g=d&a=0&b=01&c=1900&ignore=.csv";
		//String url ="http://www.wsj.com/mdc/public/page/2_3022-intlstkidx.html";
		//String url ="https://finance.yahoo.com/quote/%5EKLSE/?p=%5EKLSE";
		//http://www.wsj.com/mdc/public/page/2_3022-intlstkidx-20180529.html?mod=mdc_pastcalendar
		String date=""+year;
		if(month<10){
			date = date+"0"+month;
		}else{
			date = date+month;
		}
		
		if(day<10){
			date = date+"0"+day;
		}else{
			date = date+day;
		}
		
		
		
		String fileName = date+".html";
		System.out.println(date);
		String url ="https://finviz.com/quote.ashx?t="+ stock;
				System.out.println(url);
		HttpGet httpGet = new HttpGet(url);
		CloseableHttpResponse response1 = httpclient.execute(httpGet);
		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		try {
		    System.out.println(response1.getStatusLine());
		    HttpEntity entity1 = response1.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		   // EntityUtils.consume(entity1);
		    InputStream is = entity1.getContent();
		    
		    String filePath = "/home/joma/share/test/finviz/"+fileName;
		    FileOutputStream fos = new FileOutputStream(new File(filePath));
		    int inByte;
		    while((inByte = is.read()) != -1) fos.write(inByte);
		    is.close();
		    fos.close();
		} finally {
		    response1.close();
		}

		return fileName;
		/*
		HttpPost httpPost = new HttpPost("http://targethost/login");
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("username", "vip"));
		nvps.add(new BasicNameValuePair("password", "secret"));
		httpPost.setEntity(new UrlEncodedFormEntity(nvps));
		CloseableHttpResponse response2 = httpclient.execute(httpPost);

		try {
		    System.out.println(response2.getStatusLine());
		    HttpEntity entity2 = response2.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		    EntityUtils.consume(entity2);
		   
		} finally {
		    response2.close();
		}
		*/
    }
	
private static String encodeValue(String value) throws Exception {
    return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
}
public static void downLoadYahooIndexHistory(String symbol) throws Exception{
	String ensymbol = encodeValue(symbol);
	CloseableHttpClient httpclient = HttpClients.createDefault();
	String url ="https://query1.finance.yahoo.com/v7/finance/download/"+ensymbol+"?period1=1254929589&period2=1557521589&interval=1d&events=history&crumb=UMv9KSRErSP";
			System.out.println(url);
	HttpGet httpGet = new HttpGet(url);
	CloseableHttpResponse response1 = httpclient.execute(httpGet);
	// The underlying HTTP connection is still held by the response object
	// to allow the response content to be streamed directly from the network socket.
	// In order to ensure correct deallocation of system resources
	// the user MUST call CloseableHttpResponse#close() from a finally clause.
	// Please note that if response content is not fully consumed the underlying
	// connection cannot be safely re-used and will be shut down and discarded
	// by the connection manager. 
	try {
	    System.out.println(response1.getStatusLine());
	    HttpEntity entity1 = response1.getEntity();
	    // do something useful with the response body
	    // and ensure it is fully consumed
	   // EntityUtils.consume(entity1);
	    InputStream is = entity1.getContent();
	    String filePath = "C:\\stock\\yahoo\\history\\"+symbol+".csv";
	    FileOutputStream fos = new FileOutputStream(new File(filePath));
	    int inByte;
	    while((inByte = is.read()) != -1) fos.write(inByte);
	    is.close();
	    fos.close();
	} finally {
	    response1.close();
	}

	
}


	public static void downLoadStockHistory(String symbol) throws Exception{
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int month = Calendar.getInstance().get(Calendar.MONTH)+1;
		int day = Calendar.getInstance().get(Calendar.DAY_OF_MONTH)-1;
		System.out.println(year+" "+month+" "+day);
		//String url = "http://real-chart.finance.yahoo.com/table.csv?s="+symbol+"&d="+month+"&e="+day+"&f="+year+"&g=d&a=0&b=01&c=1900&ignore=.csv";
		//String url ="http://www.wsj.com/mdc/public/page/2_3022-intlstkidx.html";
		//String url ="https://finance.yahoo.com/quote/%5EKLSE/?p=%5EKLSE";
		//http://www.wsj.com/mdc/public/page/2_3022-intlstkidx-20180529.html?mod=mdc_pastcalendar
		String date=""+year;
		if(month<10){
			date = date+"0"+month;
		}else{
			date = date+month;
		}
		
		if(day<10){
			date = date+"0"+day;
		}else{
			date = date+day;
		}
		
		System.out.println(date);
		String url ="http://www.wsj.com/mdc/public/page/2_3022-intlstkidx-" +date +".html?mod=mdc_pastcalendar";
				System.out.println(url);
		HttpGet httpGet = new HttpGet(url);
		CloseableHttpResponse response1 = httpclient.execute(httpGet);
		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		try {
		    System.out.println(response1.getStatusLine());
		    HttpEntity entity1 = response1.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		   // EntityUtils.consume(entity1);
		    InputStream is = entity1.getContent();
		    String filePath = "C:\\stock\\yahoo\\indices\\"+symbol+".csv";
		    FileOutputStream fos = new FileOutputStream(new File(filePath));
		    int inByte;
		    while((inByte = is.read()) != -1) fos.write(inByte);
		    is.close();
		    fos.close();
		} finally {
		    response1.close();
		}

		/*
		HttpPost httpPost = new HttpPost("http://targethost/login");
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("username", "vip"));
		nvps.add(new BasicNameValuePair("password", "secret"));
		httpPost.setEntity(new UrlEncodedFormEntity(nvps));
		CloseableHttpResponse response2 = httpclient.execute(httpPost);

		try {
		    System.out.println(response2.getStatusLine());
		    HttpEntity entity2 = response2.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		    EntityUtils.consume(entity2);
		   
		} finally {
		    response2.close();
		}
		*/
    }
	
	
public static void downLoadStockHistory(String downLoadSymbol, String symbol) throws Exception{
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int month = Calendar.getInstance().get(Calendar.MONTH);
		int day = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
		//String url = "http://chart.finance.yahoo.com/table.csv?s="+downLoadSymbol+"&d="+month+"&e="+day+"&f="+year+"&g=d&a=0&b=01&c=1900&ignore=.csv";
		String url = "http://query1.finance.yahoo.com/v7/finance/download/%5EMERV?period1=1524671079&period2=1527263079&interval=1d&events=history&crumb=MI1xJehSWzX";
		System.out.println(url);
		HttpGet httpGet = new HttpGet(url);
		CloseableHttpResponse response1 = httpclient.execute(httpGet);
		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		try {
		    System.out.println(response1.getStatusLine());
		    HttpEntity entity1 = response1.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		   // EntityUtils.consume(entity1);
		    InputStream is = entity1.getContent();
		    String filePath = "C:\\stock\\yahoo\\indices\\"+symbol+".csv";
		    FileOutputStream fos = new FileOutputStream(new File(filePath));
		    int inByte;
		    while((inByte = is.read()) != -1) fos.write(inByte);
		    is.close();
		    fos.close();
		} finally {
		    response1.close();
		}

		/*
		HttpPost httpPost = new HttpPost("http://targethost/login");
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("username", "vip"));
		nvps.add(new BasicNameValuePair("password", "secret"));
		httpPost.setEntity(new UrlEncodedFormEntity(nvps));
		CloseableHttpResponse response2 = httpclient.execute(httpPost);

		try {
		    System.out.println(response2.getStatusLine());
		    HttpEntity entity2 = response2.getEntity();
		    // do something useful with the response body
		    // and ensure it is fully consumed
		    EntityUtils.consume(entity2);
		   
		} finally {
		    response2.close();
		}
		*/
    }
	

}
