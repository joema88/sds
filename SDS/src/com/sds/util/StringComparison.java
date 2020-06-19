package com.sds.util;

public class StringComparison {

	public static void main(String[] args) {
		String str1 = "String me/thod tutorial";
	       String str2 = "compareTo method example";
	       String str3 = "String method tutorial";
	       
	       int var0 = str1.indexOf("/");
	       System.out.println("str1 index of / is "+var0);

	       int var1 = str1.compareTo( str2.toUpperCase() );
	       System.out.println("str1 & str2 comparison: "+var1);

	       int var2 = str1.compareTo( str3.toUpperCase()  );
	       System.out.println("str1 & str3 comparison: "+var2);

	       int var3 = str2.compareTo("compareTo method example");
	       System.out.println("str2 & string argument comparison: "+var3);
	}

}
