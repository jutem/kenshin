package com.kenshin.search.core.demo;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class AnalyzerFile {

	public static void main(String[] args) {
		String str = "12";  
		List<String> lists = getWords(str, new StandardAnalyzer());  
		for (String s : lists) {  
		    System.out.println(s);  
		}
	}
	
	public static List<String> getWords(String str,Analyzer analyzer){  
	    List<String> result = new ArrayList<String>();  
	    TokenStream stream = null;  
	    try {  
	        stream = analyzer.tokenStream("content", new StringReader(str));  
	        CharTermAttribute attr = stream.addAttribute(CharTermAttribute.class);  
	        stream.reset();  
	        while(stream.incrementToken()){  
	            result.add(attr.toString());  
	        }  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }finally{  
	        if(stream != null){  
	            try {  
	                stream.close();  
	            } catch (IOException e) {  
	                e.printStackTrace();  
	            }  
	        }  
	    }  
	    return result;  
	}
}
