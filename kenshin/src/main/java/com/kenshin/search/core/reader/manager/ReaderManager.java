package com.kenshin.search.core.reader.manager;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import com.kenshin.search.core.reader.filter.IFilterDirectoryReader;
import com.kenshin.search.core.reader.filter.IFilterDirectoryReader.ISubReaderWrapper;
import com.kenshin.search.core.reader.query.CommonQuery;
import com.kenshin.search.core.reader.reader.CommonSearcher;

public class ReaderManager {
	
	private static final Logger logger = Logger.getLogger(ReaderManager.class);
	
	private final Analyzer analyzer = new StandardAnalyzer(); //分词器
	
	private DirectoryReader coreReader = null; //coreIndexReader
	private Map<Long, DirectoryReader> segDirectoryReaders = new ConcurrentHashMap<Long, DirectoryReader>(); //segReader key:directoryId value:segDirectoryReader
	private Map<String, DirectoryReader> ramDirectoryReaders = new ConcurrentHashMap<String, DirectoryReader>(); //ramReader key:indexerName value:ramDirectoryReader
	
	private Map<String, Boolean> ramModelIdMap = new ConcurrentHashMap<String, Boolean>(); //ram中存在的modelId, 用来过滤
	private Map<String, Boolean> segModelIdMap = new ConcurrentHashMap<String, Boolean>(); //seg中存在的modelId， 用来过滤
	
	public ReaderManager() {
		super();
	}
	
	/*************************** 提供查询功能 ******************************************/
	public List<Document> CommonQuery(CommonQuery commonQuery, int hitsPerPage) throws ParseException, IOException {
		
		Query q = MultiFieldQueryParser.parse(commonQuery.getQueries(), commonQuery.getFields(), analyzer);
		
		List<DirectoryReader> readers = sortCommonReaders();
		
		CommonSearcher commonSearcher = new CommonSearcher(q, readers, hitsPerPage);
		return commonSearcher.query();
	}

	private List<DirectoryReader> sortCommonReaders() throws IOException {
		List<DirectoryReader> readers = new LinkedList<DirectoryReader>();
		//RAMReaders
		readers.addAll(ramDirectoryReaders.values());
		
		//segReaders
		for(Map.Entry<Long, DirectoryReader> entry : segDirectoryReaders.entrySet()) {
			DirectoryReader segFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
			if(segFilterReader != null) {
				readers.add(segFilterReader);
			}
		}
		//coreReader
		Map<String, Boolean> tmpModelIdMap = new LinkedHashMap<String, Boolean>();
		tmpModelIdMap.putAll(ramModelIdMap);
		tmpModelIdMap.putAll(segModelIdMap);
		DirectoryReader coreFilterReader = new IFilterDirectoryReader(coreReader, new ISubReaderWrapper(tmpModelIdMap));
		
		if(coreFilterReader != null)
			readers.add(coreFilterReader);
		
		return readers;
	}
	
	/************************************** get/set *******************************************/
	public DirectoryReader getCoreReader() {
		return coreReader;
	}

	public void setCoreReader(DirectoryReader coreReader) {
		this.coreReader = coreReader;
	}

	public Map<Long, DirectoryReader> getSegDirectoryReaders() {
		return segDirectoryReaders;
	}

	public void setSegDirectoryReaders(
			Map<Long, DirectoryReader> segDirectoryReaders) {
		this.segDirectoryReaders = segDirectoryReaders;
	}

	public Map<String, DirectoryReader> getRamDirectoryReaders() {
		return ramDirectoryReaders;
	}

	public void setRamDirectoryReaders(
			Map<String, DirectoryReader> ramDirectoryReaders) {
		this.ramDirectoryReaders = ramDirectoryReaders;
	}

	public Map<String, Boolean> getRamModelIdMap() {
		return ramModelIdMap;
	}

	public void setRamModelIdMap(Map<String, Boolean> ramModelIdMap) {
		this.ramModelIdMap = ramModelIdMap;
	}

	public Map<String, Boolean> getSegModelIdMap() {
		return segModelIdMap;
	}

	public void setSegModelIdMap(Map<String, Boolean> segModelIdMap) {
		this.segModelIdMap = segModelIdMap;
	}
	
	
	/************************************** 测试代码 *******************************************/
//	ramModelIdMap.put("1", true);
//	logger.debug("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ramModelIdMap is : " + ramModelIdMap.size() + " map : " + ramModelIdMap.get("1"));
//	for(Map.Entry<String, DirectoryReader> entry : ramDirectoryReaders.entrySet()) {
//		logger.debug("<<<<<<<<<<<<<<<<<<<<<<<<<< entry value :" + entry.getValue());
//		DirectoryReader ramFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
////		IFilterDirectoryReader ramFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
////		DirectoryReader test = ramFilterReader.doWrapDirectoryReader(entry.getValue(), ramModelIdMap);
//		
//		if(ramFilterReader != null) {
//			readers.add(ramFilterReader); 
//		}
//	}
	
}
