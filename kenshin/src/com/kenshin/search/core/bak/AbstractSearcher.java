package com.kenshin.search.core.bak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.wltea.analyzer.lucene.IKAnalyzer;

//TODO这里应该继承callable或者future，因为searche需要返回值
public abstract class AbstractSearcher implements Runnable {
	
	//Search 类型
	public static final int SEARCH_ALL = 0; 
	public static final int SEARCH_RAM = 1;
	public static final int SEARCH_SEG = 2;
	public static final int SEARCH_INDEX = 3;
	
	private static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer();
	private static final int DEFAULT_HITSPERPAGE = 100000;
	
	protected String searcherName;
	protected Analyzer analyzer;
	protected int hitsPerPage;
	
	public AbstractSearcher(String searcherName) {
		this(searcherName, DEFAULT_ANALYZER, DEFAULT_HITSPERPAGE);
	}
	
	public AbstractSearcher(String searcherName, Analyzer analyzer) {
		this(searcherName, analyzer, DEFAULT_HITSPERPAGE);
	}
	
	public AbstractSearcher(String searcherName, int hitsPerPage) {
		this(searcherName, DEFAULT_ANALYZER, hitsPerPage);
	}
	
	public AbstractSearcher(String searchName, Analyzer analyzer, int hitsPerPage) {
		this.searcherName = searchName;
		this.analyzer = analyzer;
		this.hitsPerPage = hitsPerPage;
	}
	
	public abstract void Query() throws Exception;
	
	@Override
	public void run() {
		try {
			Query();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//搜索总索引
//	public void indexQuery(String query) throws ParseException, IOException {
//
//		Query q = new QueryParser("file1", analyzer).parse(query);
//		Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
//		int hitsPerPage = 10;
//		
//		// 从本地索引文件读取
//		IndexReader reader = DirectoryReader.open(indexDirectory);
//		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
//		IndexSearcher searcher = new IndexSearcher(reader);
//		searcher.search(q, collector);
//		ScoreDoc[] hits = collector.topDocs().scoreDocs;
//		System.out.println("<<<<<<<<<<<<<<<<<<< searcher is searching: " + searchName + " | hits length " + hits.length);
//		for (int i = 0; i < hits.length; ++i) {
//			int docId = hits[i].doc;
//			Document d = searcher.doc(docId);
//			System.out.println("<<<<<<<<<<<<<<<< local : " + d.get("file1"));
//		}
//		reader.close();
//	}
	
	//搜索总索引
//	public void ramQuery(String query, List<Directory> ramDirectorys) throws ParseException, IOException {
//		Query q = new QueryParser("file1", analyzer).parse(query);
//		int hitsPerPage = 20;
//		IndexReader reader = DirectoryReader.open(directory);
//		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
//		IndexSearcher searcher = new IndexSearcher(reader);
//		searcher.search(q, collector);
//
//		ScoreDoc[] hits = collector.topDocs().scoreDocs;
//		System.out.println("<<<<<<<<<<<<<<<<<<< searcher is searching: " + searchName + " | hits length " + hits.length);
//		for (int i = 0; i < hits.length; ++i) {
//			int docId = hits[i].doc;
//			Document d = searcher.doc(docId);
//			System.out.println("<<<<<<<<<<<<<<<< searcher find  : " + d.get("file1"));
//		}
//		reader.close();
//	}
//
//	@Override
//	public void run() {
//		
//	}
	
//	public static void main(String[] args) throws ParseException, IOException {
//		Searcher seacher = new Searcher("mysearch", new StandardAnalyzer(), "D:/indexs/index/");
//		seacher.indexQuery("1");
//	}

}
