package com.kenshin.search.core.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

public class CoreSearcher extends AbstractSearcher {
	
	private Directory coreDirectory;
	private Map<String, String> searchMap;
	
	private static final String CORE_NAME = "core_search_";
	
	/**
	 * @param searchMap key:field value:query
	 * @param ramDirectories
	 */
	public CoreSearcher(String searchName, Map<String, String> searchMap, Directory coreDirectory) {
		super(CORE_NAME + searchName);
		this.searchMap = searchMap;
		this.coreDirectory = coreDirectory;
	}
	
	public void Query() throws ParseException, IOException {
		
		Query q = MultiFieldQueryParser.parse(searchMap.values().toArray(new String[0]), searchMap.keySet().toArray(new String[0]), analyzer);
//		System.out.println("<<<<<<<<<<<<<<<<<<< search direcotry : " + Arrays.toString(directory.listAll()));
		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(coreDirectory);
		} catch (AlreadyClosedException | IndexNotFoundException e) {
//			e.printStackTrace();
			return;
		}
		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.search(q, collector);

		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		System.out.println("<<<<<<<<<<<<<<<<<<< searcher is searching: " + searcherName + " | hits length " + hits.length);
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
		}
		
		reader.close();
	}
	
}
