package com.kenshin.search.core.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

public class CommonSearcher extends AbstractSearcher {
	
	private List<Directory> directories;
	private Map<String, String> searchMap;
	
	private static final String COMMON_NAME = "common_search_";
	
	/**
	 * @param searchMap key:field value:query
	 * @param directories
	 */
	public CommonSearcher(String searchName, Map<String, String> searchMap, List<Directory> directories) {
		super(COMMON_NAME + searchName);
		this.searchMap = searchMap;
		this.directories = directories;
	}
	
	public void Query() throws ParseException, IOException {
		
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< search directories size : " + directories.size());
		
		Query q = MultiFieldQueryParser.parse(searchMap.values().toArray(new String[0]), searchMap.keySet().toArray(new String[0]), analyzer);
		
		List<IndexReader> readers = new LinkedList<IndexReader>();
		for(Directory directory : directories) {
//			System.out.println("<<<<<<<<<<<<<<<<<<< search direcotry : " + Arrays.toString(directory.listAll()));
			try {
				IndexReader reader = DirectoryReader.open(directory);
				readers.add(reader);
			} catch (AlreadyClosedException | IndexNotFoundException e) {
//				e.printStackTrace();
			}
		}
		
		MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[0]));
		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
		IndexSearcher searcher = new IndexSearcher(multiReader);
		searcher.search(q, collector);

		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		System.out.println("<<<<<<<<<<<<<<<<<<< searcher is searching: " + searcherName + " | hits length " + hits.length);
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
		}
		
		multiReader.close();
	}
	
}