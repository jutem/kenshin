package com.kenshin.search.core.bak;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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

import com.kenshin.search.core.index.RamIndexer;
import com.kenshin.search.core.reader.manager.ReaderManager;

/**
 * 主要负责组装query提交给manager去搜索
 * @author chendawei
 *
 */
public class CommonFilterReader extends AbstractSearcher {
	
	private ReaderManager searcherManager;
	
	private List<RamIndexer> indexers;
	private Map<String, String> searchMap;
	
	private static final String INDEX_SEARCHER = "indexer_search_";
	
	
	/**
	 * @param searchMap key:field value:query
	 * @param directories
	 */
	public CommonFilterReader(String searchName, Map<String, String> searchMap, List<RamIndexer> indexers, ReaderManager searcherManager) {
		super(INDEX_SEARCHER + searchName);
		this.searchMap = searchMap;
		this.indexers = indexers;
		this.searcherManager = searcherManager;
	}
	
	public void Query() throws ParseException, IOException {
		
		Query q = MultiFieldQueryParser.parse(searchMap.values().toArray(new String[0]), searchMap.keySet().toArray(new String[0]), analyzer);
		
		List<IndexReader> readers = new LinkedList<IndexReader>();
		for(RamIndexer indexer : indexers) {
//			System.out.println("<<<<<<<<<<<<<<<<<<< search direcotry : " + Arrays.toString(directory.listAll()));
			try {
				IndexReader ramReader = DirectoryReader.open(indexer.getReadDirectory());
				Queue<Directory> segsReader = indexer.getSegDirectories(); 
				
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
