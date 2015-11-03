package com.kenshin.search.core.reader.reader;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;

public class CommonSearcher{
	
	private final Query q;
	private final Collection<DirectoryReader> readers;
	private final int hitsPerPage;

	public CommonSearcher(Query q, Collection<DirectoryReader> readers,
			int hitsPerPage) {
		super();
		this.q = q;
		this.readers = readers;
		this.hitsPerPage = hitsPerPage;
	}
	
	public List<Document> query() {
		List<Document> result = new LinkedList<Document>();
		try {
			MultiReader multiReader = new MultiReader(readers.toArray(new DirectoryReader[0]));
			TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
			IndexSearcher searcher = new IndexSearcher(multiReader);
			searcher.search(q, collector);

			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				result.add(d);
			}
			multiReader.close(); //TODO
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

}
