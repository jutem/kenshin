package com.kenshin.search.core.reader.filter;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

public class IFilterLeafReader extends FilterLeafReader {
	
	private Map<String, Boolean> deleteMap;
	private int cardinality;

	public IFilterLeafReader(LeafReader in, Map<String, Boolean> deleteMap) {
		super(in);
		this.deleteMap = deleteMap;
	}

	@Override
	public Bits getLiveDocs() {
		ensureOpen();
		FixedBitSet bits = (FixedBitSet) super.getLiveDocs();
		System.out.println("<<<<<<<<<<<<<<<<< bits : " + bits);
		for(Map.Entry<String, Boolean> entry : deleteMap.entrySet()) {
			PostingsEnum pe;
			try {
				pe = in.postings(new Term("id", entry.getKey()));
				if (pe != null && bits != null) {
					bits.set(pe.nextDoc());
					cardinality ++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return bits;
	}

	@Override
	public int numDocs() {
		return in.numDocs() - cardinality;
	}

}
