package com.kenshin.search.core.reader.filter;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import com.kenshin.search.core.model.Model;

public class IFilterLeafReader extends FilterLeafReader {
	
	private Collection<String> deleteQueue;
	private int cardinality;

	public IFilterLeafReader(LeafReader in, Collection<String> deleteQueue) {
		super(in);
		this.deleteQueue = deleteQueue;
	}

	@Override
	public Bits getLiveDocs() {
		ensureOpen();
		FixedBitSet bits = (FixedBitSet) super.getLiveDocs();
		for(String id : deleteQueue) {
			PostingsEnum pe;
			try {
				pe = in.postings(new Term("id", id));
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
