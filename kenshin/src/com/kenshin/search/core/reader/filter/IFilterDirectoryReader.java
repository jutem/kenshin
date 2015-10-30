package com.kenshin.search.core.reader.filter;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

public class IFilterDirectoryReader extends
		FilterDirectoryReader {

	public IFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper)
			throws IOException {
		super(in, wrapper);
	}

	@Override
	protected DirectoryReader doWrapDirectoryReader(DirectoryReader in)
			throws IOException {
		return in;
	}
	
	public DirectoryReader doWrapDirectoryReader(DirectoryReader in, Collection<String> deleteQueue)
			throws IOException {
		DirectoryReader dr = openIfChanged(in);
		if (dr != null) {
			dr = new IFilterDirectoryReader(dr,
				new ISubReaderWrapper(deleteQueue));
		}
		return dr;
	}

	private static class ISubReaderWrapper extends SubReaderWrapper {
		private Collection<String> deleteQueue;
		
		public ISubReaderWrapper(Collection<String> deleteQueue) {
			this.deleteQueue = deleteQueue;
		}
		
		@Override
		public LeafReader wrap(LeafReader reader) {
			return new IFilterLeafReader(reader, deleteQueue);
		}

	}

}
