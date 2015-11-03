package com.kenshin.search.core.reader.filter;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

public class IFilterDirectoryReader extends
		FilterDirectoryReader {

	public IFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper)
			throws IOException {
		super(in, wrapper);
	}

	/**
	 * TODO 执行业务逻辑
	 */
	@Override
	public DirectoryReader doWrapDirectoryReader(DirectoryReader in)
			throws IOException {
		return in;
	}
	
//	public static DirectoryReader doWrapDirectoryReader(DirectoryReader in, Map<String, Boolean> deleteMap)
//			throws IOException {
////		DirectoryReader dr = openIfChanged(in);
//		if (in != null) {
//			in = new IFilterDirectoryReader(in, new ISubReaderWrapper(deleteMap, in));
//		}
//		return in;
//	}

	public static class ISubReaderWrapper extends SubReaderWrapper {
		private Map<String, Boolean> deleteMap;
		
		public ISubReaderWrapper(Map<String, Boolean> deleteMap) {
			this.deleteMap = deleteMap;
		}
		
		@Override
		public LeafReader wrap(LeafReader reader) {
			return new IFilterLeafReader(reader, deleteMap);
		}

	}

}
