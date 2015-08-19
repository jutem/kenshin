package com.kenshin.search.core.index;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class CoreIndexer {
	
//	private static final int MAX_PERINDEX_SEG = 5000;//最多处理每个indexer中seg的个数
//	
//	//分词器
//	private final Analyzer analyzer;
//	private final String indexPath;
//	
//	//将碎片文件索引到总文件里
//	private void indexAll() throws IOException {
//		
////			Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
////			IndexWriterConfig config = new IndexWriterConfig(analyzer);
////			IndexWriter w = new IndexWriter(indexDirectory, config);
////			
////			List<Directory> segDirectorys = new LinkedList<Directory>();
////			
////			for(Indexer indexer : indexers) {
////				Path segIndexerPath = Paths.get(segPath + indexer.getIndexName());
////				if(!Files.exists(segIndexerPath, LinkOption.NOFOLLOW_LINKS)) {
////					continue;
////				}
////				DirectoryStream<Path> paths = Files.newDirectoryStream(segIndexerPath); 
////				//遍历一个indexer产生的所有fsd
////				//TODO 其中可能会有未写完的数据,那就可能会报错，记录这些数据
////				for(Path path : paths) {
////					Directory segDirectory = FSDirectory.open(path);
////					segDirectorys.add(segDirectory);
////				}
////			}
//		
//		Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
//		IndexWriterConfig config = new IndexWriterConfig(analyzer);
//		IndexWriter w = new IndexWriter(indexDirectory, config);
//		
//		List<Directory> segDirectories = new LinkedList<Directory>();
//		for(Indexer indexer : indexers) {
//			Queue<Directory> segs = indexer.getSegDirectories();
//			//一直取到这部分seg取完，之后再新增的不管
//			//TODO 如果一个indexer生产seg的能力超过manager处理seg的能力将会导致死循环
//			for(int i = 0 ; i < MAX_PERINDEX_SEG; i++) {
//				Directory directory = segs.poll();
//				if(directory == null)
//					break;
//				segDirectories.add(directory);
//			}
//		}
//		
//		w.addIndexes(segDirectories.toArray(new Directory[0]));
//		w.close();
//	}
}
