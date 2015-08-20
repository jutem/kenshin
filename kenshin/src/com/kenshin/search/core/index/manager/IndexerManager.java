package com.kenshin.search.core.index.manager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.kenshin.search.core.index.Indexer;
import com.kenshin.search.core.model.DirectoryPack;

public class IndexerManager {
	
	private static final int MAX_PERINDEX_SEG = 5000;//最多处理每个indexer中seg的个数
	
	//分词器
	private final Analyzer analyzer;
	private final String indexPath;
	private final Directory indexDirectory;
	
	private final List<Indexer> indexers = new LinkedList<Indexer>();
	private final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor(); //索引所有seg到总索引中
	
	public IndexerManager(Analyzer analyzer, String indexPath) throws IOException {
		super();
		this.analyzer = analyzer;
		this.indexPath = indexPath;
		this.indexDirectory = FSDirectory.open(Paths.get(indexPath));
		
		pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					indexAll();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, 1, 30, TimeUnit.SECONDS);
	}
	
	//将碎片文件索引到总文件里
	private void indexAll() throws IOException {
		
//		Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter w = new IndexWriter(indexDirectory, config);
		
		for(Indexer indexer : indexers) {
			List<Directory> segDirectories = new LinkedList<Directory>();
			Queue<Directory> segs = indexer.getSegDirectories();
			//一直取到这部分seg取完，之后再新增的不管
			//TODO 如果一个indexer生产seg的能力超过manager处理seg的能力将会导致死循环
			for(int i = 0 ; i < MAX_PERINDEX_SEG; i++) {
				Directory directory = segs.poll();
				if(directory == null)
					break;
				segDirectories.add(directory);
			}
			w.addIndexes(segDirectories.toArray(new Directory[0]));
			w.commit();
			
			indexer.clearSegs(segDirectories);
		}
		
		w.close();
	}
	
	//indexer注册
	public void registerIndexer(Indexer indexer) {
		indexers.add(indexer);
	}
	
	public List<Indexer> getIndexers() {
		return indexers;
	}
	
	public DirectoryPack getAllDirectory() {
		
		List<Directory> ramDirectories = getAllRamDirectory();
		List<Directory> segDirectories = getAllSegSnapshot();
		Directory coreDirectory = indexDirectory;
		
		DirectoryPack directoryPack = new DirectoryPack();
		directoryPack.setCoreDirectory(coreDirectory);
		directoryPack.setRamDirectories(ramDirectories);
		directoryPack.setSegDirectories(segDirectories);
		
		return directoryPack;
	}
	
	/**
	 * 返回当前所有ramDirectory 
	 * TODO 当ram close注意线程安全问题
	 */
	public List<Directory> getAllRamDirectory() {
		List<Directory> ramDirectories = new LinkedList<Directory>();
		for(Indexer indexer : indexers) {
			ramDirectories.add(indexer.getDirectory());
		}
		return ramDirectories;
	}
	
	/**
	 * 返回当前所有seg快照
	 * TODO 不返回快照的话,这个segList会无限增长，当查询openReder的能力比seg增长的能力弱，会进入死循环
	 * TODO 返回快照的时候防止seg已经被清理
	 */
	public List<Directory> getAllSegSnapshot() {
		List<Directory> segSnapshots = new LinkedList<Directory>();
		for(Indexer indexer : indexers) {
			Queue<Directory> q = indexer.getSegDirectories();
			//addAll是将q toArray，所以并非原先q的引用
			segSnapshots.addAll(q);
		}
		return segSnapshots;
	}
	
//	public static void main(String[] args) {
//		List<Integer> l = new LinkedList<Integer>();
//		l.add(1);
//		Object[] a = l.toArray();
//		System.out.println(Arrays.toString(a));
//		l.add(2);
//		System.out.println(Arrays.toString(a));
//		
//	}
	
//	public static void main(String[] args) throws IOException {
//		Path path = Paths.get("D:/indexs/seg/");
////		Iterable<Path> files = path.getFileSystem().getRootDirectories();
//		
//		DirectoryStream<Path> paths = Files.newDirectoryStream(path); 
////		System.out.println("path : " + path.toUri());
//		
//		for(Path file : paths) {
//			System.out.println(file.toUri());
//			System.out.println(file.getParent() + " | " + file.getFileName());
//		}
//	}
	
//	Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
//	IndexWriterConfig config = new IndexWriterConfig(analyzer);
//	IndexWriter w = new IndexWriter(indexDirectory, config);
//	
//	List<Directory> segDirectorys = new LinkedList<Directory>();
//	
//	for(Indexer indexer : indexers) {
//		Path segIndexerPath = Paths.get(segPath + indexer.getIndexName());
//		if(!Files.exists(segIndexerPath, LinkOption.NOFOLLOW_LINKS)) {
//			continue;
//		}
//		DirectoryStream<Path> paths = Files.newDirectoryStream(segIndexerPath); 
//		//遍历一个indexer产生的所有fsd
//		//TODO 其中可能会有未写完的数据,那就可能会报错，记录这些数据
//		for(Path path : paths) {
//			Directory segDirectory = FSDirectory.open(path);
//			segDirectorys.add(segDirectory);
//		}
//	}
	
}
