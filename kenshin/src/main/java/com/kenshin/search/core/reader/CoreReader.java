package com.kenshin.search.core.reader;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;

public class CoreReader extends AbstractReader {
	
	private static final Logger logger = Logger.getLogger(CoreReader.class);
	
	public CoreReader(ResourcePool resourcePool, ReaderManager readerManager) {
		super(resourcePool, readerManager, 1, 1);
		
		Executors.newSingleThreadExecutor().submit(new Runnable() {
			
			@Override
			public void run() {
				try {
					reOpenCoreReader();
				} catch(Exception e) {
					logger.info("<<<<<<<<<<<<<<<<< error reOpenCoreReader " + e.getMessage());
				}
			} 
		});
	}
	
	public void reOpenCoreReader() throws InterruptedException, IOException {
		while(true) {
			CoreDirectoryDetail directoryDetail = resourcePool.getCoreDirectoryDetail();
			if(directoryDetail == null) {
				Thread.sleep(60 * 1000);
				continue;
			}
			Directory directory = directoryDetail.getDirectory();
			DirectoryReader directoryReader = DirectoryReader.open(directory);
			
			Queue<SegDirectoryDetail> segDirectoryDetails = directoryDetail.getSegDirectoryDetails();
			
			//TODO需要加锁
			readerManager.setCoreReader(directoryReader);
			Map<Long, DirectoryReader> segDirectoryReaders =  readerManager.getSegDirectoryReaders();
			for(SegDirectoryDetail segDirectoryDetail : segDirectoryDetails) {
				segDirectoryReaders.remove(segDirectoryDetail.getId());
			}
			
			resourcePool.setCoreDirectoryDetail(null);
		}
	}

}
