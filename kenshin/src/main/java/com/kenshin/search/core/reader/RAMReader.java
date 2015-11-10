package com.kenshin.search.core.reader;

import java.util.Queue;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.disruptor.event.RAMDirectoryEvent;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;
import com.lmax.disruptor.EventHandler;

public class RAMReader extends AbstractReader implements EventHandler<RAMDirectoryEvent>{
	
	private static final Logger logger = Logger.getLogger(RAMReader.class);
	
	public RAMReader(ResourcePool resourcePool, ReaderManager readerManager, long ordinal, long numberOfConsumers) {
		super(resourcePool, readerManager, ordinal, numberOfConsumers);
	}

	@Override
	public void onEvent(RAMDirectoryEvent event, long sequence, boolean onEndOfBatch) throws Exception {
		if ((sequence % numberOfConsumers) == ordinal) {
			RAMDirectoryDetail directoryDetail = event.getDirectory();
			Directory directory = directoryDetail.getDirectory();
			DirectoryReader directoryReader = DirectoryReader.open(directory);
			
			Queue<String> modelIds = directoryDetail.getModelIds();
			
			//TODO需要加锁
			readerManager.getRamDirectoryReaders().put(directoryDetail.getIndexerName(), directoryReader);
			for(String modelId : modelIds) {
				readerManager.getRamModelIdMap().put(modelId, true);
			}
			
			//通知已经可以解锁这个seg了
			logger.debug("<<<<<<<<<<<<<<<<<<<<<< RAMReader directoryId: " + directoryDetail.getId());
			resourcePool.unLockDirectoryDetail(directoryDetail.getId());
		}
	}
}
