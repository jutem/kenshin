package com.kenshin.search.core.reader;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.disruptor.event.SegDirectoryEvent;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;
import com.lmax.disruptor.EventHandler;

public class SegReader extends AbstractReader implements EventHandler<SegDirectoryEvent>{
	
	private static final Logger logger = Logger.getLogger(SegReader.class);
	
	public SegReader(ResourcePool resourcePool, ReaderManager readerManager, long ordinal, long numberOfConsumers) {
		super(resourcePool, readerManager, ordinal, numberOfConsumers);
	}

	@Override
	public void onEvent(SegDirectoryEvent event, long sequence, boolean onEndOfBatch) throws Exception {
		if ((sequence % numberOfConsumers) == ordinal) {
			logger.debug("<<<<<<<<<<<<<<<<<<<<<<<<<<< SegReader start");
			
			SegDirectoryDetail directoryDetail = event.getDirectory();
			Directory directory = directoryDetail.getDirectory();
			DirectoryReader directoryReader = DirectoryReader.open(directory);
			
			//TODO需要加锁
			readerManager.getRamDirectoryReaders().remove(directoryDetail.getRamIndexerName()); //可以清理这个directory了
			readerManager.getSegDirectoryReaders().put(directoryDetail.getId(), directoryReader);
			
			/**
			 * TODO
			 * 解决当新的更新文档进来，但是这里却把他删除了的问题
			 */
			for(String modelId : directoryDetail.getModelIds()) {
				readerManager.getRamModelIdMap().remove(modelId);
				readerManager.getSegModelIdMap().put(modelId, true);
			}
			
			//通知这个directory已经可以进入seg
			resourcePool.getToCore().add(directoryDetail);
		}
	}
}
