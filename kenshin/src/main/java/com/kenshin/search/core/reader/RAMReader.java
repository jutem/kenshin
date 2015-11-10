package com.kenshin.search.core.reader;

import java.util.Queue;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.disruptor.event.RAMDirectoryEvent;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.DisruptorResourcePool;
import com.lmax.disruptor.EventHandler;

public class RAMReader extends AbstractReader implements EventHandler<RAMDirectoryEvent>{
	
	public RAMReader(DisruptorResourcePool resourcePool, ReaderManager readerManager) {
		super(resourcePool, readerManager);
	}

	@Override
	public void onEvent(RAMDirectoryEvent event, long paramLong, boolean paramBoolean) throws Exception {
//		RAMDirectoryDetail directoryDetail = resourcePool.takeUpdateDirectoryDetail();
		RAMDirectoryDetail directoryDetail = event.getDirectory();
		Directory directory = directoryDetail.getDirectory();
		DirectoryReader directoryReader = DirectoryReader.open(directory);
		
		Queue<String> modelIds = directoryDetail.getModelIds();
//			String modelId = directoryDetail.getModel().getId();
		
		//TODO需要加锁
		readerManager.getRamDirectoryReaders().put(directoryDetail.getIndexerName(), directoryReader);
		for(String modelId : modelIds) {
			readerManager.getRamModelIdMap().put(modelId, true);
		}
		
		//通知已经可以解锁这个seg了
		resourcePool.unLockDirectoryDetail(directoryDetail.getId());
	}
}
