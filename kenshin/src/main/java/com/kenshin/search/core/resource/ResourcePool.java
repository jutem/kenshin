package com.kenshin.search.core.resource;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.reader.manager.ReaderManager;

/**
 * 资源池
 * ram的同一个directory是有可能同时出现updateData和ramData中
 */
@Component
public class ResourcePool {
	
	private static final Logger logger = Logger.getLogger(ResourcePool.class);
	
	//数据源
	private LinkedBlockingQueue<Model> originData = new LinkedBlockingQueue<Model>(); //原始数据 
	
	private LinkedBlockingQueue<RAMDirectoryDetail> updateData = new LinkedBlockingQueue<RAMDirectoryDetail>(); //更新的数据，需要通知过滤
	
	private Map<Long, RAMDirectoryDetail> readyForSeg = new ConcurrentHashMap<Long, RAMDirectoryDetail>(); //准备写入到seg的数据 key:directoryId, value:DirectoryDetail
	private LinkedBlockingQueue<RAMDirectoryDetail> toSeg = new LinkedBlockingQueue<RAMDirectoryDetail>(); //准备好写入seg
	
	private LinkedBlockingQueue<SegDirectoryDetail> readyForCore = new LinkedBlockingQueue<SegDirectoryDetail>(); //准备合并到core的数据
	private LinkedBlockingQueue<SegDirectoryDetail> toCore = new LinkedBlockingQueue<SegDirectoryDetail>(); //准备好写入core
	
	private CoreDirectoryDetail coreDirectoryDetail = null; 
	
	public ResourcePool() {
		super();
	}
	
	/********************************** push **************************************/
	
	/**
	 * 推送数据到工作队列
	 */
	public void pushOriginData(Model model) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< model in origin : " + model.getId());
		originData.add(model);
	}
	
	/**
	 * indexer推送需要写到seg中的ramDirectory
	 */
	public void pushReadyForSeg(RAMDirectoryDetail directory) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in ready for seg : " + directory.getId());
		readyForSeg.put(directory.getId(), directory);
	}
	
	/**
	 * indexer推送更新的ramDirectory
	 */
	public void pushUpdateRam(RAMDirectoryDetail directoryDetail) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in update ram : " + directoryDetail.getId());
		updateData.add(directoryDetail);
	}
	
	/**
	 * segIndexer推送segDirectory
	 */
	public void pushReadyForCore(SegDirectoryDetail directoryDetail) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in ready for core : " + directoryDetail.getId());
		readyForCore.add(directoryDetail);
	}
	
	/**
	 * seg已经准备就绪
	 */
	public void toCore(SegDirectoryDetail segDirectoryDetail) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in to core : " + segDirectoryDetail.getId());
		toCore.add(segDirectoryDetail);
	}
	
	/**
	 * 替换core
	 */
	public void setCoreDirectoryDetail(CoreDirectoryDetail coreDirectoryDetail) {
		if(coreDirectoryDetail != null) {
			logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in coreDetail : " + coreDirectoryDetail.getId());
		}
		this.coreDirectoryDetail = coreDirectoryDetail;
	}

	/************************************ take *********************************************/
	
	public Model takeOriginData() {
		try {
			return originData.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public RAMDirectoryDetail takeUpdateDirectoryDetail() {
		try {
			return updateData.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public SegDirectoryDetail takeReadyForCore() {
		try {
			return readyForCore.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public RAMDirectoryDetail takeToSeg() {
		try {
			return toSeg.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public SegDirectoryDetail takeToCore() {
		try {
			return toCore.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public CoreDirectoryDetail getCoreDirectoryDetail() {
		return coreDirectoryDetail;
	}
	
	public Queue<SegDirectoryDetail> getAllSeg() {
		Queue<SegDirectoryDetail> tmpQueue = toCore;
		toCore = new LinkedBlockingQueue<SegDirectoryDetail>();
		return tmpQueue;
	}
	
	/********************************** reader回调 ***************************************/
	
	/**
	 * 解锁这块directory可以进入seg
	 */
	public void unLockDirectoryDetail(long directoryId) {
		RAMDirectoryDetail ramDirectoryDetail = readyForSeg.remove(directoryId);
		if(ramDirectoryDetail != null) {
			logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in to seg : " + directoryId);
			ramDirectoryDetail.setReady(true);
			toSeg.add(ramDirectoryDetail);
		}
	}

	public LinkedBlockingQueue<Model> getOriginData() {
		return originData;
	}
	
}
