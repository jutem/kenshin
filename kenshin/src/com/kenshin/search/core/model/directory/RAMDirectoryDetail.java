package com.kenshin.search.core.model.directory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.store.RAMDirectory;

import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.util.CommonUtil;

public class RAMDirectoryDetail {

	private long id = CommonUtil.getUniqueId(); //唯一标识这个direcotry属于哪个indexer
	private RAMDirectory directory = new RAMDirectory(); //需要重新打开的directory
	private Queue<String> modelIds = new ConcurrentLinkedQueue<String>();//这个directory中存在的所有modelId,需要保证每个indexer中的这个队列queue都不一样
	private boolean isReady = false; //是否准备好进入seg
	private Model model; // 需要过滤的model
	private String indexerName; //产生这个directory的indexerName
	
	public RAMDirectoryDetail() {
		super();
	}
	
	public RAMDirectoryDetail(String indexerName) {
		super();
		this.indexerName = indexerName;
	}
	
	public RAMDirectoryDetail(long id, RAMDirectory directory, boolean isReady,
			Model model, String indexerName) {
		super();
		this.id = id;
		this.directory = directory;
		this.isReady = isReady;
		this.model = model;
		this.indexerName = indexerName;
	}
	
	public void addModelIds(String modelId) {
		modelIds.add(modelId);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public RAMDirectory getDirectory() {
		return directory;
	}

	public void setDirectory(RAMDirectory directory) {
		this.directory = directory;
	}

	public Model getModel() {
		return model;
	}

	public void setModel(Model model) {
		this.model = model;
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public String getIndexerName() {
		return indexerName;
	}

	public void setIndexerName(String indexerName) {
		this.indexerName = indexerName;
	}

	public Queue<String> getModelIds() {
		return modelIds;
	}

	public void setModelIds(Queue<String> modelIds) {
		this.modelIds = modelIds;
	}
	
}
