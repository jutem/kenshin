package com.kenshin.search.core.model.directory;

import java.util.Queue;

import org.apache.lucene.store.Directory;

import com.kenshin.search.core.util.CommonUtil;

public class SegDirectoryDetail {

	private long id = CommonUtil.getUniqueId(); //唯一标识这个direcotry属于哪个indexer
	private boolean isReady = false; //是否准备好进入seg
	private Directory directory; //需要重新打开的directory
	private String indexerName; //产生这个directory的indexerName
	private String ramIndexerName; //之前的ramIndexerName
	private Queue<String> modelIds; //本seg存放的modeIds
	
	public SegDirectoryDetail() {
		super();
	}
	
	public SegDirectoryDetail(String indexerName, String ramIndexerName, Directory directory, Queue<String> modelIds) {
		super();
		this.indexerName = indexerName;
		this.ramIndexerName = ramIndexerName;
		this.directory = directory;
		this.modelIds = modelIds;
	}

	public SegDirectoryDetail(long id, boolean isReady, Directory directory,
			String indexerName, String ramIndexerName, Queue<String> modelIds) {
		super();
		this.id = id;
		this.isReady = isReady;
		this.directory = directory;
		this.indexerName = indexerName;
		this.ramIndexerName = ramIndexerName;
		this.modelIds = modelIds;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public Directory getDirectory() {
		return directory;
	}

	public void setDirectory(Directory directory) {
		this.directory = directory;
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

	public String getRamIndexerName() {
		return ramIndexerName;
	}

	public void setRamIndexerName(String ramIndexerName) {
		this.ramIndexerName = ramIndexerName;
	}

	public Queue<String> getModelIds() {
		return modelIds;
	}

	public void setModelIds(Queue<String> modelIds) {
		this.modelIds = modelIds;
	}
	
}
