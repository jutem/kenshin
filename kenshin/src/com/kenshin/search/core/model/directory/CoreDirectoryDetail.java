package com.kenshin.search.core.model.directory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.lucene.store.Directory;

import com.kenshin.search.core.util.CommonUtil;

public class CoreDirectoryDetail {

	private long id = CommonUtil.getUniqueId(); //唯一标识这个direcotry属于哪个indexer
	private Directory directory; //需要重新打开的directory
	private String indexerName; //产生这个directory的indexerName
	private Queue<SegDirectoryDetail> segDirectoryDetails = new LinkedList<SegDirectoryDetail>(); //需要处理的directory
	
	public CoreDirectoryDetail() {
		super();
	}
	
	public CoreDirectoryDetail(String indexerName, Directory directory, Queue<SegDirectoryDetail> segDirectoryDetails) {
		super();
		this.indexerName = indexerName;
		this.directory = directory;
		this.segDirectoryDetails = segDirectoryDetails;
	}

	public CoreDirectoryDetail(long id, boolean isReady, Directory directory,
			String indexerName, String ramIndexerName, Queue<String> modelIds) {
		super();
		this.id = id;
		this.directory = directory;
		this.indexerName = indexerName;
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

	public String getIndexerName() {
		return indexerName;
	}

	public void setIndexerName(String indexerName) {
		this.indexerName = indexerName;
	}

	public Queue<SegDirectoryDetail> getSegDirectoryDetails() {
		return segDirectoryDetails;
	}

	public void setSegDirectoryDetails(Queue<SegDirectoryDetail> segDirectoryDetails) {
		this.segDirectoryDetails = segDirectoryDetails;
	}

}
