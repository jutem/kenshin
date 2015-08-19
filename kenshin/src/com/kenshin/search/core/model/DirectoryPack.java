package com.kenshin.search.core.model;

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.store.Directory;

public class DirectoryPack {
	
	private List<Directory> ramDirectories; //ram索引
	private List<Directory> segDirectories; //碎片索引
	private Directory coreDirectory; //核心索引
	
	private List<Directory> allDirectory = new LinkedList<Directory>();
	
	public List<Directory> getRamDirectories() {
		return ramDirectories;
	}
	public void setRamDirectories(List<Directory> ramDirectories) {
		this.ramDirectories = ramDirectories;
		allDirectory.addAll(ramDirectories);
	}
	public List<Directory> getSegDirectories() {
		return segDirectories;
	}
	public void setSegDirectories(List<Directory> segDirectories) {
		this.segDirectories = segDirectories;
		allDirectory.addAll(segDirectories);
	}
	public Directory getCoreDirectory() {
		return coreDirectory;
	}
	public void setCoreDirectory(Directory coreDirectory) {
		this.coreDirectory = coreDirectory;
		allDirectory.add(coreDirectory);
	}
	
	public List<Directory> getAllDirectory() {
		return allDirectory;
	}
	
}
