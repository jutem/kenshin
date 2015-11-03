package com.kenshin.search.core.reader.query;

import java.util.Map;

public class CommonQuery {
	private Map<String, String> queryMap; //key:queries value:fields
	private String[] queries;
	private String[] fields;
	
	public CommonQuery(Map<String, String> queryMap) {
		super();
		this.queryMap = queryMap;
		this.queries = queryMap.keySet().toArray(new String[0]);
		this.fields = queryMap.values().toArray(new String[0]);
	}
	
	public Map<String, String> getQueryMap() {
		return queryMap;
	}
	public void setQueryMap(Map<String, String> queryMap) {
		this.queryMap = queryMap;
		this.queries = queryMap.keySet().toArray(new String[0]);
		this.fields = queryMap.values().toArray(new String[0]);
	}
	
	public String[] getQueries() {
		return queries;
	}
	public String[] getFields() {
		return fields;
	}

	
	
}
