package com.sjzy.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

public class LoadObject {

	public LoadObject() {
		super();
	}
	public LoadObject(String threadName, Map<String, List<JSONObject>> jsonObjMap) {
		super();
		this.threadName = threadName;
		this.jsonObjMap = jsonObjMap;
	}
	private String threadName;
	private Map<String,List<JSONObject>> jsonObjMap;
	
	public String getThreadName() {
		return threadName;
	}
	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
	public Map<String, List<JSONObject>> getJsonObjMap() {
		return jsonObjMap;
	}
	public void setJsonObjMap(Map<String, List<JSONObject>> jsonObjMap) {
		this.jsonObjMap = jsonObjMap;
	}
	
	
	
}
