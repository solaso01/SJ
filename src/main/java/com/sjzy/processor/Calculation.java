package com.sjzy.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.sjzy.common.CommonConstants;
import com.sjzy.conf.ESConf;
import com.sjzy.utils.CurrentThreadUtil;
import com.sjzy.utils.ESUtils;
import com.sjzy.utils.IParallelThread;
import com.sjzy.utils.LoadObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * Created by wpp on 2020/7/21.
 *
 */
public class Calculation implements Serializable {

    private static final long serialVersionUID = 1L;
	public static final Logger logger = LoggerFactory.getLogger(Calculation.class);
	public static Map<String, String> mapTablePK = null;

	/**
	 * 将单个信息加入所有数据的存储变量中，并返回所有数据的变量
	 *
	 * @param allMapInfo	存放改时间段的所有数据信息
	 * @param singleStr		单个信息
	 * @return				将allMapInfo返回
	 */
	public static Map<String,List<JSONObject>> getMapForTableName(Map<String,List<JSONObject>> allMapInfo,String singleStr) {

		if (!StringUtils.isEmpty(singleStr)) {

	    	try {
	    		//拿到数据中的json信息
	    		String [] singleJson = singleStr.split("!,!");

	    		//主要是要singleJson[0]，并解析
				for(String singleInfo : singleJson) {

					if(!org.apache.commons.lang.StringUtils.isEmpty(singleInfo)&&!singleInfo.equals("")) {

						//将string（json）格式的数据解析成JSONObject
						JSONObject singleInfoJsonObject = JSON.parseObject(singleInfo);

						//获取singleStr中的表名，查看allMapInfo中是否有该表的数据
						String tableName = singleInfoJsonObject.getString("table");

						//如果有数据，则。。。如果没有，则。。。
						if (allMapInfo.get(tableName)==null) {

							List<JSONObject> tableLists = new ArrayList<JSONObject>();
							tableLists.add(singleInfoJsonObject);
							allMapInfo.put(tableName,tableLists);
						} else {

							List<JSONObject> tableLists = allMapInfo.get(tableName);
							tableLists.add(singleInfoJsonObject);
							allMapInfo.put(tableName,tableLists);
						}
					}
				}
	    	}catch (Exception e){

	    		e.printStackTrace();
	    	}
	   }

		return allMapInfo;
	}


	/**
	 * 将要存入的信息存入es中
	 *
	 * @param resMap			所有要存入es源表的json信息
	 * @param mapTablePk		mysql中存的映射表信息
	 */
	public static void nomultiThreadHandle(Map<String,List<JSONObject>> resMap, Map<String, String> mapTablePk) throws IOException {

		mapTablePK = mapTablePk;

		//线程配置
		int i = 1;
		Map<String,List<JSONObject>> jsonObjectMap1 = new HashMap<String,List<JSONObject>>();
		Map<String,List<JSONObject>> jsonObjectMap2 = new HashMap<String,List<JSONObject>>();
		Map<String,List<JSONObject>> jsonObjectMap3 = new HashMap<String,List<JSONObject>>();

		//将数据平均分配到三个线程中
		for (Map.Entry<String,List<JSONObject>> entry : resMap.entrySet()) {
			if(i%3 == 1) {
				jsonObjectMap1.put(entry.getKey(),entry.getValue());
			}else if(i%3 == 2) {
				jsonObjectMap2.put(entry.getKey(),entry.getValue());
			}else{
				jsonObjectMap3.put(entry.getKey(),entry.getValue());
			}
			i++;
		}
		List<LoadObject> loadObjects = new LinkedList<LoadObject>();
		LoadObject loadObject1 = new LoadObject("Thread1",jsonObjectMap1);
		LoadObject loadObject2 = new LoadObject("Thread2",jsonObjectMap2);
		LoadObject loadObject3 = new LoadObject("Thread3",jsonObjectMap3);
		loadObjects.add(loadObject1);
		loadObjects.add(loadObject2);
		loadObjects.add(loadObject3);

		//三个线程并行执行保存es数据库
		CurrentThreadUtil.parallelJob(new SJZYThread(), new LinkedBlockingDeque<Object>(loadObjects), 3);

	}


	/**
	 * 单个线程的处理数据逻辑（关键类）
	 */
	private static class SJZYThread implements IParallelThread {

		@Override
		public Boolean doMyJob(Object obj) throws Exception {
			//拿到所有的数据信息
			Map<String,List<JSONObject>> allMapInfo = ((LoadObject) obj).getJsonObjMap();

			//判断是否数据信息为空
			if (allMapInfo != null && allMapInfo.size() > 0){

				//创建es Client
				RestHighLevelClient esClient = ESConf.getESClient(CommonConstants.ES_HOST_NAME, CommonConstants.ES_PORT);
				//初始化BulkRequest
				BulkRequest bulkRequest = ESConf.initBulkRequest();

				try {
					//遍历每个table
					for (Map.Entry<String,List<JSONObject>> entry : allMapInfo.entrySet()) {

						if (entry.getKey()!=null) {

							//得到json信息中的表名
							String tableName = entry.getKey().toLowerCase();

							//查看表名是否在映射表中存在
							if (mapTablePK.containsKey(tableName)) {

								int num = 1;

								for (JSONObject entryJsonObject : entry.getValue()) {

									//某表的所有信息存入bulkRequest中
									getSaveESRequest(entryJsonObject,bulkRequest,tableName,mapTablePK);

									//一条一条的存入，每 CommonConstants.ES_SINGLE_FLUSH_NUM 条flush一次
									if (num % CommonConstants.ES_SINGLE_FLUSH_NUM == 0) {
										esClient.bulk(bulkRequest, RequestOptions.DEFAULT);//批量插入
										//清空bulk重新批量添加
										bulkRequest = ESConf.initBulkRequest();
									}
									num++;
								}

								//剩下的再flush一次
								if (num > 1) {
									esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
								}
							}
						}
					}

				}catch (Exception e){
					logger.error("SJZYThread出错了！");

				} finally {

					esClient.close();
				}
			}

			return true;
		}
	}


	/**
	 * 将该表的所有信息存入bulkRequest中
	 *
	 * @param entryJsonObject	要操作的单个信息
	 * @param bulkRequest		BulkRequest对象
	 * @param tableName			表名
	 * @param mapTablePk		mysql存储的映射表信息
	 */
	public static void  getSaveESRequest(JSONObject entryJsonObject, BulkRequest bulkRequest, String tableName, Map<String, String> mapTablePk) {
		Map<String, Object> beforeDataMap = null;
		String beforeData = null;
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
			String afterData = JSONObject.toJSONString(entryJsonObject.get("after"), SerializerFeature.WriteMapNullValue);
			beforeData = JSONObject.toJSONString(entryJsonObject.get("before"),SerializerFeature.WriteMapNullValue);
			String opType = entryJsonObject.getString("op_type");
			String currentTs = (entryJsonObject.getString("current_ts") == null ? "" : entryJsonObject.getString("current_ts")).split("\\.")[0].replace("T", " ");
			Map<String, Object> afterDataMap = null;
			//I插入 U更新 D删除
			if (opType.equals("I") ) {
				if (afterData != null) {
					afterDataMap = JSON.parseObject(afterData);
					afterDataMap.put("ogg_ts", currentTs);
					afterDataMap.put("es_ts",df.format(new Date()));
					ESUtils.saveEntity(afterDataMap,bulkRequest,tableName,mapTablePk);
				}
			}else if(opType.equals("U")){
				if (afterData != null) {
					afterDataMap = JSON.parseObject(afterData);
					afterDataMap.put("ogg_ts", currentTs);
					afterDataMap.put("es_ts",df.format(new Date()));
					ESUtils.updateEntity(afterDataMap,bulkRequest,tableName,mapTablePk);
				}
			}
			else if (opType.equals("D")) {
				if (beforeData != null) {
					beforeDataMap = JSON.parseObject(beforeData);
					beforeDataMap.put("ogg_ts", currentTs);
					beforeDataMap.put("es_ts",df.format(new Date()));
					ESUtils.deleteByKeys(beforeDataMap,bulkRequest,tableName,mapTablePk);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("getSaveESRequest出错了！");
		}
	}

	/**
	 * 因为es的字段全部为string类型，所以将object强转为String
	 * @param mapFields	所有字段信息
	 * @return			强转过后的信息
	 */
	private static Map<String, String> getStringMap(Map<String, Object> mapFields) {

		Map<String, String> stringMap = new HashMap<>();
		for (String fieldName : mapFields.keySet()){
			String date = mapFields.get(fieldName) == null ? "" : mapFields.get(fieldName).toString();
			stringMap.put(fieldName.toLowerCase(),date);
		}
		return stringMap;

	}


}




