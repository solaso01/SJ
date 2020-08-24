package com.sjzy.utils;

import com.alibaba.druid.support.json.JSONUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;



/**
 * Created by wpp on 2020/7/21.
 *
 */
public class ESUtils {


    public static void updateEntity(Map<String, Object> afterDataMap, BulkRequest bulkRequest, String tableName, Map<String, String> mapTablePk) {
        String[] tableFields = mapTablePk.get(tableName).split(",");
        String id = "";
        tableName = "test_msicprpcplan";



        for ( String singleField : tableFields ){
            id = id+(afterDataMap.get(singleField.toUpperCase())==null?"":afterDataMap.get(singleField.toUpperCase()));
        }
        //新建个map把旧map的key变小写
        Map<String, Object> afterDataMapLowerCase = new HashMap<>();
        for(String key: afterDataMap.keySet()){
            afterDataMapLowerCase.put(key.toLowerCase(),afterDataMap.get(key));
        }

//        String json = JSONUtils.toJSONString(afterDataMap);
        String json = JSONUtils.toJSONString(afterDataMapLowerCase);

        UpdateRequest updateRequest = new UpdateRequest(tableName.replace(".","_"),id);
        IndexRequest indexRequest = new IndexRequest(tableName.replace(".", "_")).id(id);
        indexRequest.source(json, XContentType.JSON);
        updateRequest.doc(indexRequest);

        bulkRequest.add(updateRequest);

    }

    /**
     * 插入，更新行信息
     *
     * @param afterDataMap  插入删除信息
     * @param bulkRequest   将信息存入到BulkRequest
     * @param tableName     表名
     * @param mapTablePk    映射字段，主要是拿表主键
     */
    public static void saveEntity(Map<String, Object> afterDataMap, BulkRequest bulkRequest, String tableName, Map<String, String> mapTablePk) {
        String[] tableFields = mapTablePk.get(tableName).split(",");
        String id = "";
        tableName = "test_msicprpcplan";



        for ( String singleField : tableFields ){
            id = id+(afterDataMap.get(singleField.toUpperCase())==null?"":afterDataMap.get(singleField.toUpperCase()));
        }
        //新建个map把旧map的key变小写
        Map<String, Object> afterDataMapLowerCase = new HashMap<>();
        for(String key: afterDataMap.keySet()){
            afterDataMapLowerCase.put(key.toLowerCase(),afterDataMap.get(key));
        }

//        String json = JSONUtils.toJSONString(afterDataMap);
        String json = JSONUtils.toJSONString(afterDataMapLowerCase);
        IndexRequest insertRequest = new IndexRequest(tableName.replace(".", "_")).id(id);
        insertRequest.source(json, XContentType.JSON);
        bulkRequest.add(insertRequest);

    }

    /**
     * 删除行信息
     *
     * @param beforeDataMap 删除信息
     * @param bulkRequest   将信息存入到BulkRequest
     * @param tableName     表名
     * @param mapTablePk    映射字段，主要是拿表主键
     */
    public static void deleteByKeys(Map<String, Object> beforeDataMap, BulkRequest bulkRequest, String tableName, Map<String, String> mapTablePk) {

        String[] tableFields = mapTablePk.get(tableName).split(",");
        tableName = "test_msicprpcplan";
        String id = "";
        for ( String singleField : tableFields ){
            id = id+(beforeDataMap.get(singleField.toUpperCase())==null?"":beforeDataMap.get(singleField.toUpperCase()));
        }
        DeleteRequest deleteRequest = new DeleteRequest(tableName.replace(".", "_")).id(id);
        bulkRequest.add(deleteRequest);
    }
}
