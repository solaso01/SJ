package com.msig.application;

import com.alibaba.druid.support.json.JSONUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import static com.sjzy.utils.MysqlUtil.getTablePK;


public class kuduToEs {

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

        Map<String,String> list = getTablePK("kafka.es_tb_mapping");

        String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
        String CONNECTION_URL = "jdbc:impala://192.1.6.73:21050/msic";
        Connection con = null;
        PreparedStatement ps = null;
        Class.forName(JDBC_DRIVER);
        con = DriverManager.getConnection(CONNECTION_URL);
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.1.6.74", 9200, "http")));

        for ( Map.Entry<String,String> single: list.entrySet() ){
            String tableName = single.getKey();
            ps = con.prepareStatement("select * from "+tableName);
            System.out.println(tableName+"开始");
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();// 获得结果集结构信息（元数据）
            int columnCount = md.getColumnCount();// ResultSet列数

            int count = 0;
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulkRequest.waitForActiveShards(ActiveShardCount.ALL);

            while (rs.next()) {
                Map<Object, Object> mapEs = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    mapEs.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));//字段名转小写
                }
                mapEs.put("ogg_ts","");
                mapEs.put("es_ts","");
                String[] tableFields = list.get(tableName).split(",");
                String id = "";
                for ( String singleField : tableFields ){
                    id = id+(rs.getObject(singleField.toLowerCase())==null?"":rs.getObject(singleField.toLowerCase()))+"@";
                }

                IndexRequest insertRequest = new IndexRequest(tableName.replace(".","_")).id(id);
                insertRequest.source(JSONUtils.toJSONString(mapEs), XContentType.JSON);
                bulkRequest.add(insertRequest);
                if (count % 5000 == 0) {
                    esClient.bulk(bulkRequest, RequestOptions.DEFAULT);//批量插入
                    //清空bulk重新批量添加
                    bulkRequest = new BulkRequest();
                    bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    bulkRequest.waitForActiveShards(ActiveShardCount.ALL);
                    System.out.println(count);
                }
                count++;
            }

            if(count%5000!=0){
                esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                System.out.println(tableName+"最后一次执行");
            }

            rs.close();
            ps.close();


        }
        esClient.close();
        con.close();
    }

}
