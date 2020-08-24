package com.sjzy.conf;

import com.sjzy.common.CommonConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;



/**
 * Created by wpp on 2020/7/21.
 *
 */
public class ESConf {

    /**
     * 得到es Client
     *
     * @param esHostName    es的hostname
     * @param port          es的端口号
     * @return              返回es Client
     */
    public static RestHighLevelClient getESClient(String esHostName, Integer port){
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(CommonConstants.ES_HOST_NAME, CommonConstants.ES_PORT, "http")));
        return esClient;
    }

    /**
     * 初始化BulkRequest
     *
     * @return  返回初始化后的BulkRequest
     */
    public static BulkRequest initBulkRequest() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest.waitForActiveShards(ActiveShardCount.ALL);
        return bulkRequest;
    }
}
