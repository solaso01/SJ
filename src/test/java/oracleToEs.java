
import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static com.sjzy.utils.MysqlUtil.getTablePK;

/**
 *    
 *  * Simple to Introduction  
 *  * @ProjectName:  [] 
 *  * @Package:      [.]  
 *  * @ClassName:    []   
 *  * @Description:  [一句话描述该类的功能]   
 *  * @Author:       []   
 *  * @CreateDate:   [ ]   
 *  * @UpdateUser:   []   
 *  * @UpdateDate:   [ ]   
 *  * @UpdateRemark: [说明本次修改内容]  
 *  * @Version:      [v1.0] 
 *  
 */
public class oracleToEs {

    public static void main(String[] args) throws SQLException, IOException {

        //获取映射表的信息
        Map<String,String> list = getTablePK("kafka.wpp_test");

        //获取es连接
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.1.6.74", 9200, "http")));

        //获取oracle连接
        Connection conn=null;
        PreparedStatement ps=null;
        ResultSet rs=null;

        try {
            conn = oracleUtils.getconn();

            for ( Map.Entry<String,String> single: list.entrySet() ) {

                //表名
                String mysqlTableName = "msic.prpcmaincargo";
//                String esIndexName = single.getKey().replace(".","_");
                String esIndexName = "test_wpp_prpcplan";
                int flushName = 5000;

                //创建sql语句
                ps = conn.prepareStatement("select * from " + mysqlTableName+" where policyno='DJC0009887000003'");

                //最后在把预处理sql语句进行执行，返回结果集
                rs = ps.executeQuery();

                // 获得结果集结构信息（元数据）
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();

                //遍历每行
                while (rs.next()) {
                    Map<String, Object> mapEs = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        //字段类型为DATE并且不为null
                        if (md.getColumnTypeName(i).equals("DATE") && rs.getObject(i) != null) {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            mapEs.put(md.getColumnName(i).toLowerCase(), sdf.format(rs.getTimestamp(i)));
                        } else {
                            mapEs.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
                        }
                    }

                    mapEs.put("ogg_ts","");
                    mapEs.put("es_ts","");
                    System.out.println(JSONUtils.toJSONString(mapEs));
                    break;

                }

                rs.close();
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
            esClient.close();
        }
    }

}
