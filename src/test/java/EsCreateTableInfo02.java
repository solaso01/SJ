import com.alibaba.fastjson.JSONObject;
import com.sjzy.utils.MysqlUtil;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *    
 *  *  
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
 *  *    
 *  
 */
public class EsCreateTableInfo02 {
    private static final Integer ONE = 1;
    public static void main(String[] args) throws IOException {

        Map<String, Map<String,String>> maptotal = MysqlUtil.getESColumns();

        for ( Map.Entry<String,Map<String,String>> single: maptotal.entrySet() ){
            String tableName = single.getKey();
            Map<String,String> map = single.getValue();


            JSONObject column=new JSONObject(new LinkedHashMap());
            JSONObject properties=new JSONObject((new LinkedHashMap<>()));
            JSONObject settings = new JSONObject((new LinkedHashMap<>()));
            JSONObject all = new JSONObject((new LinkedHashMap<>()));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\2\\" + tableName+".sql"),true), "UTF-8"));

                for ( Map.Entry<String,String> s : map.entrySet() ){
                    JSONObject js = new JSONObject();
                    js.put("type", s.getValue());
                    column.put(s.getKey(), js);
                }
                JSONObject js = new JSONObject();
                js.put("type", "keyword");
                column.put("ogg_ts",js);
                js = new JSONObject();
                js.put("type", "keyword");
                column.put("es_ts",js);

                properties.put("properties", column);
                settings.put("number_of_shards", 2);
                settings.put("number_of_replicas", 0);
                all.put("settings", settings);
                all.put("mappings", properties);



//                while ((nextLine = br.readLine()) != null) {
//                    String s = nextLine.trim();
//                    all.put("type", "keyword");
//                    column.put(s.toLowerCase(), "js2");
//                    System.out.println(i++ + "  " + column);
//                    count++;
//                }


                //bw.newLine();
                String allString = "curl  -H 'Content-Type: application/json' -XPUT 'http://192.1.6.74:9200/'" + tableName.replace(".","_") + " -d " + "'" + all.toString() + "'\n";
//                String allString = "curl  -H 'Content-Type: application/json' -XPOST http://localhost:9200/" + tables[iii].replace(".","_") + "/_doc -d " + "'" + column.toString().replace("\"","\\\"") + "'\n";
                System.out.println(allString);

                bw.write(allString);
                bw.close();
        }

    }
}
