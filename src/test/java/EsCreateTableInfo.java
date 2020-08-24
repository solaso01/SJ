



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.*;

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
public class EsCreateTableInfo {
    private static final Integer ONE = 1;
    public static void main(String[] args) {
        String table = "msic.prpcmain,msic.prpcaddress,msic.prpcitemkind,msic.prpcitem,msic.prpccommission,msic.prpcengage,msic.prpclimit,msic.prpcmainliab,msic.prpcmainprop,msic.prpcration,msic.prpcclaimagent,msic.prpccoins,msic.prpcfee,msic.prpcinsured,msic.prpcplan,msic.prpccommissiondetail,msic.prpcinsuredidvlist,msic.prpcmaincargo,msic.prpcmainconstruct,msic.prpcridecarinfo,msic.prpcname,msic.prpcitemship,msic.prpcitemcargo";


        String[] tables = table.split(",");
        int count = 0;

        /* 读取数据 */
        try {
            for ( int iii = 0; iii < tables.length; iii++) {


                 JSONObject smallColumn = new JSONObject((new LinkedHashMap<>()));
                 JSONObject column=new JSONObject(new LinkedHashMap());
                 JSONObject properties=new JSONObject((new LinkedHashMap<>()));
                 JSONObject settings = new JSONObject((new LinkedHashMap<>()));
                 JSONObject all = new JSONObject((new LinkedHashMap<>()));
                smallColumn.put("type", "keyword");
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:\\Users\\Administrator\\Desktop\\1\\"+tables[iii]+".sql")), "UTF-8"));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\2\\" + tables[iii]),true), "UTF-8"));
                String nextLine = null;
                int i = 1;

                while ((nextLine = br.readLine()) != null) {
                    String s = nextLine.trim();
                    JSONObject js = new JSONObject();
                    js.put("type", "keyword");
                    column.put(s.toLowerCase(), js);
                    System.out.println(i++ + "  " + column);
                    count++;
                }
                JSONObject js = new JSONObject();
                js.put("type", "keyword");
                column.put("ogg_ts",js);
                js = new JSONObject();
                js.put("type", "keyword");
                column.put("es_ts",js);

                properties.put("properties", column);
                settings.put("number_of_shards", 6);
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
                String allString = "curl  -H 'Content-Type: application/json' -XPUT 'http://192.1.6.74:9200/'" + tables[iii].replace(".","_") + " -d " + "'" + all.toString() + "'\n";
//                String allString = "curl  -H 'Content-Type: application/json' -XPOST http://localhost:9200/" + tables[iii].replace(".","_") + "/_doc -d " + "'" + column.toString().replace("\"","\\\"") + "'\n";
                System.out.println(allString);

                bw.write(allString);
                br.close();
                bw.close();
            }
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
        System.out.println("共"+count+"行");

    }
}
