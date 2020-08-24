package com.sjzy.utils;

import com.sjzy.common.CommonConstants;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * mysql utils编写
 *
 */
public class MysqlUtil {

	//加载MySql的驱动类
    public static Connection getConnection(){
        Connection connectMySQL= null;
        try{
            Class.forName("com.mysql.jdbc.Driver") ;
            connectMySQL = DriverManager.getConnection(CommonConstants.MYSQL_URL, CommonConstants.MYSQL_USER,CommonConstants.MYSQL_PASSWORD);
        }catch(Exception e){
            e.printStackTrace();
        }
        return connectMySQL;
    }


	/**
	 * 得到映射表中的内容（表明，表主键）
	 *
	 * @param tableName		mysql中的映射表名
	 * @return mapTablePk	映射表的内容（表明，表主键）
	 */
	public static Map<String,String> test(String tableName) {

		//连接MySQL得到映射表并将其存储
		List<Map<String,String>> kuduTbMapping= MysqlUtil.selectMappingTable(tableName);

		//存储mysql中记录的表名和表主键
		Map<String,String> mapTablePk = new HashMap<>();

		for(Map<String,String> map : kuduTbMapping){
			String ods_tb = map.get("ods_tb");
			String ods_tb_pk = map.get("ods_tb_pk");
			mapTablePk.put(ods_tb,ods_tb_pk);
		}

		return mapTablePk;
	}

	/**
	 * 得到映射表中的内容（表明，表主键）
	 *
	 * @param tableName		mysql中的映射表名
	 * @return mapTablePk	映射表的内容（表明，表主键）
	 */
	public static Map<String,String> getTablePK(String tableName) {

		//连接MySQL得到映射表并将其存储
		List<Map<String,String>> kuduTbMapping= MysqlUtil.selectMappingTable(tableName);

		//存储mysql中记录的表名和表主键
		Map<String,String> mapTablePk = new HashMap<>();

		for(Map<String,String> map : kuduTbMapping){
			String ods_tb = map.get("ods_tb");
			String ods_tb_pk = map.get("ods_tb_pk");
			mapTablePk.put(ods_tb,ods_tb_pk);
		}

		return mapTablePk;
	}


    public static List<Map<String,String>> selectMappingTable(String string) {
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rset = null;
        try{
            conn = getConnection();
            stmt = conn.createStatement();
            rset = stmt.executeQuery("select * from "+string+" where status = '1' ");
            int fieldsNum = rset.getMetaData().getColumnCount();
            String[] cols = new String[fieldsNum];
            for (int i = 1; i <= fieldsNum; i++) {
                cols[i-1] = rset.getMetaData().getColumnName(i).toLowerCase();
            }
            while (rset.next()) {
                Map<String,String> map = null;
                map = new HashMap<String,String>();
                for (int i = 0; i < fieldsNum; i++) {
                    String value = rset.getString(cols[i]);
                    if(value!=null)
                        value = value.trim();
                    map.put(cols[i],value);
                }
                list.add(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(conn, stmt, rset);
        }
        return list;
    }
    /**
	 * @Title: query   
	 * @Description: 获取kafka-offset
	 * @param sql
	 * @return      
	 * @throws
	 */
	public static List<Map<String,String>> query(String sql) {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Connection connection = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			int numcols = rs.getMetaData().getColumnCount();
			String[] cols = new String[numcols];
			for (int i = 1; i <= numcols; i++) {
				cols[i-1] = rs.getMetaData().getColumnName(i).toLowerCase();
			}
			while (rs.next()) {
				Map<String,String> map = null;
				map = new HashMap<String,String>();
				for (int i = 0; i < numcols; i++) {
					String value = rs.getString(cols[i]);
					if(value!=null)
						value = value.trim();
						map.put(cols[i],value);
				}
				list.add(map);
			}	        
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(connection,stmt, rs);
		}
		return list;
	}


	public static Map<String,Map<String,String>> getESColumns() {
		String sql = "select table_name, es_column, es_column_type from kafka.tb_column_mapping";
		Map<String,Map<String,String>> mapColumnType = new HashMap<String, Map<String, String>>();
		Connection connection = null;
		Statement stmt = null;
		ResultSet rset = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rset = stmt.executeQuery(sql);
			int fieldsNum = rset.getMetaData().getColumnCount();
			String[] cols = new String[fieldsNum];
			for (int i = 1; i <= fieldsNum; i++) {
				cols[i-1] = rset.getMetaData().getColumnName(i).toLowerCase();
			}
			while (rset.next()) {
				String tableName = rset.getString(cols[0]);
				if ( mapColumnType.get(tableName) == null ){
					Map<String,String> map =  new HashMap<String,String>();
					map.put(rset.getString(cols[1]),rset.getString(cols[2]));
					mapColumnType.put(tableName,map);
				}else {
					Map<String,String> map = mapColumnType.get(tableName);
					map.put(rset.getString(cols[1]),rset.getString(cols[2]));
					mapColumnType.put(tableName,map);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(connection,stmt, rset);
		}
		return mapColumnType;
	}


	/**
	 * @Title: execUpdateOffset   
	 * @Description: 执行更新offset
	 * @param strlist      
	 * @throws
	 */
	public static void execUpdateOffset(List<String> strlist) {
		String sql = "insert into kafka.offsetloges values(?,?,?,?,?,?) on duplicate key update "
				+ "topic=?,partitiona=?,fromoffset=?,untiloffset=?,operatime=?,type=?";
		Connection connection = null;
		PreparedStatement prest = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			connection.setAutoCommit(false); // 设置手动提交 
			prest = connection.prepareStatement(sql);
			long count = 1;
			for (String str : strlist) {
				String fromOffset = str.split("\\^")[0];
				String untilOffset = str.split("\\^")[1];
				String operatime = str.split("\\^")[2];
				String topic = str.split("\\^")[3];
				String partition = str.split("\\^")[4];
				String type = str.split("\\^")[5];
				prest.setString(1, topic);
				prest.setString(2, partition);
				prest.setString(3, fromOffset);
				prest.setString(4, untilOffset);
				prest.setString(5, operatime);
				prest.setString(6, type);
				prest.setString(7, topic);
				prest.setString(8, partition);
				prest.setString(9, fromOffset);
				prest.setString(10, untilOffset);
				prest.setString(11, operatime);
				prest.setString(12, type);
				prest.addBatch();//加入批量处理  
                if (count % 1000 == 0) {
                	prest.executeBatch();
                	connection.commit();// 提交  
                	prest.clearBatch();
                }
                count ++;
			}
			prest.executeBatch();// 执行批量处理  
			connection.commit();// 提交 
			prest.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(connection,prest, rs);
		}
	}
    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
