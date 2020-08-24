package com.sjzy.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * Created by xgwu on 2019/5/28.
 */
public class ConfigurationManager {

	private static Properties prop = new Properties();
	
//	/**
//	 * 静态代码块
//	 */
//	static {
//		try {
//			InputStream in = ConfigurationManager.class
//					.getClassLoader().getResourceAsStream("dataO_dev.properties");
//			prop.load(in);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	/**
	 * 获取指定key对应的value
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}

	/**
	 * 读取sql文件获取String类型的sql
	 * @param sqlFile
	 * @return String
	 */
	public static String loadSql(String sqlFile) {
		//获取sql文件路径
		String sqlFileURL = "sql/"+sqlFile;
		InputStream in = ConfigurationManager.class
				.getClassLoader().getResourceAsStream(sqlFileURL);
		StringBuffer sqlSb = new StringBuffer();
		byte[] buff = new byte[1024];
		int byteRead = 0;
		try {
			while ((byteRead = in.read(buff)) != -1) {
				sqlSb.append(new String(buff, 0, byteRead,"utf-8"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String sql = sqlSb.toString().replaceAll("--.*", "").trim();
		return sql;
	}
	
}