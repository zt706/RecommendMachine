package db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LitbDBPool {
	// recomm_mysql
	private static final String DB_RECOMM_CONN_STR = "jdbc:mysql://10.0.22.104:3306/";
	private static final String DB_RECOMM_USER_NAME = "recommdev";
	private static final String DB_RECOMM_PASSWORD = "dev360kad..com";
	
	// 疾病表oracle数据库
	private static final String DB_DISEASE_CONN_STR = "jdbc:oracle:thin:@192.168.1.180:1521:";
	private static final String DB_DISEASE_USER_NAME = "khydis";
	private static final String DB_DISEASE_PASSWORD = "khydis360kad";
	
	// 药品表oracle数据库
	private static final String DB_MEDICISE_CONN_STR = "jdbc:oracle:thin:@192.168.1.180:1521:";
	private static final String DB_MEDICISE_USER_NAME = "khydrugs";
	private static final String DB_MEDICISE_PASSWORD = "khydrugs360kad";
		
	// MySql definitions
	 
	private static final String DB_64_CONN_STR = "jdbc:mysql://172.16.0.64:3306/";
	private static final String DB_64_USER_NAME = "db_admin";
	private static final String DB_64_PASSWORD = "hx27kf1";//"bi@2012app";

//	private static final String DB_75_CONN_STR = "jdbc:mysql://172.16.0.75:3306/";
//	private static final String DB_75_USER_NAME = "db_admin";
//	private static final String DB_75_PASSWORD = "light2902";
	
	private static final String DB_17_CONN_STR = "jdbc:mysql://172.16.0.17:3306/";
	private static final String DB_17_USER_NAME = "db_admin";
	private static final String DB_17_PASSWORD = "b71bfc10";
	
	private static final String DB_170_CONN_STR = "jdbc:mysql://172.16.0.170:3306/";
	private static final String DB_170_USER_NAME = "db_admin";
	private static final String DB_170_PASSWORD = "b71bfc10";
	
//	private static final String DB_14_CONN_STR = "jdbc:mysql://172.16.0.14:3306/";
//	private static final String DB_14_USER_NAME = "db_admin";
//	private static final String DB_14_PASSWORD = "light2902";
	
//	private static final String DB_165_CONN_STR = "jdbc:mysql://172.16.0.165:3306/";
//	private static final String DB_165_USER_NAME = "db_admin";
//	private static final String DB_165_PASSWORD = "light2902";
	
	private static final String DB_TEST_15_CONN_STR = "jdbc:mysql://192.168.66.15:3306/";
	private static final String DB_TEST_15_USER_NAME = "db_admin";
	private static final String DB_TEST_15_PASSWORD = "light2010";
	
	private static final String DB_TEST_31_CONN_STR = "jdbc:mysql://192.168.66.31:3306/";
	private static final String DB_TEST_31_USER_NAME = "db_admin";
	private static final String DB_TEST_31_PASSWORD = "light2902";
	
	private static final String APPENDED_DB_INFO = "?useUnicode=true&characterEncoding=UTF8&rewriteBatchedStatements=true&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai";
//	private static final String APPENDED_DB_INFO_2 = "?useUnicode=true&characterEncoding=UTF8&useJDBCCompliantTimezoneShift=true";
//	private static final String APPENDED_DB_INFO_2 = "?characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;transformedBitIsBoolean=true&amp;sessionVariables=time_zone%3D%27%2B08%3A00%27&amp;autoReconnect=true";
	
	// Oracle definitions
	
//	private static final String DB_170_CONN_STR = "jdbc:oracle:thin:@172.16.0.170:1521:";
//	private static final String DB_170_USER_NAME = "waapp";
//	private static final String DB_170_PASSWORD = "WazzZZ";
	
	private static final String DB_223_CONN_STR = "jdbc:oracle:thin:@192.168.1.223:1521:";
	private static final String DB_223_USER_NAME = "dw01";
	private static final String DB_223_PASSWORD = "dw01";
	
	private static final String DB_100_CONN_STR = "jdbc:oracle:thin:@10.0.22.100:1521:";
	private static final String DB_100_USER_NAME = "bipr";
	private static final String DB_100_PASSWORD = "bi2014@pr";
	
//	private static final String DB_100_USER_NAME = "bidev";
//	private static final String DB_100_PASSWORD = "bikaddev";
//	"birpt"
//	"bi2014@rpt"
	
	private static final String DB_176_CONN_STR = "jdbc:oracle:thin:@172.16.0.176:1521:";
	private static final String DB_176_USER_NAME = "waapp";
	private static final String DB_176_PASSWORD = "WazzZZ";
	
	private static final String DB_176_ADPLAT_CONN_STR = "jdbc:oracle:thin:@172.16.0.176:1521:";
	private static final String DB_176_ADPLAT_USER_NAME = "adplat";
	private static final String DB_176_ADPLAT_PASSWORD = "adthick";
	
	private static final String DB_BI_ADWORDS_CONN_STR = "jdbc:oracle:thin:@172.16.0.182:1521:";
	private static final String DB_BI_ADWORDS_USER_NAME = "adplat";
	private static final String DB_BI_ADWORDS_PASSWORD = "adplat";
	
	private static final String DB_AD_ADWORDS_CONN_STR = "jdbc:oracle:thin:@172.16.0.40:1521:";
	private static final String DB_AD_ADWORDS_USER_NAME = "adplat";
	private static final String DB_AD_ADWORDS_PASSWORD = "adplat";
	
	// test database
	
	public static DBHelper getVelaTest15DBHelper() throws SQLException{
		String dbName = "litb_inbox_master";
		return new MySqlHelper(DB_TEST_15_CONN_STR + dbName + APPENDED_DB_INFO, DB_TEST_15_USER_NAME, DB_TEST_15_PASSWORD);
	}
	
	public static DBHelper getProductCenterTest31DBHelper() throws SQLException{
		String dbName = "products_center_v1";
		return new MySqlHelper(DB_TEST_31_CONN_STR + dbName + APPENDED_DB_INFO, DB_TEST_31_USER_NAME, DB_TEST_31_PASSWORD);
	}
	

	// products center connections
	
	public static DBHelper getProductCenter64DBHelper() throws SQLException{
		String dbName = "products_center_v1";
		return new MySqlHelper(DB_64_CONN_STR + dbName + APPENDED_DB_INFO, DB_64_USER_NAME, DB_64_PASSWORD);
	}
	
	public static DBHelper getProductCenter17DBHelper() throws SQLException{
		String dbName = "products_center_v1";
		return new MySqlHelper(DB_17_CONN_STR + dbName + APPENDED_DB_INFO, DB_17_USER_NAME, DB_17_PASSWORD);
	}
	
//	public static DBHelper getProductCenter165DBHelper() throws SQLException{
//		String dbName = "products_center_v1";
//		return new MySqlHelper(DB_165_CONN_STR + dbName + APPENDED_DB_INFO, DB_165_USER_NAME, DB_165_PASSWORD);
//	}
	
	// merchant center connectionss
	
//	public static DBHelper getMerchantCenter75DBHelper(SiteType siteType) throws SQLException{
//		String dbName;
//		switch (siteType) {
//		case litb:
//			dbName = "test_litb_cart";
//			break;
//		case mini:
//			dbName = "test_mini_cart";
//			break;
//		case hikari:
//			dbName = "test_hikari_cart";
//			break;
//		default:
//			throw new IllegalArgumentException("Unsupported site type for getting merchant center: " + siteType);
//		}
//		return new MySqlHelper(DB_75_CONN_STR + dbName + APPENDED_DB_INFO, DB_75_USER_NAME, DB_75_PASSWORD);
//	}
	
//	public static DBHelper getMerchantCenter14DBHelper(SiteType siteType) throws SQLException{
//		String dbName;
//		switch (siteType) {
//		case litb:
//			dbName = "merchant_center_vela_v1";
//			break;
//		case mini:
//			dbName = "merchant_center_mini_v1";
//			break;
//		case hikari:
//			dbName = "merchant_center_hikari_v1";
//			break;
//		default:
//			throw new IllegalArgumentException("Unsupported site type for getting merchant center: " + siteType);
//		}
//		return new MySqlHelper(DB_14_CONN_STR + dbName + APPENDED_DB_INFO, DB_14_USER_NAME, DB_14_PASSWORD);
//	}
	
	// Oracle
	
//	public static DBHelper getBIOracle170DBHelper() throws SQLException{
//		String dbName = "dw01";
//		return new OracleHelper(DB_170_CONN_STR + dbName, DB_170_USER_NAME, DB_170_PASSWORD);
//	}
	
	public static DBHelper getBIOracle176DBHelper() throws SQLException{
		String dbName = "dw01";
		return new OracleHelper(DB_176_CONN_STR + dbName, DB_176_USER_NAME, DB_176_PASSWORD);
	}
	
	public static DBHelper getBIOracle223DBHelper() throws SQLException{
		String dbName = "kaddw01";
		return new OracleHelper(DB_223_CONN_STR + dbName, DB_223_USER_NAME, DB_223_PASSWORD);
	}
	
	public static DBHelper getBIOracle100DBHelper() throws SQLException{
		String dbName = "pkaddw01";
		return new OracleHelper(DB_100_CONN_STR + dbName, DB_100_USER_NAME, DB_100_PASSWORD);
	}
	
	public static DBHelper getBIOracle176AdplatDBHelper() throws SQLException{
		String dbName = "dw01";
		return new OracleHelper(DB_176_ADPLAT_CONN_STR + dbName, DB_176_ADPLAT_USER_NAME, DB_176_ADPLAT_PASSWORD);
	}
	
	public static DBHelper getOracleAdwordsDBHelper() throws SQLException{
		String dbName = "dwtest";
		return new OracleHelper(DB_BI_ADWORDS_CONN_STR + dbName, DB_BI_ADWORDS_USER_NAME, DB_BI_ADWORDS_PASSWORD);
	}
	
	public static DBHelper getAdOracleAdwordsDBHelper() throws SQLException{
		String dbName = "adplat01";
		return new OracleHelper(DB_AD_ADWORDS_CONN_STR + dbName, DB_AD_ADWORDS_USER_NAME, DB_AD_ADWORDS_PASSWORD);
	}
	
	// bi_db
	
	public static DBHelper getBIDB64DBHelper() throws SQLException{
		String dbName = "bi_db";
		return new MySqlHelper(DB_64_CONN_STR + dbName + APPENDED_DB_INFO, DB_64_USER_NAME, DB_64_PASSWORD);
	}
	
	public static DBHelper getRecommendDbHelper() throws SQLException{
		String dbName = "recommend";
		return new MySqlHelper(DB_RECOMM_CONN_STR + dbName + APPENDED_DB_INFO, DB_RECOMM_USER_NAME, DB_RECOMM_PASSWORD);
	}
	
	// 疾病表配置
	public static DBHelper getOracleErpDbHelper() throws SQLException{
		String dbName = "KADERP";
		return new OracleHelper(DB_DISEASE_CONN_STR + dbName, DB_DISEASE_USER_NAME, DB_DISEASE_PASSWORD);
	}
	
	// 药品表配置
	public static DBHelper getOracleDrugDbHelper() throws SQLException{
		String dbName = "KADERP";
		return new OracleHelper(DB_MEDICISE_CONN_STR + dbName, DB_MEDICISE_USER_NAME, DB_MEDICISE_PASSWORD);
	}
	// for test
	
	public static void main(String[] args) throws SQLException {
		System.out.println("Testing recomm...");
		DBHelper helper = getRecommendDbHelper();
		String sql = "select site_id, goodsid,recommends  from recom_cart_bought_together where goodsid = 56 and"
				+ " site_id = 1 and status = 1 limit 1";
		System.out.println(sql);
		ResultSet result = helper.executeQuery(sql);
		while(result.next())
			System.out.println(result.getInt("goodsid") + "\t" + result.getString("recommends"));
		helper.close();
		System.out.println("Done.");		
	}
}
