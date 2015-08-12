package recomm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import db.LitbDBPool;
import db.DBHelper;
import oracle.jdbc.OracleDriver;

/*
 *  将   oracle106 recomm_user_maybe_buy 表中的vip页推荐信息，依据update_date时间戳
 *  增量更新到 mysql104 主机的 recomm_user_maybe_buy 表中
 */

public class VipPageRecommInfo
{
	private static final String sql_str = 
					 " select res3.* "
					+" from "
					+" (  select  "
					+"   res2.*, "
					+"   row_number() over (partition by RES2.CUSCODE ORDER BY RES2.WEIGHT DESC) as rnum2 "
					+"   FROM "
					+"   ( "
					+"     select  "
					+"     res1.*, "
					+"     row_number() over (partition by RES1.CUSCODE,RES1.nextproductid ORDER BY RES1.WEIGHT DESC) as rnum "
					+"     FROM "
					+"     ( "
					+"       select  "
					+"       ore.*, "
					+"       CASE WHEN ORE.PRODUCTTOTALORDER <= 300 THEN (TGOODS * PRODUCTTOTALORDER/300) ELSE TGOODS END as weight, "
					+"		 rank() over(partition by CUSCODE ORDER BY UPDATEDATE DESC) as daternum"
					+"       FROM "
					+"       recomm_user_maybe_buy ore "
					+"       where "
//					+"       CUSCODE = 2372205 "
//					+" 		 where rownum <= 10000 "
//					+"       to_char(UPDATEDATE, 'yyyy-mm-dd hh24') >= to_char(sysdate,'yyyy-mm-dd')||' 00:00:00' "
					+"       UPDATEDATE >= date '%s' "
					+"       and UPDATEDATE < date '%s' "
					+"     )res1 "
					+" 	   where res1.daternum = 1 "
					+"   )res2 "
					+"   WHERE "
					+"   RES2.RNUM = 1 "
					+" )res3 "
					+" where "
					+" RES3.RNUM2 <= 30 "
					+" ORDER BY RES3.CUSCODE, RES3.RNUM2 ";
	
	private static final String find_vip_id_sql_str =
			"select * "
			+" from recom_user_maybe_buy "
//			+" from test_user_maybe_buy "
			+" where user_id in( %s ) ";
	
	private static final String update_sql_str =
			 " update recom_user_maybe_buy "
//			" update test_user_maybe_buy "
		    +" set recommends = '%s' "
		    +" , update_date = now() "
		    +" where user_id = %d ";
			
	private static final String insert_sql_str = 
			 " insert recom_user_maybe_buy "
//			" insert test_user_maybe_buy "
			+" (site_id, user_id, recommends, status, update_date) "
			+" values ( %d, %d, '%s', %d, now()) "
			;
	
	/*
	 * 参数1: 需要更新的开始日期 
	 * 参数2: 需要更新的结束日期
	 * 参数3: 保存商品id和对应推荐id的文件路径
	 * 
	 */
	
	public static void main(String [] args)
	{
		if (args.length < 3)
		{
			System.out.println("请传入3个参数");
			return;
		}
		
		String begin_date_str = args[0];
		String end_date_str = args[1];
		String file_location_str = args[2];
		
		//System.out.println(sql_str);
		
		String file_name_str = file_location_str + "vip_page_recomm_info_" + begin_date_str + "_" + end_date_str + ".log";
		
		getNewVipRecommInfo2File(begin_date_str, end_date_str, file_name_str);
		
		UpdateVipRecommInfo104(file_name_str);
		
		//UpdateVipRecommInfo104("E:\\workspace\\RecommendMachine\\vip_page_recomm_info_2015-08-11.log");
		//UpdateVipRecommInfo104("/home/recomm/crontabapp/RecommendMachine/vip_page_recomm_info_2015-08-11.log");
	}
	
	// 从106的 recomm_user_maybe_buy 获取最新的vip推荐信息, 并写入文件
	public static void getNewVipRecommInfo2File(String begin_date, String end_date, String file_name)
	{
		StringBuffer all_line_buffer = new StringBuffer();
		
		Connection con = null;
		
		try 
		{
			DriverManager.registerDriver(new OracleDriver());
			con =DriverManager.getConnection("jdbc:oracle:thin:@10.0.22.106:1522:kaddw02", "bidev", "bi2015dev"); 
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		
		try 
		{
			PreparedStatement pstmt;
			String query_106_sql = String.format(sql_str, begin_date, end_date);
			pstmt = con.prepareStatement(query_106_sql);
			
//			System.out.println(query_106_sql);
			
			ResultSet set = pstmt.executeQuery(); 
            
			String cus_id_last = "";
			String cus_id_current = "";

			String recomm_list_str = "";
			
			while(set.next())
			{
				cus_id_current = set.getString("cuscode");
				
				if(!cus_id_current.equals(cus_id_last) && !cus_id_last.equals(""))
				{
					//System.out.println(cus_id_last + "\t" + recomm_list_str);
					
					// writeToFile(cus_id_last + "\t" + recomm_list_str + "\n");
					
					// 一行内容存入 buffer
					all_line_buffer.append(cus_id_last + "\t" + recomm_list_str + "\n");
					
					recomm_list_str = "";
				}
				
				if (recomm_list_str.length() <= 0)
					recomm_list_str += set.getString("nextproductid");
				else
					recomm_list_str += "," + set.getString("nextproductid");

				cus_id_last = cus_id_current;
			}
			
			writeToFile(file_name, all_line_buffer.toString());
			
			pstmt.close();
		}
		catch(SQLException e1)
		{
			e1.printStackTrace();
		}
		finally
		{
			if (con != null)
			{
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	// 写入文件
	public static void writeToFile(String file_name, String info_str)
	{
		try{
//			  File file =new File("vip_page_recomm_info.log");
			  File file =new File(file_name);

		      if(file.exists())
		      {
		    	  if (file.isFile())
		          { 
		        	  file.delete();  
		          } 
		      }
		       
		      file.createNewFile();
		      
		      //true = append file
		      FileWriter fileWritter = new FileWriter(file.getAbsolutePath(), true);
		      BufferedWriter bufferWriter = new BufferedWriter(fileWritter);
		      
		      bufferWriter.write(info_str);
		      bufferWriter.close();

		      System.out.println("Done");
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
	
	// 将最新的vip推荐信息增量更新到104主机mysql数据库中
	public static void UpdateVipRecommInfo104(String fileName)
	{
		System.out.println(fileName);
		
		File file = new File(fileName);
		BufferedReader reader = null;
		
		DBHelper helper = null;
		
		try 
		{
			reader = new BufferedReader(new FileReader(file));
			String tmpString = "";
			
			// 保存文件中的vip id 和推荐的商品ids
			HashMap<String, String> id2RecommIdsMap = new HashMap<>();
			
			helper = db.LitbDBPool.getRecommendDbHelper();
			
			while ((tmpString = reader.readLine()) != null)
			{
				// System.out.println(tmpString);
				String [] line_split_str = tmpString.split("\t");
				
				if(line_split_str != null && line_split_str.length > 0)
				{
					String vip_id_str = "";
					String vip_recomm_goods_ids = "";
					
					vip_id_str = line_split_str[0];
					vip_recomm_goods_ids = line_split_str[1];
					
					id2RecommIdsMap.put(vip_id_str, vip_recomm_goods_ids);
				}
			}
			
			// 保存每个新增的vip id是否在104数据库中已经存在的标记
			HashMap<Long, String> id2ExistMap = new HashMap<>();
			for(String new_vip_id : id2RecommIdsMap.keySet())
			{
				// 初始都设置为0,表示新增的vip id在表中不存在
				id2ExistMap.put(Long.parseLong(new_vip_id), "0");
			}
			
			String vip_ids_str = "";
			
			// 需要查询的vip总数
			int total_need_query_vip_num = id2RecommIdsMap.size();
			int checked_vip_num = 0;
			
			for(String key : id2RecommIdsMap.keySet())
			{
				if(vip_ids_str.length() <= 0 )
				{
					vip_ids_str = key;
				}
				else
				{
					vip_ids_str += "," + key;
				}
			
				checked_vip_num++;
				
				if ( checked_vip_num == total_need_query_vip_num ||
					 (checked_vip_num % 500) == 0 )
				{
					// 每500个id做一次查询       
					isVipIdExist(helper, vip_ids_str, id2ExistMap);
					
					vip_ids_str = "";
				}
				
			}
			
			for(long id_key : id2ExistMap.keySet())
			{
				String recomm_goods_ids = id2RecommIdsMap.get(id_key + "");
				
				//System.out.println(id_key + " == " + id2ExistMap.get(id_key));
				
				if (id2ExistMap.get(id_key).equals("1"))
				{
					// 新增的id在原表中存在, update操作
					
					updateVipRecommGoodsIds(helper, id_key, recomm_goods_ids);
				}
				else if (id2ExistMap.get(id_key).equals("0"))
				{
					// 新增的id在原表中不存在, insert操作
					
					insertVipRecommGoodsIds(helper, id_key, recomm_goods_ids);
				}
			}
		} 
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IOException e1) 
		{
			e1.printStackTrace();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(helper != null)
			{
				helper.close();
			}
		}
	}
	
	// 查找vip id 在表中是否存在
	public static void isVipIdExist(DBHelper helper, String vip_ids_str, HashMap<Long, String> id2ExistMap)
	{
		String find_vip_id_sql = String.format(find_vip_id_sql_str, vip_ids_str);
		
		try
		{
			ResultSet vip_id_set = helper.executeQuery(find_vip_id_sql);
			
			while(vip_id_set.next())
			{
				long exist_vip_id = vip_id_set.getLong("user_id");
				
				// 新增的用户id在表中存在
				id2ExistMap.put(exist_vip_id, "1");
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	// 更新数据库中vip id 对应的推荐信息
	public static void updateVipRecommGoodsIds(DBHelper helper, long vip_id, String vip_recomm_goods_ids)
	{
		String update_vip_id_sql = String.format(update_sql_str, vip_recomm_goods_ids, vip_id);
		
		try
		{
			System.out.println(update_vip_id_sql);
			helper.executeUpdate(update_vip_id_sql);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	
	// 增加数据库中 vip id 对应的推荐信息
	public static void insertVipRecommGoodsIds(DBHelper helper, long vip_id, String vip_recomm_goods_ids)
	{
		int site_id = 1;
		int status = 1;
		
		String insert_vip_id_sql = String.format(insert_sql_str, site_id, vip_id, vip_recomm_goods_ids, status);
		
		try
		{
			System.out.println(insert_vip_id_sql);
			helper.executeUpdate(insert_vip_id_sql);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}
}
