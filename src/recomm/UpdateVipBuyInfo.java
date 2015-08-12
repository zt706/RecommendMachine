package recomm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import oracle.jdbc.OracleDriver;

/*
 * 将订单详情表中每天有新增订单的用户订单信息
 * 增量更新到 oracle106 recomm_user_maybe_buy 表中
 */

public class UpdateVipBuyInfo
{
	private static final String sql_str = 
					" Insert into recomm_user_maybe_buy(cuscode, "
					+"                               ordercode, "
					+"                               wareskucode, "
					+"                               warename, "
					+"                               producttotalorder, "
					+"                               buywarecode, "
					+"                               notlastnum, "
					+"                               nextproductid, "
					+"                               nextproductname, "
					+"                               frequency, "
					+"                               tgoods, "
					+"                               updatedate  "
					+"                               ) "
					+" select   "
					+" res7.cuscode, "
					+" res7.ordercode, "
					+" res7.wareskucode, "
					+" res7.warename, "
					+" res8.producttotalorder, "
					+" res8.wareskucode as buywarecode, "
					+" res8.notlastnum, "
					+" res8.nextproductid, "
					+" res8.nextproductname, "
					+" res8.frequency, "
					+" res8.TGoods,sysdate  as  updatedate "
					+" from  "
					+" ( "
					+"    select  "
					+"  gk2.*,sysdate  as  updatedate2 "
					+"  from "
//					+"   (select distinct cuscode,max(row_id) as ordertotalnum "
//					+"      from "
//					+"      ( "
//					+"         select   "
//					+"         a.ordercode as  ordercode, "
//					+"         a.cuscode as cuscode， "
//					+"         a.CreateDate as CreateDate, "
//					+"         b.wareskucode as wareskucode, "
//					+"         b.warename as warename, "
//					+"         a1.Qty as Qty, "
//					+"         a1.AMT as AMT, "
//					+"         a.netamt as netamt,"
//					+"         a.OrderSource as originid, "
//					+"         dense_rank () over (partition by a.cuscode order by a.CreateDate ) as row_id "
//					+"         from  "
//					+"         OM.OM_ORDER a, "
//					+"         OM.OM_ORDERDETAIL a1， "
//					+"         base.WI_WARESKU   b  "
//					+"         where  "
//					+"         a.ordercode=a1.ordercode "
//					+"         and a1.WARECODE=b.wareskucode "
//					+"         and a.orderstatus not in (0,10) "
//					+"         and  a.OrderSource in (1,2,13,14,15,20,21)  "
//					+"         and a1.AMT >  0 "
//					+"         and to_char(a.CreateDate, 'yyyy-mm-dd hh24') >= to_char(sysdate-2,'yyyy-mm-dd')||' 24:00:00'  "
//					+"         and to_char(a.CreateDate, 'yyyy-mm-dd hh24') < to_char(sysdate - 1,'yyyy-mm-dd')||' 24:00:00' "
//					+"         order by a.cuscode,row_id "
//					+"             ) "
//					+"   group by cuscode "
//					+"   )gk1, "
					+"   ( "
					+"     select   "
					+"         a.ordercode as  ordercode, "
					+"         a.cuscode as cuscode， "
					+"         a.CreateDate as CreateDate, "
					+"         b.wareskucode as wareskucode, "
					+"         b.warename as warename, "
					+"         a1.Qty as Qty, "
					+"         a1.AMT as AMT, "
					+"         a.netamt as netamt, "
					+"         a.OrderSource as originid, "
					+"         dense_rank () over (partition by a.cuscode order by a.CreateDate desc) as row_id "
					+"         from  "
					+"         OM.OM_ORDER a, "
					+"         OM.OM_ORDERDETAIL a1， "
					+"         base.WI_WARESKU   b "
					+"         where  "
					+"         a.ordercode=a1.ordercode "
					+"         and a1.WARECODE=b.wareskucode "
					+"         and a.orderstatus not in (0,10) "
					+"         and  a.OrderSource in (1,2,13,14,15,20,21)  "
					+"         and a1.AMT >  0 "
					+"         and to_char(a.CreateDate, 'yyyy-mm-dd hh24') >= to_char(sysdate-2,'yyyy-mm-dd')||' 24:00:00'  "
					+"         and to_char(a.CreateDate, 'yyyy-mm-dd hh24') < to_char(sysdate - 1,'yyyy-mm-dd')||' 24:00:00' "
					+"         order by a.cuscode,row_id "
					+"   )  gk2 "
					+"   where "
//					+"   gk1.cuscode=gk2.cuscode "
//					+"   and  gk2.row_id = gk1.ordertotalnum "
					+"   gk2.row_id = 1 "
					+" ) res7, "
					+" ( "
					+"       select res10.* "
					+"       from "
					+"       (select  "
					+"       res9.*, "
					+"       row_number() over (partition by res9.wareskucode order by res9.frequency desc) as rerowid "
					+"       from "
					+"       OM.OM_Recommend_1 res9 "
					+"       )res10 "
					+"       where res10.rerowid <= 20 "
					+" ) res8 "
					+"  where  "
					+"  res7.wareskucode=res8.wareskucode "
					+"  order  by res7.cuscode ";
			
	public static void main(String [] args)
	{
		//System.out.println(sql_str);
		
		formaSql();
	}
	
	public static void formaSql()
	{
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
			pstmt = con.prepareStatement(sql_str);
			
			//System.out.println(sql_str);
			
			ResultSet set = pstmt.executeQuery(); 
            
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
				try 
				{
					con.close();
				} 
				catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

