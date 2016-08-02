package cassandra;


import java.util.List;

import utils.CassandraDao;
import cassandraUtils.CassandraUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class TestCassandra {
	private static final String[] CONTACT_POINTS = { "10.10.121.119","10.10.121.120","10.10.121.121","10.10.121.122"};
	public static void main(String[] args) {
		
		Session session = CassandraUtils.getSession(CassandraUtils.getCluster(9042, CONTACT_POINTS),"presist");
		session.execute("CREATE TABLE IF NOT EXISTS presist.user_count (chnname text,count counter,PRIMARY KEY(chnname));");
//		ResultSet result = session.execute("SELECT * FROM presist.user_info limit 10");
//		session.execute("UPDATE presist.asd SET b = b+1 WHERE a = 'ccc'");
//		CassandraDao.update_user_count(session, "total");
//		CassandraDao.insert(session, "presist", "user_info", new String[]{"mac","chnname","logtime","pt"}, new Object[]{"aa	","bbb","cccc","ddddd"});
//		session.execute("UPDATE presist.user_count SET count = count + 1 WHERE chnname = 'total' ;");
		CassandraDao.update_user_count(session, "total",true);
		CassandraDao.update_user_count(session, "CCAV-1",false);
//		System.out.println(result.all().size());
//		for(Row row:result){
//			System.out.println("begin");
//			System.out.println(row.getString("mac")+"\t"+row.getString("chnname")+"\t"+row.getString("logtime"));
//		}
		
//		insert(session);
//		result =session.execute(QueryBuilder.select("chnname","count").from("presist", "user_count"));
//		result = session.execute("SELECT * FROM presist.user_count");
//		List<Row> list = result.all();
//		if(list.isEmpty()){
//			
//		}else{
//			int count = 0;
//			for(Row row:list){
//				count = (int) row.getObject("count");
//			}
//			session.execute(QueryBuilder.update("presist", "user_count").with(QueryBuilder.set("count", count+1)).where(QueryBuilder.eq("chnname","total")));
//		}
//		System.out.println(list.size());
//		System.out.println(list.get(0).getObject("count"));
//		session.execute(QueryBuilder.update("presist", "user_count").with(QueryBuilder.set("count", 100)).where(QueryBuilder.eq("chnname","total")));
//		for(Row row:list){
//			System.out.println(row.getString("chnname")+"\t"+row.getObject("count"));
//			
//		}
//		insert(session);
//		ResultSet result1 = null;
//		result = session.execute(QueryBuilder.select("chnname","count").from("presist", "user_count").where(QueryBuilder.eq("chnname", "CCAV-2")));
//		result1=result;
//		List<Row> list = result.all();
//		System.out.println(list.size());
//		System.out.println(list.isEmpty()?1:0);
//		for(Row row:list){
//			System.out.println(row.getString("chnname")+"\t"+row.getObject("count"));
//		}
	
	}
	
	public static void insert(Session session){
		session.execute(QueryBuilder.insertInto("presist", "user_count").values(new String[]{"chnname","count"}, new Object[]{"total",20}));
	}

}
