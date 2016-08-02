package utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraDao {
	private CassandraDao() {

	}

	public static void update_user_count(Session session, String column,
			boolean tag) {
		if (tag) {
			session.execute("UPDATE presist.user_count SET count = count + 1 WHERE chnname= '"
					+ column + "' ;");
		} else {
			session.execute("UPDATE presist.user_count SET count = count - 1 WHERE chnname= '"
					+ column + "' ;");
		}
	}

	public static void insert(Session session, String keyspace,
			String tablename, String[] columns, Object[] values) {
		session.execute(QueryBuilder.insertInto(keyspace, tablename).values(
				columns, values));
	}

	public static void delete(Session session) {
		
	}

	public static ResultSet select_user_count(Session session, String value) {
		return session.execute(QueryBuilder.select("chnname", "count")
				.from("presist", "user_count")
				.where(QueryBuilder.eq("chnname", value)));
	}

}
