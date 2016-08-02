package cassandraUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraUtils {
	
	private CassandraUtils(){
		
	}
	
	public static Cluster getCluster(int port,String... contactPoint){
		Cluster cluster = Cluster.builder().addContactPoints(contactPoint).withPort(port).build();
		return cluster;
	}
	
	public static Session getSession(Cluster cluster){
		return cluster.connect();
	}
	
	public static Session getSession(Cluster cluster,String keySpace){
		return cluster.connect(keySpace);
	}
	
	public static void close(Cluster cluster,Session session){
		cluster.close();
		session.close();
	}
	
}
