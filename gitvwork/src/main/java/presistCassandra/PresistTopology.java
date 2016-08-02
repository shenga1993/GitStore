package presistCassandra;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cassandraUtils.CassandraUtils;

public class PresistTopology {

	public static class PresistBolt extends BaseRichBolt {

		private static final Logger LOG = LoggerFactory
				.getLogger(PresistBolt.class);
		private OutputCollector collector;
		private Session session;
		private Cluster cluster;
		private static final String[] CONTACT_POINTS = { "10.10.121.119",
				"10.10.121.120", "10.10.121.121", "10.10.121.122" };
		private static final int PORT = 9042;
		private static final String TOTAL = "total";

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String log = input.getString(0);
			LOG.info("REC[ LOG : " + log + " ]");
			if (log.split("\\|").length == 8) {
				String[] infos = log.split("\\|");
				String vodType = infos[0];
				String action = infos[1];
				if (action.equals("5") && vodType.toUpperCase().equals("LIVOD")) {
					String mac = infos[2];
					String albumId = infos[7];
					String chnName = infos[5];
					String pt = infos[4];
					String logTime = infos[6];
					// String partner = infos[8];
					// String city = infos[9];
					// 维护一张user_count表里面存储用户总数和每个频道下用户总数
					int chnCount = 0;
					int total = 0;
					if (pt.equals("0")) {
						// pt=0时存储mac的记录到cassandra
						session.execute(QueryBuilder.insertInto("presist",
								"user_info").values(
								new String[] { "mac", "chnname", "logtime",
										"pt" },
								new Object[] { mac, chnName, logTime }));
						// 获取TOTAL的result，如果为空就insert，不为空就update
						ResultSet result = session.execute(QueryBuilder
								.select("chnname", "count")
								.from("presist", "user_count")
								.where(QueryBuilder.eq("chnname", TOTAL)));
						List<Row> list = result.all();
						if (list.isEmpty()) {
							total = 1;
							session.execute(QueryBuilder.insertInto("presist",
									"user_count").values(
									new String[] { "chnname", "count" },
									new Object[] { TOTAL, total }));
						} else {
							int count = list.get(0).getInt("count");
							total = count + 1;
							session.execute(QueryBuilder
									.update("presist", "user_count")
									.with(QueryBuilder.set("count", total))
									.where(QueryBuilder.eq("chnname", TOTAL)));
						}
						// 获取chnName的result，为空就insert，不为空就update
						result = session.execute(QueryBuilder
								.select("chnname", "count")
								.from("presist", "user_count")
								.where(QueryBuilder.eq("chnname", chnName)));
						list = result.all();
						if (list.isEmpty()) {
							chnCount = 1;
							session.execute(QueryBuilder.insertInto("presist",
									"user_count").values(
									new String[] { "chnname", "count" },
									new Object[] { chnName, chnCount }));
						} else {
							int count = list.get(0).getInt("count");
							chnCount = count + 1;
							session.execute(QueryBuilder
									.update("presist", "user_count")
									.with(QueryBuilder.set("count", chnCount))
									.where(QueryBuilder.eq("chnname", chnName)));
						}
					} else if (pt.equals("1") || pt.equals("2")) {
						// pt=1或者2时根据user_info表查询出mac的begin和end时间插入到play_record表中
						ResultSet result = session.execute(QueryBuilder
								.select("mac", "logtime")
								.from("presist", "user_info")
								.where(QueryBuilder.eq("mac", mac)));
						List<Row> list = result.all();
						// 有种情况可能是记录前就已经在观看了，所以只能获取到最新的pt=1或者2的，所以过滤一下
						if (!list.isEmpty()) {
							// pt=1或者2时，查出user_info下的begin时间,插入到play_record表后从user_info表把该mac记录删除;
							String begintime = list.get(0).getString(1);
							LOG.info("mac : " + mac + " beginTime : "
									+ begintime + "---------> endTime : "
									+ logTime);
							session.execute(QueryBuilder.insertInto("presist",
									"play_record").values(
									new String[] { "mac", "begintime",
											"endtime" },
									new Object[] { mac, begintime, logTime }));
							session.execute(QueryBuilder.delete()
									.from("presist", "user_info")
									.where(QueryBuilder.eq("mac", mac)));
						}
						result = session.execute(QueryBuilder
								.select("chnname", "count")
								.from("presist", "user_count")
								.where(QueryBuilder.eq("chnname", TOTAL)));
						list = result.all();
						if (list.isEmpty()) {
							total = -1;
							session.execute(QueryBuilder.insertInto("presist",
									"user_count").values(
									new String[] { "chnname", "count" },
									new Object[] { TOTAL, total }));
						} else {
							int count = list.get(0).getInt("count");
							total = count - 1;
							session.execute(QueryBuilder
									.update("presist", "user_count")
									.with(QueryBuilder.set("count", total))
									.where(QueryBuilder.eq("chnname", TOTAL)));
						}
						result = session.execute(QueryBuilder
								.select("chnname", "count")
								.from("presist", "user_count")
								.where(QueryBuilder.eq("chnname", chnName)));
						list = result.all();
						if (list.isEmpty()) {
							chnCount = -1;
							session.execute(QueryBuilder.insertInto("presist",
									"user_count").values(
									new String[] { "chnname", "count" },
									new Object[] { chnName, chnCount }));
						} else {
							int count = list.get(0).getInt("count");
							chnCount = count - 1;
							session.execute(QueryBuilder
									.update("presist", "user_count")
									.with(QueryBuilder.set("count", chnCount))
									.where(QueryBuilder.eq("chnname", chnName)));
						}
					}
					double audienceRatingPercent = (double) chnCount
							/ (double) total * 100;
					BigDecimal bigDecimal = new BigDecimal(
							audienceRatingPercent);
					audienceRatingPercent = bigDecimal.setScale(2,
							BigDecimal.ROUND_HALF_UP).doubleValue();
					session.execute(QueryBuilder.insertInto("presist",
							"audience_rating").values(
							new String[] { "logtime", "chnname", "albumid",
									"total", "chncount", "percent" },
							new Object[] { logTime, chnName, albumId, total,
									chnCount, audienceRatingPercent + "%" }));

				}
			}
			collector.ack(input);

		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			CassandraUtils.close(cluster, session);
		}

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			this.cluster = CassandraUtils.getCluster(PORT, CONTACT_POINTS);
			this.session = CassandraUtils.getSession(cluster);
			session.execute("CREATE KEYSPACE IF NOT EXISTS presist WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
			session.execute("CREATE TABLE IF NOT EXISTS presist.audience_rating (logtime text,chnname text,albumid text,total bigint,chncount bigint,percent text,PRIMARY KEY (logtime,chnname));");
			session.execute("CREATE TABLE IF NOT EXISTS presist.user_count (chnname text,count bigint,PRIMARY KEY(chnname));");
			session.execute("CREATE TABLE IF NOT EXISTS presist.user_info(mac text,chnname text,logtime text,pt text,PRIMARY KEY(mac,chnname,logtime));");
			session.execute("CREATE TABLE IF NOT EXISTS presist.play_record(mac text,begintime text,endtime text,PRIMARY KEY(mac));");

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "logtest1",
				"/brokers", "spout");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list", "localhost:9092");
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt1", new PresistBolt()).shuffleGrouping("spout");
		StormSubmitter.submitTopology("logto", conf, builder.createTopology());
	}

}
