package bak;

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

import utils.CassandraDao;
import utils.LogTimeParse;
import utils.ZkUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cassandraUtils.CassandraUtils;

public class AudienceRatingTopology {

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
			try {

				String log = input.getString(0);
				LOG.info("REC[ LOG : " + log + " ]");
				if (log.split("\\|").length == 8) {
					String[] infos = log.split("\\|");
					String vodType = infos[0];
					String action = infos[1];
					if (action.equals("5")
							&& vodType.toUpperCase().equals("LIVOD")) {
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
							LOG.info("save record: mac-->" + mac + "chnname-->"
									+ chnName + "logtime-->" + logTime
									+ "pt-->" + pt);
							CassandraDao.insert(session, "presist",
									"user_info", new String[] { "mac",
											"chnname", "logtime", "pt" },
									new Object[] { mac, chnName, logTime, pt });
							// 获取TOTAL的result，如果为空就insert，不为空就update
							// ResultSet result = session.execute(QueryBuilder
							// .select("chnname", "count")
							// .from("presist", "user_count")
							// .where(QueryBuilder.eq("chnname", TOTAL)));
							// List<Row> list = result.all();
							// LOG.info(logTime + "\t" + TOTAL + "\t"
							// + (list.isEmpty() ? 1 : 0));
							// if (list.isEmpty()) {
							// LOG.info("TOTAL is empty then insert totalCount="
							// + 1);
							// total = 1;
							// session.execute(QueryBuilder.insertInto(
							// "presist", "user_count").values(
							// new String[] { "chnname", "count" },
							// new Object[] { TOTAL, total + "" }));
							// LOG.info("empty insert success");
							// } else {
							// LOG.info(TOTAL + " not empty");
							// int count = 0;
							// for (Row row : list) {
							// LOG.info(count + "----------------------");
							// count = Integer.parseInt(row
							// .getString("count"));
							// LOG.info(count + "-----------------------");
							// }
							// total = count + 1;
							// LOG.info("TOTAL is not empty then insert totalCount="
							// + total);
							// session.execute(QueryBuilder
							// .update("presist", "user_count")
							// .with(QueryBuilder.set("count", total
							// + ""))
							// .where(QueryBuilder
							// .eq("chnname", TOTAL)));
							// LOG.info("not empty update success");
							// }
							CassandraDao.update_user_count(session, TOTAL,true);
							CassandraDao.update_user_count(session, chnName,true);
							// 获取chnName的result，为空就insert，不为空就update
							// result = session
							// .execute(QueryBuilder
							// .select("chnname", "count")
							// .from("presist", "user_count")
							// .where(QueryBuilder.eq("chnname",
							// chnName)));
							// list = result.all();
							// LOG.info(logTime + "\t" + chnName + "\t"
							// + (list.isEmpty() ? 1 : 0));
							// if (list.isEmpty()) {
							// LOG.info(chnName
							// + " is empty then insert totalCount="
							// + 1);
							// chnCount = 1;
							// session.execute(QueryBuilder.insertInto(
							// "presist", "user_count")
							// .values(new String[] { "chnname",
							// "count" },
							// new Object[] { chnName,
							// chnCount + "" }));
							// LOG.info(chnName + " empty insert success ");
							// } else {
							// int count = Integer.parseInt(list.get(0)
							// .getString("count"));
							// chnCount = count + 1;
							// LOG.info(chnName
							// + " is not empty then insert totalCount="
							// + chnCount);
							// session.execute(QueryBuilder
							// .update("presist", "user_count")
							// .with(QueryBuilder.set("count",
							// chnCount + ""))
							// .where(QueryBuilder.eq("chnname",
							// chnName)));
							// LOG.info(chnName + "update success");
							// }
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
								String begintime = list.get(0).getString(
										"logtime");
								LOG.info("mac : " + mac + " beginTime : "
										+ begintime + "---------> endTime : "
										+ logTime);
								// session.execute(QueryBuilder.insertInto(
								// "presist", "play_record")
								// .values(new String[] { "mac",
								// "begintime", "endtime" },
								// new Object[] { mac, begintime,
								// logTime }));
								CassandraDao
										.insert(session, "presist",
												"play_record", new String[] {
														"mac", "begintime",
														"endtime" },
												new Object[] { mac, begintime,
														logTime });
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
								session.execute(QueryBuilder.insertInto(
										"presist", "user_count").values(
										new String[] { "chnname", "count" },
										new Object[] { TOTAL, total + "" }));
							} else {
								int count = Integer.parseInt(list.get(0)
										.getString("count"));
								total = count - 1;
								session.execute(QueryBuilder
										.update("presist", "user_count")
										.with(QueryBuilder.set("count", total
												+ ""))
										.where(QueryBuilder
												.eq("chnname", TOTAL)));
							}
							result = session
									.execute(QueryBuilder
											.select("chnname", "count")
											.from("presist", "user_count")
											.where(QueryBuilder.eq("chnname",
													chnName)));
							list = result.all();
							if (list.isEmpty()) {
								chnCount = -1;
								session.execute(QueryBuilder.insertInto(
										"presist", "user_count")
										.values(new String[] { "chnname",
												"count" },
												new Object[] { chnName,
														chnCount + "" }));
							} else {
								int count = Integer.parseInt(list.get(0)
										.getString("count"));
								chnCount = count - 1;
								session.execute(QueryBuilder
										.update("presist", "user_count")
										.with(QueryBuilder.set("count",
												chnCount + ""))
										.where(QueryBuilder.eq("chnname",
												chnName)));
							}
							CassandraDao.update_user_count(session, TOTAL, false);
							CassandraDao.update_user_count(session, chnName, false);
						}
						double audienceRatingPercent = 0;
						if (total != 0) {
							audienceRatingPercent = (double) chnCount
									/ (double) total * 100;
						}
						LOG.info(logTime + " : " + chnName
								+ "------>audienceRatingPercent ："
								+ audienceRatingPercent + "%");
						BigDecimal bigDecimal = new BigDecimal(
								audienceRatingPercent);
						audienceRatingPercent = bigDecimal.setScale(2,
								BigDecimal.ROUND_HALF_UP).doubleValue();
						long time = LogTimeParse.getParseTime(logTime);
						session.execute(QueryBuilder.insertInto("presist",
								"audience_rating").values(
								new String[] { "logtime", "chnname", "albumid",
										"total", "chncount", "percent" },
								new Object[] { logTime, chnName, albumId,
										total + "", chnCount + "",
										audienceRatingPercent + "%" }));

					}
				}
			} catch (Exception e) {
				LOG.info("error :\t" + e.getMessage());
				collector.fail(input);
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
			LOG.info("create keyspace");
			session.execute("CREATE TABLE IF NOT EXISTS presist.audience_rating (logtime text,chnname text,albumid text,total text,chncount text,percent text,PRIMARY KEY (logtime,chnname));");
			LOG.info("create table audience_rating");
			session.execute("CREATE TABLE IF NOT EXISTS presist.user_count (chnname text,count counter,PRIMARY KEY(chnname));");
			LOG.info("create table user_count");
			session.execute("CREATE TABLE IF NOT EXISTS presist.user_info(mac text,chnname text,logtime text,pt text,PRIMARY KEY(mac,chnname,logtime));");
			LOG.info("create table user_info");
			session.execute("CREATE TABLE IF NOT EXISTS presist.play_record(mac text,begintime text,endtime text,PRIMARY KEY(mac));");
			LOG.info("create table play_record");

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub
		BrokerHosts brokerHosts = new ZkHosts(ZkUtils.ZKHOSTS);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "logtest-2",
				"/storm", "audience");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list", ZkUtils.BROKERLISTS);
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		conf.setNumWorkers(1);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt1", new PresistBolt(), 1).shuffleGrouping("spout");
		StormSubmitter.submitTopology("logto", conf, builder.createTopology());
	}

}