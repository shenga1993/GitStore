package audienceRating;

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
import utils.PercentUtils;
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
						long chnCount = 0;
						long total = 0;
						if (pt.equals("0")) {
							// pt=0时存储mac的记录到cassandra
							LOG.info("save record: mac-->" + mac + "chnname-->"
									+ chnName + "logtime-->" + logTime
									+ "pt-->" + pt);
							CassandraDao.insert(session, "presist",
									"user_info", new String[] { "mac",
											"chnname", "logtime", "pt" },
									new Object[] { mac, chnName, logTime, pt });
							// pt=0时total自增
							CassandraDao
									.update_user_count(session, TOTAL, true);
							// pt=0时chnName自增
							CassandraDao.update_user_count(session, chnName,
									true);
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
							// pt=1或者2时 total 自减
							CassandraDao.update_user_count(session, TOTAL,
									false);
							// pt=1或者2时 chnName 自减
							CassandraDao.update_user_count(session, chnName,
									false);
						}
						// 查出当前的total总数
						ResultSet result = CassandraDao.select_user_count(
								session, TOTAL);
						for (Row row : result) {
							total = row.getLong("count");
						}
						// 查出当前的chnCount总数
						result = CassandraDao.select_user_count(session,
								chnName);
						for (Row row : result) {
							chnCount = row.getLong("count");
						}
						double audienceRatingPercent = 0;
						if (total != 0) {
							audienceRatingPercent = (Double
									.parseDouble(chnCount + "") / Double
									.parseDouble(total + "")) * 100;
						}
						LOG.info(logTime + " : " + chnName
								+ "------>audienceRatingPercent ："
								+ audienceRatingPercent + "%");
						audienceRatingPercent = PercentUtils.percent(audienceRatingPercent);
						long time = LogTimeParse.getParseTime(logTime);
						CassandraDao.insert(session, "presist",
								"audience_rating", new String[] { "logtime",
										"chnname", "albumid", "total",
										"chncount", "percent" }, new Object[] {
										time+"", chnName, albumId, total +"",
										chnCount +"",
										audienceRatingPercent + "%" });

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