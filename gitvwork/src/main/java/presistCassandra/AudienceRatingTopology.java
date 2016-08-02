package presistCassandra;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import utils.MyComparator;
import utils.ZkUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class AudienceRatingTopology {

	private final static String TOTAL = "total";
	private static final Logger LOG = LoggerFactory
			.getLogger(AudienceRatingTopology.class);

	public static class OperateRedisBolt extends AbstractRedisBolt {

		private final HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		private final HashMap<String, String> info = new HashMap<String, String>();
		private TreeSet<String> tsSet ;
		private final static Logger LOG = LoggerFactory
				.getLogger(OperateRedisBolt.class);

		public OperateRedisBolt(JedisClusterConfig config) {
			super(config);
		}

		public OperateRedisBolt(JedisPoolConfig config) {
			super(config);
		}
		
		@Override
		public void prepare(Map map, TopologyContext topologyContext,
				OutputCollector collector) {
			// TODO 自动生成的方法存根
			super.prepare(map, topologyContext, collector);
			tsSet = new TreeSet<>(new MyComparator());
		}

		@SuppressWarnings("unchecked")
		@Override
		public void execute(Tuple input) {
			// TODO 自动生成的方法存根
			if (TupleUtils.isTick(input)) {
				JedisCommands jedis = null;
				try {
					jedis = getInstance();
					String value = jedis.get("All_json");
					JSONObject obj = (JSONObject) JSONValue.parse(value);
					LinkedHashMap<String, ArrayList<String>> obj2 = new LinkedHashMap<>();
					int total = hmap.get(TOTAL);
					for (String key : hmap.keySet()) {
						if (key.equals(TOTAL)) {
							continue;
						}
						if (obj.containsKey(key)) {
							// jedis.set(key, Integer.parseInt(obj.get(key))
							// + hmap.get(key) + "");
							ArrayList<String> al = (ArrayList<String>) obj
									.get(key);
							int count = Integer.parseInt(al.get(0))
									+ hmap.get(key);
							al.clear();
							al.add(count + "");
							al.add(info.get(key));
							al.add(tsSet.first());
							al.add(total + "");
							obj2.put(key, al);
						} else {
							ArrayList<String> al = new ArrayList<String>();
							int count = hmap.get(key);
							al.add(count + ""); // index 0 每个频道总人数
							al.add(info.get(key)); // index 1 栏目名
							al.add(tsSet.first()); // index 2 最新时间戳
							al.add(total + ""); // index 3 所有频道总人数
							obj2.put(key, al);
							// jedis.set(key, hmap.get(key) + "");
						}
					}
					jedis.set("All_json", JSONValue.toJSONString(obj2));
					collector.emit(new Values(JSONValue.toJSONString(obj2)));
					hmap.clear();
				} finally {
					if (jedis != null) {
						returnInstance(jedis);
					}
				}
			} else {

				String log = input.getString(0);
				String[] columns = log.split("\\|");
				// JedisCommands jedis = null;
				try {
					if (columns.length == 8
							&& columns[0].toUpperCase().equals("LIVOD")
							&& columns[1].equals("5")) {

						// jedis = getInstance();
						String albumId = columns[7];
						String chnName = columns[5];
						String pt = columns[4];
						String userTs = columns[6];
						if (pt.equals("0")) {
							// jedis.incr(TOTAL);
							// jedis.incr(chnName);
							if (hmap.containsKey(TOTAL)) {
								hmap.put(TOTAL, hmap.get(TOTAL) + 1);
							} else {
								hmap.put(TOTAL, 1);
							}

							if (hmap.containsKey(chnName)) {
								hmap.put(chnName, hmap.get(chnName) + 1);
							} else {
								hmap.put(chnName, 1);
							}
							info.put(chnName, albumId);
							tsSet.add(userTs);
							if (tsSet.size() > 1) {
								tsSet.remove(tsSet.last());
							}
							// collector
							// .emit(new Values(albumId, chnName, userTs));

						} else if (pt.equals("1") || pt.equals("2")) {
							// jedis.decr(TOTAL);
							// jedis.decr(chnName);
							if (hmap.containsKey(TOTAL)) {
								hmap.put(TOTAL, hmap.get(TOTAL) - 1);
							} else {
								hmap.put(TOTAL, -1);
							}

							if (hmap.containsKey(chnName)) {
								hmap.put(chnName, hmap.get(chnName) - 1);
							} else {
								hmap.put(chnName, -1);
							}
							info.put(chnName, albumId);
							if (tsSet.size() > 1) {
								tsSet.remove(tsSet.last());
							}
							// collector
							// .emit(new Values(albumId, chnName, userTs));
						}
						collector.ack(input);
					}
				} catch (Exception e) {
					this.collector.reportError(e);
					this.collector.fail(input);
					LOG.error(e.getMessage());
				}
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO 自动生成的方法存根
			declarer.declare(new Fields("json"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO 自动生成的方法存根
			Map<String, Object> conf = new HashMap<>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
			return conf;
		}
	}

	public static class AudienceRatingBolt extends AbstractRedisBolt {

		private static final Logger LOG = LoggerFactory
				.getLogger(AudienceRatingBolt.class);
		private AudienceRatingTopology art;

		public AudienceRatingBolt(JedisClusterConfig config) {
			super(config);
		}

		public AudienceRatingBolt(JedisPoolConfig config) {
			super(config);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			JedisCommands jedis = null;
			Session session = null;
			try {
				art = new AudienceRatingTopology();
				art.connect(CONTACT_POINTS, 9042);
				session = art.getSession();
				String json = input.getStringByField("json");
				// String albumId = input.getStringByField("albumId");
				JSONObject obj = (JSONObject) JSONValue.parse(json);
				Set<String> chnSet = obj.keySet(); // 所有的 频道名
				for (String chnName : chnSet) {
					ArrayList<String> al = (ArrayList) obj.get(chnName);// 频道名下的所有信息
					String albumId = al.get(1);
					int chnCount = Integer.parseInt(al.get(0));
					String userTs = al.get(2);
					int total = Integer.parseInt(al.get(3));
					double audienceRatingPercent = (double) chnCount
							/ (double) total * 100;
					BigDecimal bigDecimal = new BigDecimal(
							audienceRatingPercent);
					audienceRatingPercent = bigDecimal.setScale(2,
							BigDecimal.ROUND_HALF_UP).doubleValue();
					LinkedHashMap<String, String> lhmap = new LinkedHashMap<>();
					lhmap.put("userTs", userTs);
					lhmap.put("chnName", chnName);
					lhmap.put("albumId", albumId);
					lhmap.put("audienceRatingPercent", audienceRatingPercent
							+ "%");
					lhmap.put("chnCount", chnCount + "");
					lhmap.put("total", total + "");
					session.execute("INSERT INTO yesheng.audience (userTs,chnName,albumId,json) VALUES ( "
							+ userTs
							+ " , "
							+ chnName
							+ " , "
							+ albumId
							+ " , " + JSONValue.toJSONString(lhmap) + " );");
					collector.ack(input);
					// collector.emit(new Values(userTs, chnName, albumId,
					// jsonObject.toJSONString()));
				}
				// String chnName = input.getStringByField("chnName");
				// String userTs = input.getStringByField("userTs");
				// jedis = getInstance();
				// String totalStr = jedis.get(TOTAL);
				// String all_json = jedis.get("All_json");

				// if (totalStr != null && chncountStr != null) {
				// Integer total = Integer.parseInt(totalStr);
				// Integer chncount = Integer.parseInt(chncountStr);
				// LOG.info("deBug" + total + "\t" + chncount);
				// double audienceRatingPercent = (double) chncount
				// / (double) total;
				// BigDecimal bigDecimal = new BigDecimal(
				// audienceRatingPercent);
				// audienceRatingPercent = bigDecimal.setScale(2,
				// BigDecimal.ROUND_HALF_UP).doubleValue();
				// JSONObject jsonObject = new JSONObject();
				// jsonObject.put("userTs", userTs);
				// jsonObject.put("chnName", chnName);
				// jsonObject.put("albumId", albumId);
				// jsonObject.put("audienceRatingPercent",
				// audienceRatingPercent);
				// jsonObject.put("chnCount", chncount);
				// jsonObject.put("total", total);
				// // jedis.hset(userTs, chnName, jsonObject.toJSONString());
				// // jedis.expire(userTs, 50);
				// art.connect(CONTACT_POINTS, 9042);
				// session = art.getSession();
				// session.execute("INSERT INTO yesheng.audience (userTs,chnName,albumId,json) VALUES ( "
				// + userTs
				// + " , "
				// + chnName
				// + " , "
				// + albumId
				// + " , " + jsonObject.toJSONString() + " );");
				// collector.ack(input);
				// collector.emit(new Values(userTs, chnName, albumId,
				// jsonObject.toJSONString()));
				// }
				// collector.ack(input);
			} catch (Exception e) {
				this.collector.fail(input);
				LOG.error("error" + "\tyyyyssss\t" + e.getMessage());
			} finally {
				if (session != null) {
					session.close();
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			// declarer.declare(new Fields("userTs", "chnName", "albumId",
			// "json"));
		}

	}

	// public static class PersistBolt extends BaseRichBolt {
	//
	// private OutputCollector collector;
	// private final AudienceRatingTopology art = new AudienceRatingTopology();
	//
	// @Override
	// public void prepare(Map stormConf, TopologyContext context,
	// OutputCollector collector) {
	// // TODO 自动生成的方法存根
	// this.collector = collector;
	// }
	//
	// @Override
	// public void execute(Tuple input) {
	// // TODO 自动生成的方法存根
	// String userTs = input.getStringByField("userTs");
	// String chnName = input.getStringByField("chnName");
	// String albumId = input.getStringByField("albumId");
	// String json = input.getStringByField("json");
	// Session session = null;
	// try {
	// art.connect(CONTACT_POINTS, 9042);
	// session = art.getSession();
	// session.execute("INSERT INTO yesheng.audience (userTs,chnName,albumId,json) VALUES ( "
	// + userTs
	// + " , "
	// + chnName
	// + " , "
	// + albumId
	// + " , "
	// + json + " );");
	// session.close();
	// collector.ack(input);
	// } catch (Exception e) {
	// LOG.error(e.getMessage());
	// collector.reportError(e);
	// collector.fail(input);
	// } finally {
	// if (session != null) {
	// session.close();
	// }
	// }
	// }
	//
	// @Override
	// public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// // TODO 自动生成的方法存根
	// }
	//
	// }

	private Cluster cluster;
	private Session session;
	private static final String[] CONTACT_POINTS = { "10.10.121.119",
			"10.10.121.120", "10.10.121.121", "10.10.121.122" };

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub

		AudienceRatingTopology art = new AudienceRatingTopology();
		art.connect(CONTACT_POINTS, 9042);
		Cluster cluster = art.getCluster();
		Metadata metadata = cluster.getMetadata();
		if (!metadata.getKeyspaces().contains("yesheng")) {
			art.createKeyspaceAndTable("yesheng", "audience");
		} else {
			if (!metadata.getKeyspace("audience").getTables()
					.contains("audience")) {
				art.createTable("yesheng", "audience");
			}
		}
		art.close();
		BrokerHosts brokerHosts = new ZkHosts(ZkUtils.ZKHOSTS);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "logtest-1",
				"/brokers", "spout");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list", ZkUtils.BROKERLISTS);
		map.put("serializer.class", ZkUtils.SERIALIZERCLASS);
		conf.put("kafka.broker.properties", map);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		conf.setNumWorkers(3);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 3);
		JedisPoolConfig config = new JedisPoolConfig.Builder()
				.setHost("10.10.121.123").setPort(6379).build();
		builder.setBolt("bolt1", new OperateRedisBolt(config), 1)
				.shuffleGrouping("spout");
		builder.setBolt("bolt2", new AudienceRatingBolt(config), 1)
				.shuffleGrouping("bolt1");
		StormSubmitter.submitTopology("logto", conf, builder.createTopology());
	}

	public void connect(String[] contactPoints, int port) {

		cluster = Cluster.builder().addContactPoints(contactPoints)
				.withPort(port).build();

		LOG.info("Connected to cluster: %s%n", cluster.getMetadata()
				.getClusterName());

		session = cluster.connect();
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Session getSession() {
		return session;
	}

	public void createKeyspaceAndTable(String keyspace, String tablename) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS "
				+ keyspace
				+ " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");

		session.execute("CREATE TABLE IF NOT EXISTS "
				+ keyspace
				+ "."
				+ tablename
				+ " (userTs text,chnName text,albumId text,jsonValue text,PRIMARY KEY(userTs,chnName,albumId));");
	}

	public void createTable(String keyspace, String tablename) {
		session.execute("CREATE TABLE IF NOT EXISTS "
				+ keyspace
				+ "."
				+ tablename
				+ " (userTs text,chnName text,albumId text,jsonValue text,PRIMARY KEY(userTs,chnName,albumId));");
	}

	public void close() {
		session.close();
		cluster.close();
	}

}
