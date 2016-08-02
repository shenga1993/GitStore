package audienceRatingDao;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;

public class OperateRedisBolt extends AbstractRedisBolt {
	
	private final static Logger LOG = LoggerFactory.getLogger(OperateRedisBolt.class);
	private final static String TOTAL = "total";

	public OperateRedisBolt(JedisClusterConfig config) {
		super(config);
	}

	public OperateRedisBolt(JedisPoolConfig config) {
		super(config);
	}

	@Override
	public void execute(Tuple input) {
		// TODO 自动生成的方法存根
		String log = input.getString(0);
		String[] columns = log.split("\\|");
		JedisCommands jedis = null;
		try {
			if (columns.length == 8 && columns[0].toUpperCase().equals("LIVOD")
					&& columns[1].equals("5")) {
				jedis = getInstance();
				String vodtype = columns[0];
				String action = columns[1];
				String mac = columns[2];
				String albumId = columns[7];
				String chnName = columns[5];
				String pl = columns[3];
				String pt = columns[4];
				String userTs = columns[6];
				if (pt.equals("0")) {
					jedis.incr(TOTAL);
					jedis.incr(chnName);
					collector.emit(new Values(albumId, chnName, userTs));

				} else if (pt.equals("1") || pt.equals("2")) {
					jedis.decr(TOTAL);
					jedis.decr(chnName);
					collector.emit(new Values(albumId, chnName, userTs));
				}
				collector.ack(input);
			}
		} catch (Exception e) {
			this.collector.reportError(e);
			this.collector.fail(input);
			LOG.error(e.getMessage());
		} finally {
			if(jedis!=null){
				returnInstance(jedis);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO 自动生成的方法存根
		declarer.declare(new Fields("albumId", "chnName", "userTs"));
	}

}
