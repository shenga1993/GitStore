package audienceRatingDao;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import redis.clients.jedis.Jedis;

public class RealTimeBolt extends BaseRichBolt {
	
	private static final String TOTAL = "total";
	private OutputCollector collector;
	private Jedis jedis;

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String log = input.getString(0);
		String[] columns = log.split("\\|");
		if (columns.length == 8 && columns[0].toUpperCase().equals("LIVOD")
				&& columns[1].equals("5")) {
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
				collector.ack(input);

			} else if (pt.equals("1") || pt.equals("2")) {
				jedis.decr(TOTAL);
				jedis.decr(chnName);
				collector.emit(new Values(albumId, chnName, userTs));
				collector.ack(input);
			}
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		jedis = new Jedis("10.10.121.119");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("albumId", "chnName", "userTs"));
	}

}
