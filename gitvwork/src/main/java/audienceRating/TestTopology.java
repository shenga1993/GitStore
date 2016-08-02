package audienceRating;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTopology {

	
	public static class MyBolt extends BaseRichBolt {
		
		private static final Logger LOG = LoggerFactory.getLogger(MyBolt.class);
		private OutputCollector collector;
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO 自动生成的方法存根
			this.collector = collector;
			LOG.info("prepare");
		}

		@Override
		public void execute(Tuple input) {
			// TODO 自动生成的方法存根
			collector.ack(input);
			LOG.info("execute");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO 自动生成的方法存根
			
		}

	}

	public static class MySpout extends BaseRichSpout {
		
		private SpoutOutputCollector collector;
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO 自动生成的方法存根
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			// TODO 自动生成的方法存根
			collector.emit(new Values("123"));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO 自动生成的方法存根
			declarer.declare(new Fields("msg"));
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// TODO 自动生成的方法存根
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		conf.setNumWorkers(3);
		builder.setSpout("spout", new MySpout());
		builder.setBolt("bolt", new MyBolt()).shuffleGrouping("spout");
		StormSubmitter.submitTopology("pretest", conf, builder.createTopology());
	}

}
