package audienceRating;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import utils.ZkUtils;

public class LogProducer implements Runnable {

	private final KafkaProducer<String, String> producer;
	private final String topic;
	private static final DecimalFormat DATE_FORMAT = new DecimalFormat("#00");
	private static final String YEAR = "2015";
	private static final String MONTH = "11";
	private static final String DAY = "27";
	private static final int[] HOURS = { 9, 10, 11 };
	public static final Random RDOM = new Random();
	public static final String[] A = { "VOD", "LIVOD" };
	public static final String[] CHANNEL = { "CCTV-5", "CCTV-1" , "CCTV-2", "CCTV-3", "CCTV-4", "CCTV-6", "CCTV-7", "CCTV-8", "CCTV-9"};
	public static final String[] AN = { "军事节目", "美食节目", "综艺节目", "新闻在线", "幽默娱乐", "时事政治", "非常时刻", "相亲节目", "明星采访" };
	private static final String[] AP_RIGHT = { "7Daysinn", "CMCC-EASY", "CMCC", "CMCC-EDU" };


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	private String make_reportTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(System.currentTimeMillis()));
	}

	public LogProducer(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers",ZkUtils.BROKERLISTS);
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<>(props);
		this.topic = topic;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			StringBuilder sb = new StringBuilder();
			String type = A[RDOM.nextInt(A.length)];
			String A = 5+"";
			String mac = make_apmac();
			String pl = RDOM.nextInt(100) + "";
			String pt = (RDOM.nextInt(1)) + "";
			String channel = CHANNEL[RDOM.nextInt(CHANNEL.length)];
			String ts =make_reportTime();
			String an = AN[RDOM.nextInt(AN.length)];
			sb.append(type).append("|").append(A).append("|").append(mac).append("|").append(pl).append("|").append(pt)
					.append("|").append(channel).append("|").append(ts).append("|").append(an);
//			System.out.println(sb.toString());
			System.out.println(sb.toString());
			producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
		}
	}
	
	private String make_apmac() {
		/* 用来产生:左边的随机数列 */
		StringBuilder left = new StringBuilder();
		for (int i = 0; i < 6; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < 2; j++) {
				int choose = RDOM.nextInt(2);
				switch (choose) {
				case 0:
					int fromchar = 'a';
					int endchar = 'z';
					int need = 0;
					while (need < fromchar) {
						need = RDOM.nextInt(endchar);
					}
					char f = (char) need;
					sb.append(f + "");
					break;

				case 1:
					int need1 = RDOM.nextInt(10);
					sb.append(need1 + "");
					break;
				}
			}
			left.append(sb.toString() + "-");
		}
		/* 用来产生:右边的随机数列 */
		String right = AP_RIGHT[RDOM.nextInt(3)];
		return left.deleteCharAt(left.length() - 1).toString() + ":" + right;

	}
}
