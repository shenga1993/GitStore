package utils;

public interface ZkUtils {
	public final static String ZKHOSTS = "10.10.121.119:2181,"
			+ "10.10.121.120:2181," + "10.10.121.121:2181,"
			+ "10.10.121.122:2181," + "10.10.121.123:2181";

	public final static String BROKERLISTS = "10.10.121.121:9092,"
			+ "10.10.121.122:9092," + "10.10.121.120:9092";
	
	public final static String SERIALIZERCLASS = "kafka.serializer.StringEncoder";
	
	
}
