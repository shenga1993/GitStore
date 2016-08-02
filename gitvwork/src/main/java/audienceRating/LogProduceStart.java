package audienceRating;

public class LogProduceStart {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LogProducer producer = new LogProducer("logtest-2");
		new Thread(producer).start();
	}

}
