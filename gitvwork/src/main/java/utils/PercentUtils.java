package utils;

import java.math.BigDecimal;

public class PercentUtils {
	private PercentUtils() {

	}

	public static double percent(double value) {
		BigDecimal bigDecimal = new BigDecimal(value);
		return bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
}
