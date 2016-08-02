package utils;

import java.util.Comparator;

public class MyComparator implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		// TODO 自动生成的方法存根
		return o2.hashCode() - o1.hashCode();
	}

}
