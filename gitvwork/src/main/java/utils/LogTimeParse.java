package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogTimeParse {
	private LogTimeParse(){
		
	}
	
	public static long getParseTime(String logTime){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date date = sdf.parse(logTime);
			return date.getTime();
		} catch (ParseException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		return 0;
	}
	
}
