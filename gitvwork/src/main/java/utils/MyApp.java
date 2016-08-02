package utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MyApp {
	
	private MyApp(){
	}
	
	@SuppressWarnings("unchecked")
	public static void parseJson(String json){
//		Object obj = JSONValue.parse(json);
		JSONObject obj = (JSONObject) JSONValue.parse(json);
		ArrayList<String> al = null;
		
		for(Object key : obj.keySet()){
			al = (ArrayList<String>)obj.get(key);
			int count = Integer.parseInt(al.get(0));
			String albumId = al.get(1);
			
		}
		
//		JSONArray array = (JSONArray)obj;
		System.out.println(obj.toString());
//		System.out.println(array.toJSONString());
//		for(int i=0;i<array.size();i++){
//			for(String str :(String[])array.get(i)){
//				System.out.print(str+"\t");
//			}
//			System.out.println();
//		}
	}
	
	public static String makeJson(){
		LinkedHashMap<String,List<String>> obj = new LinkedHashMap<>();
		ArrayList<String> al = new ArrayList<>();
		al.add("123");
		al.add("无敌");
		obj.put("CCTV1", al);
		ArrayList<String> al2 = new ArrayList<>();
		al2.add("1234");
		al2.add("牛逼");
		obj.put("CCTV2", al2);
		return JSONValue.toJSONString(obj);
		
	}
	
	public static void main(String[] args) {
		String json = "{\"CCTV1\":[\"123\",\"成人影视\"],\"CCTV2\":[\"1234\",\"综艺节目\"]}";
//		;
		parseJson(json);
	}
	
}
