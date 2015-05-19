import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class HashMapper {

	public static Map<String, ArrayList<String>> map= new HashMap<String, ArrayList<String>>();
	public static void main(String[] args) throws IOException, ParseException {
		FileReader file= new FileReader(new File("C:/Users/aayush/Desktop/input.txt"));
		BufferedReader bfr= new BufferedReader(file);//new InputStreamReader(System.in));
		String line="";
		while((line=bfr.readLine())!=null) {
			
			if(line==null || line.isEmpty()) {
				continue;
			}
			
			JSONParser parser= new JSONParser();
			JSONObject obj=(JSONObject) parser.parse(line);
			
			JSONObject entities= (JSONObject)obj.get("entities");
			
			JSONArray hashtags= (JSONArray)entities.get("hashtags");
			if(hashtags.isEmpty()) {
				continue;
			}
			String tweetId= (String)obj.get("id_str");
			JSONObject user = (JSONObject) obj.get("user");
			if (user == null)
				continue;
			
			String userId = (String) user.get("id_str");
			
			String createdAt = (String) obj.get("created_at");
			if (createdAt == null || createdAt.isEmpty())
				continue;
			String created = getDate(createdAt);
			String res=tweetId+","+userId+","+created;
			for(int i=0;i<hashtags.size();i++) {
				JSONObject each=(JSONObject)hashtags.get(i);
				String tag=(String)each.get("text");
				if(tag.matches("[a-zA-Z0-9]+")) {
					if(map.containsKey(tag)) {
						ArrayList<String> al= map.get(tag);
						al.add(res);
					} else {
						ArrayList<String> al= new ArrayList<String>();
						al.add(res);
						map.put(tag, al);
					}
				}
			}
		}
		bfr.close();
		for(String s: map.keySet()) {
			/*Collections.sort(map.get(s), new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					String[] check1= s1.split(",");
					String[] check2=s2.split(",");
					
					return check1[0].compareTo(check2[0]);
				}
			});*/
			StringBuilder result=new StringBuilder();
			for(String st: map.get(s)) {
				String res= st+";";
				result.append(res);
			}
			System.out.println(s+"\t"+result.toString());
		}
	}
	
	private static String getDate(String date) {
		Date d=null;
		final String format="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
		SimpleDateFormat sdf= new SimpleDateFormat(format);
		try {
			d= sdf.parse(date);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			System.out.println(System.err);
		}
		SimpleDateFormat sf= new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
		String res= sf.format(d);
		return res;
	}
}
