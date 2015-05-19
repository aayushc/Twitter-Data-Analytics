import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


public class HashReducer {

	public static void main(String[] args) throws FileNotFoundException {
		FileReader file1= new FileReader(new File("C:/Users/aayush/Desktop/input1.txt"));
		 BufferedReader file = new BufferedReader(file1);//new InputStreamReader(System.in));
		
		String line = "";
		String key="";
		String prevKey="";
		StringBuilder consolidated=new StringBuilder();
		try {
			while ((line = file.readLine()) != null) {

				if (line.trim().isEmpty())
					continue;
				String[] each=line.split("\t");
				key= each[0];
				if(key.equals(prevKey)) {
					consolidated.append(each[1]);
				} else {
					if(!prevKey.isEmpty()) {
						StringBuilder st=sort(consolidated);
						System.out.println(prevKey+"\t"+st.toString());
					}
					consolidated= new StringBuilder();
					consolidated.append(each[1]);
					prevKey = key;
					
				}
				
			}
			if(key.equals(prevKey)) {
				StringBuilder st=sort(consolidated);
				System.out.println(prevKey+"\t"+st.toString());
			} else {
				StringBuilder st=sort(consolidated);
				System.out.println(key+"\t"+st.toString());
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static StringBuilder sort(StringBuilder consolidated) {
		ArrayList<String> list= new ArrayList<String>();
		String[] splitted= consolidated.toString().split(";");
		for(String s: splitted) {
			list.add(s);
		}
		Collections.sort(list, new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				// TODO Auto-generated method stub
				String[] check1= s1.split(",");
				String[] check2=s2.split(",");
				
				return check1[0].compareTo(check2[0]);
			}
		});
		StringBuilder st= new StringBuilder();
		for(String s:list) {
			st.append(s+";");
		}
		return st;
	}
}
