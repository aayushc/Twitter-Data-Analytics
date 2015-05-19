import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;


public class q4Frontend {

	private static List<Connection> connectionPool = new ArrayList<Connection>();
	
	public static String processresult(String hashtag, String start, String end) {
		StringBuilder response = new StringBuilder("TeamCloudCrackers,117333310089,020697801606,984101504455\n");
		String result= getAllId(hashtag);
		if(result==null) {
			return response.toString();
		}
		System.out.println("result:"+result);
		//ArrayList<String> list= new ArrayList<String>();
		
		//System.out.println("res:"+result);
		String[] splitted= result.split(";");
		for(int i=0;i<splitted.length;i++) {
			if(splitted[i]==null || splitted[i].isEmpty()) {
				continue;
			}
			String[] each= splitted[i].split(",");
			//System.out.println(each[2]);
			String date= each[2].split("\\+")[0];
			if(date.compareTo(start) <0) {
				continue;
			}
			if(date.compareTo(end)>0) {
				continue;
			}
			response.append(splitted[i]+"\n");
			//list.add(splitted[i]);
			
		}
		/*Collections.sort(list, new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				// TODO Auto-generated method stub
				String date1= s1.split(",")[2];
				String date2= s2.split(",")[2];
				
				return date1.split("\\+")[0].compareTo(date2.split("\\+")[0]);
			}
		});*/
		/*for(String s: list) {
			response.append(s+"\n");
		}*/
		return response.toString();
	}
	
	public static String getAllId(String hashtag) {
		System.out.println("in sql");
		Connection con=null;
		try {
			con= getConnection();
			System.out.println("connection made");
			PreparedStatement ps= con.prepareStatement("SELECT * FROM tweetq4 WHERE hashkey=?");
			ps.setString(1, hashtag);
			ResultSet rs= ps.executeQuery();
			String res="";
			while(rs.next()) {
				res=rs.getString("resultq4");
			}
			rs.close();
			ps.close();
			releaseConnection(con);
			return res;
		} catch (Exception e) {
            try { if (con != null) con.close(); } catch (SQLException e2) { /* ignore */ }
            return null;
        	//throw new MyDAOException(e);
		}
	}
	//Make connection to database
		private static synchronized Connection getConnection() throws MyDAOException {
			System.out.println("inside connection");
			if(connectionPool.size() > 0) {
				return connectionPool.remove(connectionPool.size()-1);
			}
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch(ClassNotFoundException e) {
				throw new MyDAOException(e);
			}
			
			try {
				String url="jdbc:mysql://ec2-52-4-40-218.compute-1.amazonaws.com/test";
				String username="user";
				String password="password";
				System.out.println("trying connection");
				return DriverManager.getConnection(url, username, password);
				//return DriverManager.getConnection(url);
			} catch (SQLException e) {
				throw new MyDAOException(e);
			}
		}
		
		
		//Release connection
		private static synchronized void releaseConnection(Connection con) {
			connectionPool.add(con);
		}
		
		public static void main(final String[] args) throws FileNotFoundException {
			File file = new File("out.txt");
			FileOutputStream fos = new FileOutputStream(file);
			PrintStream ps = new PrintStream(fos);
			System.setOut(ps);
	    	System.out.println("hello");
	        Undertow server = Undertow.builder().setWorkerThreads(30)
	                .addHttpListener(80,"ec2-52-4-40-218.compute-1.amazonaws.com")
	                .setHandler(new HttpHandler() {

						@Override
						public void handleRequest(final HttpServerExchange exchange) throws Exception {
							System.out.println("inside handle request");
							exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
							if(exchange.getRequestURI().equals("/index")) {
								exchange.getResponseSender().send("hello");
							}
							Map<String,Deque<String>>map=exchange.getQueryParameters();
							String userId=map.get("hashtag").getFirst();
							String start=map.get("start").getFirst();
							String end=map.get("end").getFirst();
							System.out.println(userId+" "+start+" "+end);
							String output=processresult(userId, start,end);
							System.out.println("out:"+output);
							exchange.getResponseSender().send(output);
							
	                        
						}
	                }).build();
	        server.start();
	    }
		
		/*public static void main(String[] args) {
			String res= processresult("android", "2014-03-21", "2014-05-16");
			System.out.println(res);
		}*/
		
}
