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
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class q5Frontend {

	private static List<Connection> connectionPool = new ArrayList<Connection>();
	
	private static String processRequest(String[] users,String start, String end) {
		System.out.println(start+" "+end);
		StringBuilder response = new StringBuilder("TeamCloudCrackers,117333310089,020697801606,984101504455\n");
		final HashMap<String, Long> map= new HashMap<String, Long>();
		ArrayList<String> al= new ArrayList<String>();
		for(String s:users) {
			
			String first=s+":"+start;
			String last=s+":"+end;
			System.out.println("users:"+first+" "+last);
			String res= getOutput(first,last);
			System.out.println("res:  "+res);
			String[] each= res.split(" ");
			Long followers= Long.parseLong(each[0]);
			Long frnds= Long.parseLong(each[1]);
			int tweets= Integer.parseInt(each[2]);
			Long points= tweets+3*frnds+5*followers;
			map.put(s, points);
			al.add(s);
		}
		Collections.sort(al, new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				// TODO Auto-generated method stub
				long diff= map.get(s2)-map.get(s1);
				if(diff!=0) return (int)diff;
				else {
					long diff1=Long.parseLong(s2)- Long.parseLong(s1);
					if(diff1>0) return 1;
					else return -1;
				}
			}
		});
		for(String s:al) {
			response.append(s+","+map.get(s)+"\n");
		}
		System.out.println("response:"+response.toString());
		return response.toString();
	}
	
	private static String getOutput(String start, String end) {
		Connection con=null;
		try {
			con= getConnection();
			System.out.println("inside getoutput");
			PreparedStatement ps= con.prepareStatement("SELECT MAX(follower) AS Followers, MAX(friend) AS Frnd, SUM(tweet) AS Tweet FROM q5 WHERE userId between ? AND ?");
			ps.setString(1, start);
			ps.setString(2, end);
			ResultSet rs= ps.executeQuery();
			String foll="";
			String frnd="";
			String tweet="";
			while(rs.next()) {
				System.out.println("inside result string");
				foll=rs.getString("Followers");
				frnd= rs.getString("Frnd");
				tweet= rs.getString("Tweet");
			}
			rs.close();
			ps.close();
			releaseConnection(con);
			return foll+" "+frnd+" "+tweet;
		}catch (Exception e) {
            try { if (con != null) con.close(); } catch (SQLException e2) { /* ignore */ }
            return null;
        	//throw new MyDAOException(e);
		}
	}
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
						String userIds=map.get("userlist").getFirst();
						String start=map.get("start").getFirst();
						String end=map.get("end").getFirst();
						String[] each= userIds.split(",");
						System.out.println(start+" "+end);
						String output=processRequest(each, start,end);
						System.out.println("out:"+output);
						exchange.getResponseSender().send(output);
						
                        
					}
                }).build();
        server.start();
    }
}
