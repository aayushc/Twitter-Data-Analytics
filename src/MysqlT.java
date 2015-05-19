
import io.undertow.Undertow;
import io.undertow.io.Sender;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 *
 * @author CloudCrackers
 */
public class MysqlT {
	static String x = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";

	/**
	 * .
	 *
	 * @param encoded
	 *            string which will be decoded by this function
	 * @param key
	 *            String coming from request
	 * @return decoded string
	 */
	private static String deCode(String encoded, String key) {

		int n = (int) Math.sqrt(encoded.length());
		String decoded = "";
		char[][] arr = new char[n][n];

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				arr[i][j] = encoded.charAt(n * i + j);
			}
		}
		int m = 0;

		while (m <= encoded.length()) {
			for (int j = 0 + m; j < n - m; j++) {
				decoded = decoded + arr[m][j];
			}
			for (int i = 1 + m; i < n - m; i++) {
				decoded = decoded + arr[i][n - 1 - m];
			}
			for (int j = n - 2 - m; j >= 0 + m; j--) {
				decoded = decoded + arr[n - 1 - m][j];
			}
			for (int i = n - 2 - m; i >= 1 + m; i--) {
				decoded = decoded + arr[i][m];
			}
			m = m + 1;
		}

		// calculate the minikey z
		BigInteger bigIn = new BigInteger(key);
		int y = bigIn.divide(new BigInteger(x)).intValue();
		int z = y % 25 + 1;

		String result = "";
		for (int i = 0; i < decoded.length(); i++) {
			int minus = decoded.charAt(i) - z;
			if (minus < 65) {
				int check = decoded.charAt(i) - 65;
				check = z - check;
				minus = 91 - check;
			}
			char intermediate = (char) minus;
			result = result + intermediate;
		}
		Date date = new Date();
		SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String todayDate = formatDate.format(date);
		String response = "TeamCloudCrackers,117333310089,020697801606,984101504455\n" + todayDate + "\n" + result
				+ "\n";

		return response;
	}

	/**
	 * .
	 *
	 * @param encoded
	 *            string which will be decoded by this function
	 * @param key
	 *            String coming from request
	 * @return response of the request URL
	 */
	public static String processRequest(String encoded, String key) {
		String result = deCode(encoded, key);
		String response = "";
		if (result == null || result.equals("")) {
			response = "Invalid Message";
		}

		response = result;
		return response;

	}

	public static void main(final String[] args) throws FileNotFoundException {
		File file = new File("out.txt");
		FileOutputStream fos = new FileOutputStream(file);
		PrintStream ps = new PrintStream(fos);
		System.setOut(ps);
    	System.out.println("hello");
		final MysqlT t=new MysqlT();
		Undertow server = Undertow.builder().addHttpListener(80, "ec2-52-4-164-136.compute-1.amazonaws.com").setHandler(new HttpHandler() {

			@Override
			public void handleRequest(final HttpServerExchange exchange) throws Exception {
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
				Map<String, Deque<String>> map = exchange.getQueryParameters();
				String uri = exchange.getRequestURI();
				Sender sender = exchange.getResponseSender();
				if (uri.equals("/q1")) {
					String key = map.get("key").getFirst();
					String message = map.get("message").getFirst();
					
					String output = processRequest(message, key);
					exchange.getResponseSender().send(output);
				}
				if (uri.equals("/q3")) {
					String id = map.get("userid").getFirst();
					long t1=System.currentTimeMillis();
					System.out.println();
					String result = t.processRequest(id);
					System.out.println(System.currentTimeMillis()-t1);
//					response.put("name", result);
//					String content = "returnRes(" + mapper.writeValueAsString(response) + ")";
					sender.send(result);
				}

			}
		}).build();
		server.start();
	}
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://ec2-52-4-164-136.compute-1.amazonaws.com/test";

//	static final String USER = "amey";
//	static final String PASS = "anuragnagar";
	static java.sql.Connection conn = null;
	PreparedStatement stmt = null;
	private static List<Connection> connectionPool = new ArrayList<Connection>();

	private static synchronized Connection getConnection() throws MyDAOException {
		if(connectionPool.size() > 0) {
			return connectionPool.remove(connectionPool.size()-1);
		}
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch(ClassNotFoundException e) {
			throw new MyDAOException(e);
		}
		
		try {
			String url="jdbc:mysql://ec2-52-4-164-136.compute-1.amazonaws.com/test";
			String username="user";
			String password="password";
			return DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			throw new MyDAOException(e);
		}
	}

	public String processRequest(String id) {
		conn = null;
		String result = "";
		try {
			conn=getConnection();
			PreparedStatement ps = conn.prepareStatement("select results from q3 where id=? ");
			ps.setString(1, id);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getString("results");
				long ts=System.currentTimeMillis();
				
				//result=StringEscapeUtils.escapeJava(result);
				
				//System.out.println("name retrieved " + result);
				//result=StringEscapeUtils.unescapeJava(result);
				System.out.println("time for excaping"+( System.currentTimeMillis()-ts));
			} else {
				return "unauthorized";
			}
			rs.close();
			ps.close();		
			releaseConnection(conn);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (MyDAOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return "Team,1234-5678-1234,1234-5678-1234,1234-5678-1234\n"+result;
	}
	private static synchronized void releaseConnection(Connection con) {
		connectionPool.add(con);
	}
}
