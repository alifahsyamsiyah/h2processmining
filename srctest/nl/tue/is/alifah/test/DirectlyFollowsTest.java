package nl.tue.is.alifah.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DirectlyFollowsTest {
	
	static Connection conn;
	static Statement stat;
	static ResultSet rs;
	static long startTime;
	static BufferedWriter out;
	
	public static void loadLog(String logName) throws SQLException, IOException{
		stat.execute("DROP TABLE IF EXISTS " + logName);
		
		startTimeMeasurement();		
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "'case' as CaseID,"
				+ "'event' as Activity,"
				+ "convert(parseDateTime(startTime,'yyyy/MM/dd HH:mm:ss.SSS'),TIMESTAMP) AS CompleteTimestamp"
				//+ "Variant,"
				//+ "VariantIndex "
				+ " FROM CSVREAD('C:/Users/asyamsiy/workspace/h2processmining-master/resources/"+logName+".csv', null, 'fieldSeparator=,');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");
		printTimeTaken();

		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+"Event_Log"+" a, "+"Event_Log"+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp"
				);
	}
	
	public static void startTimeMeasurement(){
		startTime = System.nanoTime();
	}
	
	public static void printTimeTaken() throws IOException{
		long endTime = System.nanoTime();
		double timeTaken =  (endTime - startTime)/1000000000.0; 
		System.out.println("\ttime taken: " + timeTaken + " sec.");
		//out.write("TIME: " + timeTaken + "\n");
	}
	
	public static void directlyFollowsOperator(String logName) throws SQLException, IOException {
		startTimeMeasurement();		
		rs = stat.executeQuery("SELECT * FROM DIRECTLYFOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		
		while(rs.next()) {
			//System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
			out.write(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\n");
		}
		printTimeTaken();
	}
	
	public static void sortedLogApproach() throws SQLException, IOException {
		startTimeMeasurement();
		rs = stat.executeQuery("select * "
				+ "from "+ "Event_Log"
				+ " order by CaseID asc, CompleteTimestamp asc");
		
		Map<String,Integer> follows = new HashMap<String,Integer>();
		
		String prevCase = "", prevAct = "";
		
		if(rs.next()) {
			prevCase = rs.getString(1);
			prevAct = rs.getString(2);
		}
		
		while(rs.next()) {
			String caseID = rs.getString(1);
			String activity = rs.getString(2);
			
			if(prevCase.equals(caseID)) {
				String key = prevAct + "---" + activity;
				Integer val = follows.get(key);
				
				Integer temp = (val == null) ? follows.put(key, 1) : follows.put(key, val + 1);
				
				prevAct = activity;
			} else {
				prevCase = caseID;
				prevAct = activity;
			}
			
		}
		
		for(String key : follows.keySet()) {
			String[] arr = key.split("---");
			String ec1 = arr[0]; 
			String ec2 = arr[1];
			Integer freq = follows.get(key);
			
			//System.out.println(ec1 + " " + ec2 + " " + freq);
			out.write(ec1 + "\t" + ec2 + "\t" + freq + "\n");			
		}
		printTimeTaken();
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		out = new BufferedWriter(new FileWriter("C:\\Users\\asyamsiy\\Documents\\Experiment\\h2\\result\\h2.txt", false));
		
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:tcp://ais-hadoop-1.win.tue.nl/~/test2;cache_size=60000000;multi_threaded=1", "sa", "");
		//conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "");
		//conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        
		stat = conn.createStatement();
		
		//loadLog("tess2");
		directlyFollowsOperator("t1500Ka100");
		//sortedLogApproach();
		
		out.close();
	}

}
