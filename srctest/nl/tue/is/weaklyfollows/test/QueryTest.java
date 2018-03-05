package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class QueryTest {
	long startTime;
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}

	public static void main(String[] args) throws Exception{
	}
	
	public void test(Connection conn, String tableName) throws ClassNotFoundException, SQLException {
		SandboxWeaklyFollows test = new SandboxWeaklyFollows();
		
		Class.forName("org.h2.Driver");
		Statement stat = conn.createStatement();
		ResultSet rs;

		//Create the event log
		/*stat.execute("CREATE TABLE Event_Log(CaseID INT, Event VARCHAR(100), CompleteTimestamp TIME)");
		
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:22:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'B','00:23:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:24:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:25:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'C','00:26:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'A','00:26:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'C','00:27:00')");*/
		
		rs = stat.executeQuery("select * "
				+ "from "+ tableName
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
			
			System.out.println(ec1 + " " + ec2 + " " + freq);
			
		}
		
	}
}
