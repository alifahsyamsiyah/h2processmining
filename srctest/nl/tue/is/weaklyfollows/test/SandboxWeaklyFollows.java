package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.table.Table;


public class SandboxWeaklyFollows {
	long startTime;
	static Statement stat;
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}
	
	public static void loadLog(String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd HH:mm:ss.SSS'),TIMESTAMP) AS CompleteTimestamp"
				//+ "Variant,"
				//+ "VariantIndex "
				+ " FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=,');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");
	}

	public static void main(String[] args) throws Exception{
		String logName = "t100000a23";
		SandboxWeaklyFollows test = new SandboxWeaklyFollows();
		
		Class.forName("org.h2.Driver");
        //Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
		//Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "");
		//Connection conn = DriverManager.getConnection("jdbc:h2:tcp://131.155.239.220:9092/~/test", "sa", "");
		//Connection conn = DriverManager.getConnection("jdbc:h2:tcp://ais-hadoop-1.win.tue.nl/~/test", "sa", "");
        
		stat = conn.createStatement();
		ResultSet rs;
		
		loadLog(logName);
		
		/*rs = stat.executeQuery("select * from directlyfollows(select * from event_log)");
		while(rs.next()) {
			System.out.println(rs.getString(1));
		}*/		

		//Create the event log
		/*stat.execute("CREATE TABLE Event_Log(Case_Id INT, Event VARCHAR(100), End_Time TIMESTAMP)");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A',convert('2017/01/10 01:22:00.320', timestamp))");
		stat.execute("INSERT INTO Event_Log VALUES (1,'B',convert('2017/01/10 02:08:00.123', timestamp))");
		stat.execute("INSERT INTO Event_Log VALUES (1,'E',convert('2017/01/10 02:32:00.121', timestamp))");*/
		/*stat.execute("INSERT INTO Event_Log VALUES (2,'A','02:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'D','03:19:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'E','05:07:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'A','02:29:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'D','04:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (3,'E','06:53:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'A','03:10:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'B','05:09:00')");
		stat.execute("INSERT INTO Event_Log VALUES (4,'E','07:29:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'A','03:44:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'B','06:06:00')");
		stat.execute("INSERT INTO Event_Log VALUES (5,'E','07:52:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'A','04:20:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'C','07:12:00')");
		stat.execute("INSERT INTO Event_Log VALUES (6,'E','09:07:00')");*/
		

		// WEAKLY FOLLOWS
		/*test.startTimeMeasurement();
		rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM Event_Log)");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}     
		test.printTimeTaken();*/
		
		/*System.out.println("FOLLOWS - SELECTION");
		test.startTimeMeasurement();
		rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT * FROM Event_Log WHERE Event = 'A' or Event = 'B' or Event = 'C')");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}    
		test.printTimeTaken();*/
		
		/*System.out.println("SELECTION - FOLLOWS");
		test.startTimeMeasurement();
		rs = stat.executeQuery("SELECT * FROM (FOLLOWS(SELECT * FROM Event_Log)) where (event_label_p = 'A' or event_label_p = 'B' or event_label_p = 'C')"
				+ "and (event_label_s = 'A' or event_label_s = 'B' or event_label_s = 'C');");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}  
		test.printTimeTaken();*/
		
		/*System.out.println("OCCURRENCE");
		rs = stat.executeQuery("SELECT * FROM OCCURRENCE(SELECT * FROM Event_Log);");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}  */
		
		/*System.out.println("CONTROLLER");
		rs = stat.executeQuery("SELECT * FROM CONTROLLER(SELECT * FROM Event_Log);");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4));
		}  */
		
		// AUTOFOLLOWS
		/*rs = stat.executeQuery("SELECT * FROM autofollows('*','Event_Log','')");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
		}  
		
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:22:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'B','00:23:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:24:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'A','00:25:00')");
		stat.execute("INSERT INTO Event_Log VALUES (1,'C','00:26:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'A','00:26:00')");
		stat.execute("INSERT INTO Event_Log VALUES (2,'C','00:27:00')");
		
		stat.execute("insert into Event_Log values(" + "1" + ",'" + "X" + "', '03:22:00');");	

		// AUTO DIRECTLY FOLLOWS
		rs = stat.executeQuery("select * from auto_directlyfollows");
		while(rs.next()) {
			System.out.println("auto directly: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}*/
		
		/*stat.execute("insert into Event_Log values(" + "11" + ",'" + "Y" + "', '03:22:00');");	

		rs = stat.executeQuery("select * from auto_directlyfollows");
		while(rs.next()) {
			System.out.println("auto directly: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
		
		stat.execute("insert into Event_Log values(" + "11" + ",'" + "Z" + "', '04:22:00');");	

		rs = stat.executeQuery("select * from auto_directlyfollows");
		while(rs.next()) {
			System.out.println("auto directly: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}*/

		// DIRECTLY FOLLOWS
		rs = stat.executeQuery("SELECT * FROM directlyfollows(select * from "+ logName + ")");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3));
		}  
		
		// ALIAS
		/*stat.execute("CREATE ALIAS aliasFollows " +
                "FOR \"org.h2.util.DirectlyFollows2.directlyFollows2\" ");
		
		rs = stat.executeQuery("SELECT * FROM aliasFollows(select * from event_log) where (event_label_p = 'A')"
				+ "and (event_label_s = 'A' or event_label_s = 'B' or event_label_s = 'C');");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3));
		}*/
		
		// TRIGGER
		/*stat.execute("drop table if exists auto_directlyfollows;");
		stat.execute("create table auto_directlyfollows(event_label_p varchar(255), event_label_s varchar(255), frequency int, primary key(event_label_p, event_label_s));");
		
		//stat.execute("select * from triggerdf(select * from event_log)");   
		
		rs = stat.executeQuery("select * from Event_Log");    
		while(rs.next()) {
			System.out.println(rs.getString(1));
		}*/
		
		/*DatabaseMetaData md = conn.getMetaData();
		
		Table t = (Table) md.getTables("TEST", "%", "Event_Log", new String[]{"TABLE"});
		
		System.out.println(t.getColumn(1).getName());*/
		
		stat.close();
		conn.close();
		
		System.out.println(System.getProperty("java.runtime.version"));
	}

}
