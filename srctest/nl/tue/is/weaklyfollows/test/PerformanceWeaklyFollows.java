package nl.tue.is.weaklyfollows.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class PerformanceWeaklyFollows {

	static Connection conn;
	static Statement stat;
	long startTime;

	public PerformanceWeaklyFollows() throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:;DB_CLOSE_ON_EXIT=FALSE", "sa", "");
		//conn =  DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test2;DB_CLOSE_ON_EXIT=FALSE", "sa", "");
		//conn = DriverManager.getConnection("jdbc:h2:tcp://ais-hadoop-1.win.tue.nl/~/test", "sa", "");
		stat = conn.createStatement();
	}
	
	public void close() throws SQLException{
		stat.close();
		conn.close();
	}
	
	public void createTable(String logName) throws SQLException {
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " (CaseID varchar(255),"
				+ "Activity varchar(255),"
				+ "CompleteTimestamp timestamp"
				//+ "Variant varchar(255),"
				//+ "VariantIndex int
				+");");
		
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");
	}
	
	public void insertTable(String logName) throws SQLException {
		stat.execute("INSERT INTO " + logName 
				+ "(CaseID, Activity, CompleteTimestamp) SELECT "
				+ "cases,"
				+ "event,"
				+ "convert(parseDateTime(CompleteTime,'yyyy/MM/dd HH:mm:ss.SSS'),TIMESTAMP) AS CompleteTimestamp"
				+ " FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=,');");
	}
	
	public void loadLog(String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd HH:mm:ss'),TIMESTAMP) AS CompleteTimestamp"
				//+ "Variant,"
				//+ "VariantIndex "
				+ " FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=,');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");

		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp"
				);
	}
	
	public void loadLog2(String logName) throws SQLException{
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "Sequence "
				+ "FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=,');");
		stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS Sequence_idx ON "+logName+"(Sequence)");

		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.Sequence = b.Sequence - 1"
				);
	}

	public void printTableDefinition(String tableName) throws SQLException{
		ResultSet rs = stat.executeQuery("SELECT * FROM " + tableName);
		System.out.println(tableName + "(");
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++){
			System.out.println("\t" + rs.getMetaData().getColumnName(i) + "\t" + rs.getMetaData().getColumnTypeName(i));
		}
		System.out.println(")");
	}

	public void printTableSize(String tableName) throws SQLException{
		System.out.println("Statistics for " + tableName + ":");
		ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
		rs.next();
		System.out.println("\tnumber of events:\t" + rs.getInt(1));
		
		rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT CaseID FROM " + tableName + " GROUP BY CaseID)");
		rs.next();
		System.out.println("\tnumber of cases:\t" + rs.getInt(1));

		rs = stat.executeQuery("SELECT COUNT(*) FROM (SELECT Activity FROM " + tableName + " GROUP BY Activity)");
		rs.next();
		System.out.println("\tnumber of event types:\t" + rs.getInt(1));
}

	public void printResultSet(ResultSet rs) throws SQLException{
		while (rs.next()){
			StringBuilder sb = new StringBuilder();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			for (int i = 1; i <= numberOfColumns; i++) {
				sb.append(rs.getString(i));
				if (i < numberOfColumns) {
					sb.append(", ");
				}
			}
			String data = sb.toString();
			System.out.println(data);
		}
	}public ResultSet executeQuery(String query) throws SQLException{
		return stat.executeQuery(query);
	}
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, SchedulerException {
		if (args.length < 1){
			System.out.println("Provide the event log that must be loaded as an argument, e.g.: BPI2011.");
			System.exit(0);
		}
		String logName = args[0];
		
		PerformanceWeaklyFollows test = new PerformanceWeaklyFollows();

		// TEST CONTROLLER
		/*test.loadLog(logName);
		test.printTableSize(logName);

		System.out.println("Measuring the time taken to execute the weakly follows relation on the "+logName+" log ...");
		test.startTimeMeasurement();
		test.executeQuery("SELECT * FROM CONTROLLER(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		test.printTimeTaken();*/
		
		/*ResultSet rs2 = test.executeQuery("SELECT "
				+ "CaseID,"
				+ "Activity,"
				+ "convert(parseDateTime(CompleteTimestamp,'yyyy/MM/dd HH:mm:ss'),TIMESTAMP) AS CompleteTimestamp,"
				+ "Variant,"
				+ "VariantIndex "
				+ "FROM CSVREAD('./resources/"+logName+".csv', null, 'fieldSeparator=;')");
		
		while(rs2.next()) {
			System.out.println(rs2.getString(1) + " " + rs2.getString(2) + " " + rs2.getString(3) + " " + rs2.getString(4) + " " + rs2.getString(5));
		}*/
				
		
		// TEST TRIGGER
		test.startTimeMeasurement();
		
		test.createTable(logName);	
		
		test.executeQuery("SELECT * FROM autofollows('caseid,activity,completetimestamp','"+logName+"','','2016-01-01','2016-10-01',31556926000)");	
		test.insertTable(logName);
		
		/*ResultSet rs = stat.executeQuery("select * from dfr");
		while(rs.next()) {
			System.out.println("dfr: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
		
		rs = stat.executeQuery("select * from to_delete_event");
		while(rs.next()) {
			System.out.println("to_delete_event: " + rs.getString(1) + " " + rs.getString(2));
		}*/
			
		test.printTimeTaken();
				
		Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
		List<JobExecutionContext> currentlyExecuting = scheduler.getCurrentlyExecutingJobs();
	
		for( JobExecutionContext jobExecutionContext : currentlyExecuting) {
			JobKey jk = jobExecutionContext.getJobDetail().getKey();
	        if(jk.getName().equals("deleteJob")) {
	        	System.out.println("interrupt job");
	            scheduler.interrupt(jk);
	            scheduler.deleteJob(jk);
	            scheduler.shutdown();
	       }
		}
	
		

		// TEST BUILT-IN
		/*System.out.println("Measuring the time taken to execute a nested query to get the weakly follows relation on the "+logName+" log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM "+logName+" c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp"
				+ "  );");
		test.printTimeTaken();		*/
		
		// TEST MY QUERY
		/*String logName2 = "BPI2011_flat";
		test.loadLog2(logName2);
		test.printTableSize(logName2);
		
		System.out.println("Measuring the time taken to execute my query to get the weakly follows relation on the "+logName2+" log ...");
		test.startTimeMeasurement();
		test.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+logName2+" a, "+logName2+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.Sequence = b.Sequence - 1 ;");
		test.printTimeTaken();	*/	
		
		// TEST EDU'S QUERY
		/*test.loadLog(logName);
		test.printTableSize(logName);*/
		
		/*test.startTimeMeasurement();
		QueryTest qt = new QueryTest();		
		qt.test(conn, logName);
		test.printTimeTaken();*/
		
		// TEST DIRECTLY FOLLOWS
		/*test.loadLog(logName);
		test.printTableSize(logName);*/

		/*test.startTimeMeasurement();
		ResultSet rs = test.executeQuery("SELECT * FROM DIRECTLYFOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
		test.printTimeTaken();*/
		
		//test.close();
	}

}
