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

import org.deckfour.xes.classification.XEventClass;
import org.processmining.plugins.InductiveMiner.dfgOnly.Dfg;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgImpl;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgMiningParameters;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgMiningParametersIMd;
import org.processmining.plugins.InductiveMiner.dfgOnly.plugins.IMdProcessTree;
import org.processmining.processtree.ProcessTree;

public class InductiveMinerTest {
	
	static Connection conn;
	static Statement stat;
	static ResultSet rs;
	static long startTime;
	static BufferedWriter out;
	static double avgAbstraction;
	static double avgMining;
	static double avgRetrieval;	
	static double avgInitRetrieval;	
	static double avgDbRetrieval;
	static double avgFreqRetrieval;
	static double avgDfgRetrieval;
	static long startTimeRet;
	
	public static void loadLog(String logName) throws SQLException, IOException{
		stat.execute("DROP TABLE IF EXISTS " + logName);
		
		startTimeMeasurement();		
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "cases as CaseID,"
				+ "event as Activity,"
				+ "convert(parseDateTime(completeTime,'yyyy/MM/dd HH:mm:ss.SSS'),TIMESTAMP) AS CompleteTimestamp "
				//+ "MonthlyCost, Selected, ApplicationID, CreditScore, FirstWithdrawalAmount, OfferedAmount, Accepted, NumberOfTerms, OfferID, Action, EventOrigin, EventID "
				+ " FROM CSVREAD('C:/Users/asyamsiy/workspace/h2processmining-master/resources/"+logName+".csv', null, 'fieldSeparator=,');");
		/*stat.execute("CREATE INDEX IF NOT EXISTS CaseID_idx ON "+logName+"(CaseID)");
		stat.execute("CREATE INDEX IF NOT EXISTS Activity_idx ON "+logName+"(Activity)");
		stat.execute("CREATE INDEX IF NOT EXISTS CompleteTimestamp_idx ON "+logName+"(CompleteTimestamp)");
*/		printTimeTaken();

		//Execute a simple query first, because otherwise the indexes do not seem to be initialized, 
		//which causes a very slow response in the nested query.
		/*stat.executeQuery(
				"  SELECT DISTINCT a.Activity, b.Activity "
				+ "FROM "+"Event_Log"+" a, "+"Event_Log"+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp"
				);*/
	}
	
	public static void startTimeMeasurement(){
		startTime = System.nanoTime();
	}
	
	public static double printTimeTaken() throws IOException{
		long endTime = System.nanoTime();
		double timeTaken =  (endTime - startTime)/1000000000.0; 
		//System.out.println(timeTaken);
		return timeTaken;
	}
	
	public static Dfg directlyFollowsOperator(String logName) throws SQLException, IOException {
		stat.setFetchSize(10000);
		
		// abstraction
		startTimeMeasurement();		
		rs = stat.executeQuery("SELECT * FROM DIRECTLYFOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		double timeTaken = printTimeTaken();
		avgAbstraction += timeTaken;
		System.out.println(timeTaken);
		
		// retrieval
		startTimeMeasurement();	
		Dfg dfg = createDfg(rs);
		double timeTaken2 = printTimeTaken();
		avgRetrieval += timeTaken2;
		System.out.println(timeTaken2);
		
		rs.close();		
		return dfg;	
	}
	
	public static Dfg weaklyFollowsOperator(String logName) throws SQLException, IOException {
		stat.setFetchSize(10000);
		
		// abstraction
		startTimeMeasurement();		
		rs = stat.executeQuery("SELECT * FROM FOLLOWS(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		double timeTaken = printTimeTaken();
		avgAbstraction += timeTaken;
		System.out.println(timeTaken);
		
		// retrieval
	/*	startTimeMeasurement();	
		Dfg dfg = createDfg(rs);
		double timeTaken2 = printTimeTaken();
		avgRetrieval += timeTaken2;
		System.out.println(timeTaken2);
		
		rs.close();		*/
		//return dfg;
		return null;
	}
	
	public static Dfg builtIn(String logName) throws SQLException, IOException {
		stat.setFetchSize(10000);
		
		// abstraction
		startTimeMeasurement();
		rs = stat.executeQuery(
				"  SELECT a.Activity, b.Activity, count(*) "
				+ "FROM "+logName+" a, "+logName+" b "
				+ "WHERE a.CaseID = b.CaseID AND a.CompleteTimestamp < b.CompleteTimestamp AND "
				+ "  NOT EXISTS("
				+ "    SELECT * "
				+ "    FROM "+logName+" c "
				+ "    WHERE c.CaseID = a.CaseID AND a.CompleteTimestamp < c.CompleteTimestamp AND c.CompleteTimestamp < b.CompleteTimestamp) "
				+ "GROUP BY a.Activity, b.Activity;");
		double timeTaken = printTimeTaken();
		avgAbstraction += timeTaken;
		
		// retrieval
		startTimeMeasurement();
		Dfg dfg = createDfg(rs);
		double timeTaken2 = printTimeTaken();
		avgRetrieval += timeTaken2;
		
		rs.close();
		return dfg;	
	}
	
	public static Dfg sortedLogApproach(String logName) throws SQLException, IOException {
		/*rs = stat.executeQuery("select count(*) "
				+ "from "+ logName);
		rs.next();
		int rows = rs.getInt(1);*/
		stat.setFetchSize(1000000);
		
		// abstraction
		startTimeMeasurement();
		rs = stat.executeQuery("select * "
				+ "from "+ logName
				+ " order by CaseID asc, CompleteTimestamp asc");
		double timeTaken = printTimeTaken();
		avgAbstraction += timeTaken;
		System.out.println(timeTaken);
		
		// retrieval
		startTimeMeasurement();
		startTimeRet = System.nanoTime();
		Map<String,Integer> follows = new HashMap<String,Integer>();		
		String prevCase = "", prevAct = "";
		
		if(rs.next()) {
			prevCase = rs.getString(1);
			prevAct = rs.getString(2);
			String key = "start---" + prevAct;
			follows.put(key, 1);
		}		
		avgInitRetrieval += printTimeTaken();
		
		// calculate frequency of pairs
		startTimeMeasurement();
		while(rs.next()) {			
			String caseID = rs.getString(1);
			String activity = rs.getString(2);
			avgDbRetrieval += printTimeTaken();
			
			startTimeMeasurement();
			if(prevCase.equals(caseID)) {
				String key = prevAct + "---" + activity;
				Integer val = follows.get(key);
				
				Integer temp = (val == null) ? follows.put(key, 1) : follows.put(key, val + 1);
				
				prevAct = activity;
			} else {
				String key = prevAct + "---end";
				Integer val = follows.get(key);
				Integer temp = (val == null) ? follows.put(key, 1) : follows.put(key, val + 1);
				
				key = "start---" + activity;
				val = follows.get(key);
				temp = (val == null) ? follows.put(key, 1) : follows.put(key, val + 1);
				
				prevCase = caseID;
				prevAct = activity;
			}
			
			avgFreqRetrieval += printTimeTaken();
			startTimeMeasurement();
			
		}
		
		rs.close();
		
		// create dfg
		startTimeMeasurement();
		Dfg dfg = new DfgImpl();
		HashMap<String,Integer> eventClassIndex = new HashMap<String, Integer>();
		int index = 0;
		
		for(String key : follows.keySet()) {
			String[] arr = key.split("---");
			String elem1 = arr[0]; 
			String elem2 = arr[1];
			Integer freq = follows.get(key);
			
			out.write(elem1 + "\t" + elem2 + "\t" + freq + "\n");		
			
			if(!eventClassIndex.containsKey(elem1)) {
				eventClassIndex.put(elem1, index);
				index++;
			}				
			if(!eventClassIndex.containsKey(elem2)) {
				eventClassIndex.put(elem2, index);
				index++;
			}
			
			XEventClass ec1 = new XEventClass(elem1, eventClassIndex.get(elem1));
			XEventClass ec2 = new XEventClass(elem2, eventClassIndex.get(elem2));		
			
			if(elem1.equalsIgnoreCase("start")) {
				dfg.addActivity(ec2);
				dfg.addStartActivity(ec2, freq); 
			} else if(elem2.equalsIgnoreCase("end")) {
				dfg.addActivity(ec1);
				dfg.addEndActivity(ec1, freq);
			} else {
				dfg.addActivity(ec1);
				dfg.addActivity(ec2);
				dfg.addDirectlyFollowsEdge(ec1, ec2, freq);
			}
		}
		
		avgDfgRetrieval += printTimeTaken();
		double endTimeRet = System.nanoTime();
		double timeTakenRet =  (endTimeRet - startTimeRet)/1000000000.0; 
		System.out.println(timeTakenRet);
		avgRetrieval += timeTakenRet;
		
		return dfg;
	}
	
	public static Dfg createDfg(ResultSet rs) throws SQLException, IOException {
		Dfg dfg = new DfgImpl();
		HashMap<String,Integer> eventClassIndex = new HashMap<String, Integer>();
		int index = 0;
		// add pair of activities to DFG
		while(rs.next()) {
			String elem1 = rs.getString(1);
			String elem2 = rs.getString(2);
			int freq = rs.getInt(3);
			out.write(elem1 + "\t" + elem2 + "\t" + freq + "\n");
						
			if(!eventClassIndex.containsKey(elem1)) {
				eventClassIndex.put(elem1, index);
				index++;
			}				
			if(!eventClassIndex.containsKey(elem2)) {
				eventClassIndex.put(elem2, index);
				index++;
			}
			
			XEventClass ec1 = new XEventClass(elem1, eventClassIndex.get(elem1));
			XEventClass ec2 = new XEventClass(elem2, eventClassIndex.get(elem2));		
			
			if(elem1.equalsIgnoreCase("start")) {
				dfg.addActivity(ec2);
				dfg.addStartActivity(ec2, freq); 
			} else if(elem2.equalsIgnoreCase("end")) {
				dfg.addActivity(ec1);
				dfg.addEndActivity(ec1, freq);
			} else {
				dfg.addActivity(ec1);
				dfg.addActivity(ec2);
				dfg.addDirectlyFollowsEdge(ec1, ec2, freq);
			}
		}		
		return dfg;
	}
	
	public static void discover(Dfg dfg) throws IOException {
		startTimeMeasurement();		
		DfgMiningParameters param = new DfgMiningParametersIMd();
		ProcessTree pt = IMdProcessTree.mineProcessTree(dfg, param);
		double timeTaken = printTimeTaken();
		avgMining += timeTaken;
		System.out.println(timeTaken);
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		
		String[] logs = {"t128Ka50i"}; // set of logs
		int i = 0;
		int iteration = 3;
		
		System.out.println(iteration);

		// looping for logs
		for (int j = 0; j < logs.length; j++) {
			avgAbstraction = 0.0;
			avgMining = 0.0;
			avgRetrieval = 0.0;			
			
			// loop for iteration 
			for (i = 0; i < iteration; i++) {
				out = new BufferedWriter(new FileWriter("C:\\Users\\asyamsiy\\Documents\\Experiment\\h2\\Dfg\\H2" + logs[j] + ".txt", false));
				String fileName = logs[j];
				
				Class.forName("org.h2.Driver");
				//conn = DriverManager.getConnection("jdbc:h2:tcp://ais-hadoop-1.win.tue.nl/~/h2processmining;cache_size=60000000;multi_threaded=1", "sa", "");
				conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test2", "sa", "");
				//conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
		        
				conn.setAutoCommit(false);
				stat = conn.createStatement();				
				stat.execute("SET MULTI_THREADED 1");
				
				//loadLog(fileName);
				
				//Dfg dfg = directlyFollowsOperator(fileName);
				Dfg dfg = weaklyFollowsOperator(fileName);
				//Dfg dfg = sortedLogApproach(fileName);		
				//Dfg dfg = builtIn(fileName);
				
				if(dfg!= null)
				discover(dfg);				
				
				out.close();
				conn.close(); // closing takes time in native
				stat.close();
				System.gc();
			}
			
			System.out.println("-----" + logs[j]);			
			System.out.println(avgAbstraction / i);
			System.out.println(avgRetrieval / i);
			System.out.println(avgMining / i);		
			System.out.println(avgInitRetrieval/i);
			System.out.println(avgDbRetrieval/i);
			System.out.println(avgFreqRetrieval/i);
			System.out.println(avgDfgRetrieval/i);
			
		}
	}

}
