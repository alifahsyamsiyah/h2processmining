package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.tools.SimpleResultSet;

public class ControllerFunction {
	
	long startTime;
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}
	
	public static ResultSet controllerFunction(Connection conn, ResultSet eventLog) throws SQLException {
		ControllerFunction test = new ControllerFunction();
		test.startTimeMeasurement();
		System.out.println("enter controller");
		
		class LogEvent implements Comparable<LogEvent> {
			long time;
			String label;
			
			LogEvent(long time, String label){
				this.time = time;
				this.label = label;
			}
			
			@Override
			public int compareTo(LogEvent e2) {
				if (time - e2.time < 0){
					return -1;
				}else{
					if (time - e2.time > 0){
						return 1;
					}else{
						return 0;
					}
				}
			}
		}
		
		//If there is no data in the eventLog, return an empty result
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection") || (eventLog == null) || (eventLog.getMetaData().getColumnCount() != 3)){
			SimpleResultSet result = new SimpleResultSet();
			result.addColumn("TYPE", java.sql.Types.INTEGER, 0, 0);
			result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
			result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
			result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			return result;
		}
		
		//Map of case identifiers to sequences for that case. Each sequence is a list of log events.
		Map<String,List<LogEvent>> caseId2Sequence = new HashMap<String,List<LogEvent>>();
		//Controller Function
		Map<String,Map<String,Integer>> controllerMatrices = new HashMap<String,Map<String,Integer>>();
		//List of distinct eventclasses
		ArrayList<String> eventclasses = new ArrayList<String>();
		//Occurrence
		Map<String,Integer> occurrence = new HashMap<String,Integer>();
		//No Following
		Map<String,Integer> nofollowing = new HashMap<String,Integer>();
		//No Preceding
		Map<String,Integer> nopreceding = new HashMap<String,Integer>();
		//No Co-Occurring
		Map<String,Integer> nocooccurring = new HashMap<String,Integer>();
		//DirectlyFollows
		Map<String,Integer> directlyfollows = new HashMap<String,Integer>();
		//Repetition
		Map<String,Integer> repetition = new HashMap<String,Integer>();
		//Repetition (Backwards)
		Map<String,Integer> repetitionback = new HashMap<String,Integer>();
		
		//Create a map of case to the sequence of events and create a list of eventclasses 
		while (eventLog.next()){
			String caseId = eventLog.getString(1);
			String eventClass = eventLog.getString(2);
			Timestamp time = eventLog.getTimestamp(3);
			
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			if (sequence == null){
				sequence = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseId, sequence);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			sequence.add(new LogEvent(time.getTime(), eventClass));
			
			//Add a new observed activity
			if(!eventclasses.contains(eventClass)) {
				eventclasses.add(eventClass);
			}
		}
				
		//For each case 
		for(String caseId : caseId2Sequence.keySet()){
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			Collections.sort(sequence);
						
			//controller matrix per case
			Map<String,Integer> controllerMatrix = new HashMap<String,Integer>();
			
			//For each event
			for(int i = 0; i < sequence.size(); i++) {
				String eventX = sequence.get(i).label;
				
				//DIrectlyFollows
				if(i != sequence.size() - 1) {
					String nextEvent = sequence.get(i+1).label;
					String key = eventX + "---" + nextEvent;
					Integer val = directlyfollows.get(key);
					Integer temp = (val == null) ? directlyfollows.put(key, 1) : directlyfollows.put(key, val+1);
				}
				
				//For any other event classes
				for(int j = 0; j < eventclasses.size(); j++) {
					String eventY = eventclasses.get(j);
					
					String xx = eventX + "---" + eventX;
					String xy = eventX + "---" + eventY;
					String yx = eventY + "---" + eventX;
					String yy = eventY + "---" + eventY;
					
					Integer xxVal = controllerMatrix.get(xx);
					Integer xyVal = controllerMatrix.get(xy);
					Integer yxVal = controllerMatrix.get(yx);
					Integer yyVal = controllerMatrix.get(yy);
					
					Integer val, newval, temp, temp2;
					
					if(eventX.equals(eventY)) { // eventX == eventY
						val = occurrence.get(xx);
						temp = (val == null) ? occurrence.put(xx, 1) : occurrence.put(xx, val + 1); // add 1 to occurrence(x,x)
					
						// if eventX is the first occurrence in the case
						if(xxVal == null || xxVal == 0) {
							for(String k : controllerMatrix.keySet()) {
								String[] arr = k.split("---");
								String ec1 = arr[0]; String ec2 = arr[1];
								Integer occ = controllerMatrix.get(k);
								
								if(ec1.equals(ec2) && occ != null) {
									String removed = ec1+"---"+eventX;
									val = nocooccurring.get(removed);
									newval = val-occ;
									temp = (newval == 0) ? nocooccurring.remove(removed) : nocooccurring.put(removed, newval); // minus nocooccurring(y,x) with controllermatrix(y,y)
								}
							}
						}
						
						temp = (xxVal == null) ? controllerMatrix.put(xx, 1) : controllerMatrix.put(xx, xxVal + 1); // add 1 to controllermatrix(x,x)
					
					} else { // eventX != eventY
						temp = (xyVal == null) ? controllerMatrix.put(xy, 1) : controllerMatrix.put(xy, xyVal + 1); // add 1 to controllermatrix(x,y)
						
						val = nofollowing.get(xy);
						temp = (val == null) ? nofollowing.put(xy, 1) : nofollowing.put(xy, val + 1); // add 1 to nofollowing(x,y)
						
						temp = (controllerMatrix.get(yx) == null) ? 1 : controllerMatrix.remove(yx); // reset
						
						if(yxVal != null ) {								
							if(yxVal > 0) {
								val = nofollowing.get(yx);
								newval = val - yxVal;
								temp = (newval == 0) ? nofollowing.remove(yx) : nofollowing.put(yx, newval); // minus nofollowing(y,x) with controllermatrix(y,x)
							} 
							if(yxVal > 1) {
								val = repetition.get(yx);
								temp = (val == null) ? repetition.put(yx, yxVal - 1) : repetition.put(yx, val + (yxVal - 1)); // add controllermatrix(y,x) - 1 to repetition(y,x)
							}							
						}
							
						Integer valnoprec = nopreceding.get(xy);
						Integer valnocooc = nocooccurring.get(xy);
						
						if(yyVal == null || yyVal == 0) {
							temp = (valnoprec == null) ? nopreceding.put(xy, 1) : nopreceding.put(xy, valnoprec + 1); // add 1 to copreceding(x,y)
							temp = (valnocooc == null) ? nocooccurring.put(xy, 1) : nocooccurring.put(xy, valnocooc + 1); // add 1 to nocooccurring(x,y)
						}
						
						val = repetitionback.get(xy);
						
						if(yyVal != null && yyVal >= 1 && xyVal != null && xyVal >= 1) {
							temp = (val == null) ? repetitionback.put(xy, 1) : repetitionback.put(xy, val + 1); // add 1 to repetitionback(x,y)
						}					
						
					}
				}				
			}
			
			//controllerMatrices.put(caseId, controllerMatrix);
		}
		
		/*for(String id : controllerMatrices.keySet()) {
			
			Map<String,Integer> m = controllerMatrices.get(id);
			
			for(String key : m.keySet()) {
				System.out.println("controller function: "+ id + " " + key + " " + m.get(key));
			}
		}
		System.out.println();*/
		
		//Create the result.
		SimpleResultSet result = new SimpleResultSet();
		ResultSetMetaData rsm = eventLog.getMetaData();
		result.addColumn("TYPE", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
		result.addColumn("FREQUENCY", java.sql.Types.INTEGER, 0, 0);
		
		for(String id : occurrence.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("OCCURRENCE",eventclass[0],eventclass[1],occurrence.get(id));
		}
		for(String id : nofollowing.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("NO FOLLOWING",eventclass[0],eventclass[1],nofollowing.get(id));
		}
		for(String id : nopreceding.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("NO PRECEDING",eventclass[0],eventclass[1],nopreceding.get(id));
		}
		for(String id : nocooccurring.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("NO COOCCURRING",eventclass[0],eventclass[1],nocooccurring.get(id));
		}
		for(String id : directlyfollows.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("DIRECTLY FOLLOWS",eventclass[0],eventclass[1],directlyfollows.get(id));
		}
		for(String id : repetition.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("REPETITION",eventclass[0],eventclass[1],repetition.get(id));
		}
		for(String id : repetitionback.keySet()) {
			String[] eventclass = id.split("---");
			result.addRow("REPETITION BACK",eventclass[0],eventclass[1],repetitionback.get(id));
		}
		
		test.printTimeTaken();
				
		System.out.println("end controller");
		
		return result;
		
	}
}
