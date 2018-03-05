package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.tools.SimpleResultSet;

public class DirectlyFollows2 {	
	
	public static ResultSet directlyFollows2(Connection conn, ResultSet eventLog) throws SQLException {
		
		System.out.println("enter directly follows");
		
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
		
		String url = conn.getMetaData().getURL();
        if (url.equals("jdbc:columnlist:connection") || (eventLog == null) || (eventLog.getMetaData().getColumnCount() != 3)) {
        	SimpleResultSet result = new SimpleResultSet();
    		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
    		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
    		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
        	return result;
        }
		
		//Map of case identifiers to sequences for that case. Each sequence is a list of log events.
		Map<Object,List<LogEvent>> caseId2Sequence = new HashMap<Object,List<LogEvent>>();
		//Map for directly follows relations
		Map<String, Integer> directlyFollows = new HashMap<String, Integer>();
		
		//For each event in the event log:
		while (eventLog.next()){
			String caseId = eventLog.getString(1);
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			if (sequence == null){
				sequence = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseId, sequence);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			sequence.add(new LogEvent(eventLog.getTimestamp(3).getTime(), eventLog.getString(2)));
		}
		
		//For each sequence that is constructed in this manner:
		for (List<LogEvent> sequence: caseId2Sequence.values()){
			//Sort the sequence.
			Collections.sort(sequence);
			
			for(int i = 0; i < sequence.size(); i++) {
				// end event class
				if(i == sequence.size() - 1) {
					String ec1 = (String) sequence.get(i).label;
					String ec2 = "END";
					
					String key = ec1 + "-" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);					
				} else {
					if(i == 0) {
						String ec1 = "START";
						String ec2 = (String) sequence.get(i).label;
						
						String key = ec1 + "-" + ec2;
						
						Integer val = directlyFollows.get(key);
						Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);
					}
					String ec1 = (String) sequence.get(i).label;
					String ec2 = sequence.get(i+1).label;
					
					String key = ec1 + "-" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);
				}				
			}
			
		}
		
		//Create the result.
		SimpleResultSet result = new SimpleResultSet();
		ResultSetMetaData rsm = eventLog.getMetaData();
		result.addColumn("EVENT_LABEL_P", rsm.getColumnType(2), rsm.getPrecision(2), 0);
		result.addColumn("EVENT_LABEL_S", rsm.getColumnType(2), rsm.getPrecision(2), 0);
		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			
		for (String key: directlyFollows.keySet()){
			String[] arr = key.split("-");
			String ec1 = arr[0];
			String ec2 = arr[1];
			Integer freq = directlyFollows.get(key);
			
			result.addRow(ec1,ec2,freq);
		}
		
	    eventLog.getStatement().close();
	    
	    System.out.println("end directly follows");
		
		return result;
	}
}
