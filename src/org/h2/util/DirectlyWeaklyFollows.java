package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

public class DirectlyWeaklyFollows {	
	
	public static ResultSet directlyFollows(Connection conn, ResultSet eventLog) throws SQLException {
		
		System.out.println("enter directly weakly follows " + System.nanoTime());
		
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
						System.out.println("2 events sama");
						return 0;
					}
				}
			}
		}
		
		//If it is the first or second call of the query, or there is no data in the eventLog, return an empty result
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection") || (eventLog == null)  || (eventLog.getMetaData().getColumnCount() != 3)){
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
			String caseId = null, eventClass = null;
			Timestamp timestamp = null;
			caseId = eventLog.getString(1);		
			eventClass = eventLog.getString(2);
			timestamp = eventLog.getTimestamp(3);
			
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			if (sequence == null){
				sequence = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseId, sequence);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			sequence.add(new LogEvent(timestamp.getTime(), eventClass));
		}
		
		//For each sequence that is constructed in this manner:
		for (List<LogEvent> sequence: caseId2Sequence.values()){
			//Sort the sequence.
			Collections.sort(sequence);
			int sa = 0; //start index of the antecedents
			int ea = 0; //end index of the antecedents, this is the index of the last event that has the same timestamp as sequence[sa]
			long aTime = sequence.get(sa).time;
			while ((ea+1 < sequence.size()) && (aTime == sequence.get(ea+1).time)){
				ea++;
			}
			int sc = ea+1; //start index of the consequents
			int ec = sc; //end index of the consequents, this is the index of the last event that has the same timestamp as sequence[sc]
			//While there are consequents:
			while (ec < sequence.size()){
				long cTime = sequence.get(sc).time;
				while ((ec+1 < sequence.size()) && (cTime == sequence.get(ec+1).time)){
					ec++;
				}
				//Add all antecendent/consequent combinations to antecedent2consequents.
				for (int i = sa; i <= ea; i++){
					String antecedentStr = sequence.get(i).label;		
					for (int j = sc; j <= ec; j++){
						String consequentStr = sequence.get(j).label;
						String pair = antecedentStr + "---" + consequentStr;
						Integer counter = directlyFollows.get(pair);
						if(counter == null) {
							directlyFollows.put(pair, 1);
						} else {
							counter++;
							directlyFollows.put(pair, counter);
						}
					}
				}
				//Go the the next batch of antercedents/consequents.
				sa = sc;
				ea = ec;
				sc = ea+1;
				ec = sc;
			}
		}
		
		//Create the result.
		SimpleResultSet result = new SimpleResultSet();
		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			
		for (String key: directlyFollows.keySet()){
			String[] arr = key.split("---");
			String ec1 = arr[0];
			String ec2 = arr[1];
			Integer freq = directlyFollows.get(key);
			
			result.addRow(ec1,ec2,freq);
		}
		
	    eventLog.getStatement().close();
	    	    
	    System.out.println("end directly weakly follows");
		
		return result;
	}
}
