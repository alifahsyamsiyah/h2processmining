package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.tools.SimpleResultSet;
import org.quartz.SchedulerException;
import org.h2.util.DirectlyFollowsRecurrent;
import org.h2.util.TriggerDirectlyFollowsLastEvent;

public class AutoFollows {
	
	long startTime;
	
	public void startTimeMeasurement(){
		startTime = System.currentTimeMillis();
	}
	
	public void printTimeTaken(){
		long endTime = System.currentTimeMillis();
		System.out.println("\ttime taken: " + (endTime - startTime) + " ms.");
	}
	
	public static ResultSet autoFollows(Connection conn, String select, String from, String where, String dateFrom, String dateTo, long interval) throws SQLException {	
		System.out.println("enter auto followss " + System.nanoTime());
		
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")){
			SimpleResultSet result = new SimpleResultSet();
			result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
			result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
			result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			return result;
		}
		
		// process the input
		String[] arr = select.split(",");
		String time = arr[2];
		if(where.equals("")) {
			where = time + " >= '" + dateFrom + "' and " + time + " <= '" + dateTo + "'";
			System.out.println(where);
		} else {
			where = where + " and " + time + " >= '" + dateFrom + "' and " + time + " <= '" + dateTo + "'";
		}		
		
		// compute DFR
		String followsQuery = "select " + select + " from " + from + " where " + where;
		
		Statement stat = conn.createStatement();
		ResultSet eventLog = stat.executeQuery(followsQuery);

		DirectlyFollowsRecurrent dfrec = new DirectlyFollowsRecurrent();
		//ResultSet result = dfrec.directlyFollowsRecurrent(conn, eventLog, interval);
		dfrec.directlyFollowsRecurrent(conn, eventLog, interval);
		
		 		
		// create trigger
		stat.execute("drop trigger if exists trigger_dfr");
		stat.execute("create alias if not exists trigger_set for \"" +
				TriggerDirectlyFollowsLastEvent.class.getName() +
                ".setInterval\"");
		stat.execute("create trigger if not exists TRIGGER_DFR before insert on " + from + " for each row call \"" +
				TriggerDirectlyFollowsLastEvent.class.getName() + "\"");
		stat.execute("call trigger_set('TRIGGER_DFR', "+interval+")");
		
		//eventLog.getStatement().close(); // close the statement
		
		// call delete scheduler
		/*DeleteScheduler ds = new DeleteScheduler(conn);
		ds.run();*/
		
		System.out.println("end auto follows");			
		
		SimpleResultSet result = new SimpleResultSet();
		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
		return result;
	}
}
