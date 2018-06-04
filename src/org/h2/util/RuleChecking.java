package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.ddl.CreateTrigger;
import org.h2.tools.SimpleResultSet;

public class RuleChecking {
	
	public static ResultSet ruleChecking(Connection conn, String logTable, Set<AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>>> rules, Set<AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>>> rulesWithToken, HashMap<String, Integer> eventClassToInt) throws SQLException {	
		
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")){
			SimpleResultSet result = new SimpleResultSet();
			result.addColumn("VALID", Types.VARCHAR, 255, 0);
			return result;
		}
		
		Statement stat = conn.createStatement();
		
		// create table ErrorLog
		stat.execute("drop table if exists ERRORLOG");
		stat.execute("create table ERRORLOG(caseid varchar(255), index int);");		
		
		// create table EventClassOccurrence
		stat.execute("drop table if exists EVENTCLASSOCCURRENCE");
		
		String[] eventClasses = new String[eventClassToInt.size() + 1];
		for(String label : eventClassToInt.keySet()) {
			eventClasses[eventClassToInt.get(label)] = label;
		}
		String query = "create table EVENTCLASSOCCURRENCE(caseid varchar(255), ";
		for(int i = 1 ; i < eventClasses.length; i++) {
			query = query + eventClasses[i] + " int, ";
		}
		query = query + "lastindex int)";
		System.out.println("query for create table: " + query);
		stat.execute(query);
		
		// create trigger
		/*TriggerRuleChecking trigger = new TriggerRuleChecking();		
		trigger.init(conn, "PUBLIC", TriggerRuleChecking.class.getName(), "log", true, 1);
		String query2 = "create trigger if not exists TRIGGER_RULECHECKING before insert on " + logTable + " for each row call \"" +
				TriggerRuleChecking.class.getName() + "\"";
		System.out.println(query2);*/
		
		stat.execute("create trigger if not exists TRIGGER_RULECHECKING before insert on " + logTable + " for each row call \"" +
				TriggerRuleChecking.class.getName() + "\"");
		/*stat.execute("call " + TriggerRuleChecking.class.getName() +
                ".setRule('TRIGGER_RULECHECKING', " + rules + "," + rulesWithToken + "," +  eventClassToInt.size() +  ")");*/
		stat.execute("create alias if not exists TRIGGER_SET_RULECHECKING for \"" +
				TriggerRuleChecking.class.getName() +
                ".setRule\"");
		stat.execute("call " + "TriggerRuleChecking.setRule('TRIGGER_RULECHECKING',"  +  eventClassToInt.size() +  ")");
		
			
		/*TriggerRuleChecking trigger = new TriggerRuleChecking();
		trigger.init(conn, "PUBLIC", "TRIGGER_RULECHECKING", "LOG", false, 1);*/
		//trigger.setRule(conn, "TRIGGER_RULECHECKING", rules, rulesWithToken, eventClassToInt.size());
		
		/*stat.execute("drop trigger if exists TRIGGER_RULECHECKING");
		stat.execute("create alias if not exists TRIGGER_SET_RULECHECKING for \"" +
				TriggerRuleChecking.class.getName() +
                ".setRule\"");
		stat.execute("create trigger if not exists TRIGGER_RULECHECKING before insert on " + logTable + " for each row call \"" +
				TriggerRuleChecking.class.getName() + "\"");
		stat.execute("call TRIGGER_SET_RULECHECKING('TRIGGER_RULECHECKING', " + rules + ", "+ rulesWithToken + ", " + eventClassToInt.size() +  ")");*/
		//stat.execute("call TRIGGER_SET_RULECHECKING('TRIGGER_RULECHECKING', " + eventClassToInt.size() +  ")");
		
	/*	stat.execute("insert into log values('1','Z',11)");
		stat.execute("insert into log values('1','F',11)");
		stat.execute("insert into log values('1','Y',11)");
		stat.execute("insert into log values('2','X',11)");
		stat.execute("insert into log values('2','C',11)");
		stat.execute("insert into log values('2','A',11)");
		stat.execute("insert into log values('2','B',11)");
		stat.execute("insert into log values('2','Y',11)");
		stat.execute("insert into log values('3','W',11)");
		stat.execute("insert into log values('3','W',11)");
		
		HashMap<String, ArrayList<Integer>> error = new HashMap<String,ArrayList<Integer>>();
		ResultSet rs = stat.executeQuery("select * from ErrorLog");
		while(rs.next()) {
			String caseid = rs.getString(1);
			Integer index = rs.getInt(2);
			ArrayList<Integer> list;
			if(error.containsKey(caseid)) {
				list = error.get(caseid);
			} else {
				list = new ArrayList<Integer>();
			}
			list.add(index);
			error.put(caseid, list);
		}
		
		for(String caseid: error.keySet()) {
			ArrayList<Integer> list = error.get(caseid);
			System.out.print("=== case: " + caseid + " error in: ");
			for(int i = 0; i < list.size(); i++) {
				System.out.print(list.get(i) + "\t");
			}
			System.out.println();
		}*/
		
		SimpleResultSet result = new SimpleResultSet();
		result.addColumn("VALID", Types.VARCHAR, 255, 0);
		return result;
	}
}
