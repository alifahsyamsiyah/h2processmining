package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.h2.api.Trigger;

public class TriggerRuleChecking implements Trigger {
	
    private static final Map<String, TriggerRuleChecking> TRIGGERS =
            Collections.synchronizedMap(new HashMap<String, TriggerRuleChecking>());
    private HashMap<Integer, AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>>> rules;
	private HashMap<Integer, AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>>> rulesWithToken;
	private HashMap<String, Integer> eventClassToInt;
	private Integer totalEventClasses;
	private HashMap<Integer, ArrayList<Integer>> rulesOfaLabel;
	private HashMap<Integer, ArrayList<Integer>> rulesTokenOfaLabel;
	private String tableName;
	
	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {
		TRIGGERS.put(triggerName, this);
		System.out.println("init trigger " + schemaName + " " + triggerName + " " + tableName + " " + before +" " + type);
		this.tableName = tableName;
	}
	
	public static void setRule(String triggerName, String rulesString, String ruleTokenString, String eventClassIntString, String ruleOfaLabelString, String ruleTokenOfaLabelString) throws SQLException {
		
		HashMap<Integer, AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>>> ruleSet = new HashMap<Integer, AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>>>();
		String[] rules =  rulesString.split(";");
		for(int i = 0; i < rules.length; i++) {
			String[] aRule = rules[i].split(">>");
			Integer ruleNumber = Integer.parseInt(aRule[0]);
			String[] theRule = aRule[1].split("=");
			String[] in = theRule[0].split(",");
			String[] out = theRule[1].split(",");
			
			ArrayList<Integer> listIn = new ArrayList<Integer>();
			ArrayList<Integer> listOut = new ArrayList<Integer>();
			for(int j = 0; j < in.length && !in[j].isEmpty(); j++) {
				listIn.add(Integer.parseInt(in[j]));
			}
			for(int j = 0; j < out.length && !out[j].isEmpty(); j++) {
				listOut.add(Integer.parseInt(out[j]));
			}
			
			AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>> map = new SimpleEntry<ArrayList<Integer>,ArrayList<Integer>>(listIn, listOut);
			ruleSet.put(ruleNumber, map);
		}
		
		HashMap<Integer, AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>>> ruleTokenSet = new HashMap<Integer, AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>>>();
		String[] rulesToken = ruleTokenString.split(";");
		for(int i = 0; i < rulesToken.length; i++) {
			String[] aRule = rulesToken[i].split(">>");
			Integer ruleNumber = Integer.parseInt(aRule[0]);
			String[] theRule = aRule[1].split("=");
			Integer token = Integer.parseInt(theRule[0]);
			String[] in = theRule[1].split(",");
			String[] out = theRule[2].split(",");
						
			ArrayList<Integer> listIn = new ArrayList<Integer>();
			ArrayList<Integer> listOut = new ArrayList<Integer>();
			for(int j = 0; j < in.length && !in[j].isEmpty(); j++) {
				listIn.add(Integer.parseInt(in[j]));
			}
			for(int j = 0; j < out.length && !out[j].isEmpty(); j++) {
				listOut.add(Integer.parseInt(out[j]));
			}
			
			AbstractMap.SimpleEntry<Integer,ArrayList<Integer>> mapToken = new SimpleEntry<Integer,ArrayList<Integer>>(token, listIn);
			AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>> map = new SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>>(mapToken, listOut);
			ruleTokenSet.put(ruleNumber, map);			
 		}
		
		HashMap<String, Integer> eventClassInt = new HashMap<String, Integer>();
		String[] eventClassIntArray = eventClassIntString.split(";");
		for(int i = 0; i < eventClassIntArray.length; i++) {
			String[] aMap = eventClassIntArray[i].split("=");
			String act = aMap[0];
			Integer actInt = Integer.parseInt(aMap[1]);
			eventClassInt.put(act, actInt);
		}
		
		HashMap<Integer,ArrayList<Integer>> ruleLabel = new HashMap<Integer,ArrayList<Integer>>();
		String[] ruleOfaLabelArray = ruleOfaLabelString.split(";");
		for(int i = 0; i < ruleOfaLabelArray.length; i++) {
			String[] aMap = ruleOfaLabelArray[i].split("=");
			Integer act = Integer.parseInt(aMap[0]);
			String[] ruleNumbers = aMap[1].split(",");
			ArrayList<Integer> ruleNumbersList = new ArrayList<Integer>();
			for(int j = 0; j < ruleNumbers.length; j++) {
				ruleNumbersList.add(Integer.parseInt(ruleNumbers[j]));
			}
			ruleLabel.put(act, ruleNumbersList);			
		}
		
		HashMap<Integer,ArrayList<Integer>> ruleTokenLabel = new HashMap<Integer,ArrayList<Integer>>();
		String[] ruleTokenOfaLabelArray = ruleTokenOfaLabelString.split(";");
		for(int i = 0; i < ruleTokenOfaLabelArray.length; i++) {
			String[] aMap = ruleTokenOfaLabelArray[i].split("=");
			Integer act = Integer.parseInt(aMap[0]);
			String[] ruleNumbers = aMap[1].split(",");
			ArrayList<Integer> ruleNumbersList = new ArrayList<Integer>();
			for(int j = 0; j < ruleNumbers.length; j++) {
				ruleNumbersList.add(Integer.parseInt(ruleNumbers[j]));
			}
			ruleTokenLabel.put(act, ruleNumbersList);			
		}
		
		TRIGGERS.get(triggerName).rules = ruleSet;
	    TRIGGERS.get(triggerName).rulesWithToken = ruleTokenSet;
	    TRIGGERS.get(triggerName).eventClassToInt = eventClassInt;
	    TRIGGERS.get(triggerName).totalEventClasses = eventClassInt.size(); 
	    TRIGGERS.get(triggerName).rulesOfaLabel = ruleLabel;
	    TRIGGERS.get(triggerName).rulesTokenOfaLabel = ruleTokenLabel;
        System.out.println("set rule");
	}

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {	
		System.out.println("fire");
		Statement stat = conn.createStatement();
		ResultSet rs;
		
		// a new inserted row
		String newCase = (String) newRow[0];
		String newEC = (String) newRow[1];
		Integer newECInt = eventClassToInt.get(newEC); 
		Integer[] eventClassOccurrence = new Integer[totalEventClasses];
		Integer index = 0; Integer occurrence = 1;
		
		// get previous occurrence and index
		String query = "select " + newEC + ", lastindex from EventClassOccurrence" + tableName + " where caseid = '"+ newCase + "'";
		rs = stat.executeQuery(query);
		while(rs.next()) {
			Integer prevOccurrence = rs.getInt(1);
			Integer prevIndex = rs.getInt(2);
			occurrence = prevOccurrence + 1;
			index = prevIndex + 1;
		}
		
		// a new case inserted
		if(index == 0) {
			query = "insert into EventClassOccurrence"+tableName+"(caseid,"+ newEC + ", lastindex) values ('"+ newCase +"',1,0)";
			stat.execute(query);
		} else { // update new occurrence and new index			
			query = "update EventClassOccurrence"+tableName+" set " + newEC + " = " + occurrence + ", lastindex = " + index + " where caseid = '"+ newCase + "'";
			stat.execute(query);
		}
		
		// get occurrence for each event class
		rs = stat.executeQuery("select * from EventClassOccurrence"+tableName+" where caseid = '"+ newCase + "'");
		while(rs.next()){
			for(int i = 0; i< totalEventClasses; i++) {
				eventClassOccurrence[i] = rs.getInt(i+2);				
			}
		}
		
		// check rule
		Boolean valid = true;
		
		if(rulesOfaLabel.containsKey(newECInt)) {
			ArrayList<Integer> associatedRules = rulesOfaLabel.get(newECInt);
			for(int j = 0; j < associatedRules.size(); j++) {
				Integer ruleNumber = associatedRules.get(j);
				AbstractMap.SimpleEntry<ArrayList<Integer>,ArrayList<Integer>> pair = rules.get(ruleNumber);
				ArrayList<Integer> leftSideRule = pair.getKey();
				ArrayList<Integer> rightSideRule = pair.getValue();
				
				Integer leftValue = 0;
				Integer rightValue = 0;
			
				for(int i = 0; i < leftSideRule.size(); i++) {
					int eventClassIndex = leftSideRule.get(i);
					int occ = 0;
					if(eventClassOccurrence[eventClassIndex] != null)
						occ = eventClassOccurrence[eventClassIndex];
					leftValue += occ;
				}
				for(int i = 0; i < rightSideRule.size(); i++) {
					int eventClassIndex = rightSideRule.get(i);
					int occ = 0;
					if(eventClassOccurrence[eventClassIndex] != null)
						occ = eventClassOccurrence[eventClassIndex];
					rightValue += occ;
				}
				
				// violate the rule
				if(leftValue < rightValue) {
					valid = false;
					
					rs = stat.executeQuery("select * from errorlog"+tableName+" where caseid = '" + newCase + "' and rulenumber = " + ruleNumber);
					if(!rs.next()){
						stat.execute("insert into errorlog"+tableName+" values('" + newCase + "'," + index + "," + ruleNumber+")");
					}
					
					break;				
				}
			}
		}
		
		// check rule with token
		if(valid ==  true && rulesTokenOfaLabel.containsKey(newECInt)) {
			ArrayList<Integer> associatedRulesToken = rulesTokenOfaLabel.get(newECInt);
			for(int j = 0; j < associatedRulesToken.size(); j++) {
				Integer ruleNumber = associatedRulesToken.get(j);
				AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Integer,ArrayList<Integer>>, ArrayList<Integer>> pair = rulesWithToken.get(ruleNumber);
				AbstractMap.SimpleEntry<Integer,ArrayList<Integer>> pair2 = pair.getKey();
				ArrayList<Integer> rightSideRule = pair.getValue();
				Integer token = pair2.getKey();
				ArrayList<Integer> leftSideRule = pair2.getValue();
				
				Integer leftValue = 0;
				Integer rightValue = 0;
			
				for(int i = 0; i < leftSideRule.size(); i++) {
					int eventClassIndex = leftSideRule.get(i);
					int occ = 0;
					if(eventClassOccurrence[eventClassIndex] != null)
						occ = eventClassOccurrence[eventClassIndex];
					leftValue += occ;
				}
				leftValue += token;
				for(int i = 0; i < rightSideRule.size(); i++) {
					int eventClassIndex = rightSideRule.get(i);
					int occ = 0;
					if(eventClassOccurrence[eventClassIndex] != null)
						occ = eventClassOccurrence[eventClassIndex];
					rightValue += occ;
				}
				
				// violate the rule
				if(leftValue < rightValue) {
					valid = false;
					
					rs = stat.executeQuery("select * from errorlog"+tableName+" where caseid = '" + newCase + "' and rulenumber = " + ruleNumber);
					if(!rs.next()){
						stat.execute("insert into errorlog"+tableName+" values('" + newCase + "'," + index + "," + ruleNumber+")");
					}
					
					break;
					
				}
			}
		}		
	
	}	

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove() throws SQLException {
		// TODO Auto-generated method stub
		
	}

}
