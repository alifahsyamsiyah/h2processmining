package nl.tue.is.alifah.test.slidingwindow;

import java.sql.Timestamp;

public class Event {
	String traceID;
	Integer eventID;
	String task;
	String resource;
	Timestamp time;
	String lifecycle;
	int year;
	int month;
	int date;
	long scaledEpoch;
	Integer sequence;
	
	public Event(Integer eventID) {
		this.eventID = eventID;
	}
}
