package org.h2.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.api.Trigger;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.table.Table;

public class TriggerDirectlyFollows2 implements Trigger {
	
	Session session;
	Database database;

	public TriggerDirectlyFollows2(Session session, Database database) {
		this.session = session;
		
		System.out.println("masuk trigger");
		
		ArrayList<Table> tables = database.getTableOrViewByName("Event_Log");
        
        System.out.println(tables.size());
        
        Table autodf = tables.get(0);
        Column[] cols = autodf.getColumns();
        
        for(int i = 0; i < cols.length; i++) {
        	System.out.println("cols: " + cols[i].getName());
        }
        
	}

	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		// TODO Auto-generated method stub
		
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
