/**
 *  Author: Saket Singh
 *  UT EID: sks637
 *  Last Modified: November 6, 2017
 *
 */

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.concurrent.ThreadLocalRandom;

public class test {

    public static ArrayList<Integer> sorted = new ArrayList<Integer>(3000000);
    public static ArrayList<Integer> random = new ArrayList<Integer>(3000000);

    // Initializes sorted and random integer key arrays
    public static void initArray() {
        System.out.println("Initializing Linear Array");
        for (int i = 1; i < 3000001; i++) {
            sorted.add(i);
        }
        System.out.println("Done Initializing Linear Array");
        System.out.println("Initializing Random Array");
        for (int i = 1; i < 3000001; i++) {
            random.add(i);
        }
        Collections.shuffle(random);
        System.out.println("Done Initializing Arrays");
    }

	public static String randomString() {
	    StringBuilder result = new StringBuilder("");
	    String alpha = "abcdefghijklmnopqrstuvwxyz";
	    for (int i = 0; i < 247; i++) {
	        int key = ThreadLocalRandom.current().nextInt(25);
	        result.append(alpha.charAt(key));
        }
        return result.toString();
    }

    public static void createTable(Connection conn) throws SQLException {
		System.out.println("creating table");
	    String createString = 
	    	"CREATE TABLE benchmark ("+
	     	"theKey INTEGER PRIMARY KEY," +
	     	"columnA INTEGER," +
			"columnB INTEGER," +
			"filler CHAR(247)" +
			")";
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(createString);
	    stmt.close();
	}

	public static void insertRowSorted(Connection conn) throws SQLException {
        // Batch insertion with primary keys in ascending order
        System.out.println("inserting row");
        int colA, colB;
        colA = colB = 0;
        ListIterator<Integer> intKey = sorted.listIterator();
        for (int i = 0; i < 3000; i++) {
            StringBuilder insertString = new StringBuilder("");
            insertString.append("INSERT INTO benchmark (theKey, columnA, columnB, filler) VALUES");
            for (int j = 0; j < 999; j++) {
                colA = ThreadLocalRandom.current().nextInt(50000) + 1;
                colB = ThreadLocalRandom.current().nextInt(50000) + 1;
                String rs = randomString();
                insertString.append("(" + intKey.next() + "," + colA + "," + colB + "," + "'" + rs + "'" + "), ");
            }
            String rs = randomString();
            insertString.append("(" + intKey.next() + "," + colA + "," + colB + "," + "'" + rs + "'" + ")");
            String output = insertString.toString();
            //System.out.println("output = " + output);
		    Statement stmt = conn.createStatement();
		    stmt.executeUpdate(output);
            stmt.close();
        }
	}

    public static void insertRowRandom(Connection conn) throws SQLException {
        // Batch insertion with primary keys in random order
        System.out.println("inserting row");
        int colA, colB;
        colA = colB = 0;
        ListIterator<Integer> intKey = random.listIterator();
        for (int i = 0; i < 3000; i++) {
            StringBuilder insertString = new StringBuilder("");
            insertString.append("INSERT INTO benchmark (theKey, columnA, columnB, filler) VALUES");
            for (int j = 0; j < 999; j++) {
                colA = ThreadLocalRandom.current().nextInt(50000) + 1;
                colB = ThreadLocalRandom.current().nextInt(50000) + 1;
                String rs = randomString();
                insertString.append("(" + intKey.next() + "," + colA + "," + colB + "," + "'" + rs + "'" + "), ");
            }
            String rs = randomString();
            insertString.append("("+intKey.next()+","+colA+","+colB+","+"'"+rs+"'"+")");
            String output = insertString.toString();
            //System.out.println("output = " + output);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(output);
            stmt.close();
        }
    }

	public static void printTable(Connection conn) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"SELECT * FROM benchmark";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		while (rs.next()) {
	    	System.out.println(rs.getString(1) + "," + rs.getString(2)
			+ "," + rs.getString(3) + "," + rs.getString(4));
		}
		rs.close();
	    stmt.close();
	}

	public static void dropTable(Connection conn) throws SQLException {
		System.out.println("dropping table");
	    String dropString = 
	    	"DROP TABLE benchmark";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(dropString);
	    stmt.close();
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		System.out.println("loading driver");
		Class.forName("org.postgresql.Driver");
		System.out.println("driver loaded");
		System.out.println("Connecting to DB");
		Connection conn = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "postgres");
		System.out.println("Connected to DB");

		try {
			// drops if there
			dropTable(conn);
		}
		catch (SQLException e) {}

		long beginTime, endTime;
        System.out.println();
        initArray();
        System.out.println();

        // VARIATION I: Execute timed insertion of 3 million rows sorted on primary key
		System.out.println("VARIATION I:");
        beginTime = System.currentTimeMillis();
		createTable(conn);
		insertRowSorted(conn);
		dropTable(conn);
		endTime = System.currentTimeMillis();
		System.out.println("Elapsed time: " + (endTime - beginTime) + " milliseconds");
        System.out.println();

        // VARIATION II: Execute timed insertion of 3 million rows with random primary key
        System.out.println("VARIATION II:");
        beginTime = System.currentTimeMillis();
        createTable(conn);
        insertRowRandom(conn);
        dropTable(conn);
        endTime = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (endTime - beginTime) + " milliseconds");
	}
}