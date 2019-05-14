package com.bic.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class DbObject {
    private static Logger logger = LogManager.getLogger(DbObject.class);

    Connection conn = null;


    public static Connection getConnection() throws Exception {
        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@CLWSLWCCDB01D:10670:WCCDEV";
        String username = "DEV_OCS";
        String password = "B1cl3rwat$r";
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);

        return conn;
    }

    public boolean runInsert(Integer _uniqueId, String _headerid,
                             String _docName) throws SQLException {
        boolean good = false;
        PreparedStatement pstmt = null;
        //TODO fix columens and any variables
        String query =
            "insert into AFOBJECTS(DAFID, dafapplication , dafbusinessobjecttype, dafbusinessobject,ddocname) values(?, ?, ?,?,?)";


        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(query); // create a statement
            pstmt.setInt(1, _uniqueId);
            pstmt.setString(2, "EBS_instanceA");
            pstmt.setString(3, "OE_ORDER_HEADERS");
            pstmt.setString(4, _headerid);
            pstmt.setString(5, _docName);
            pstmt.executeUpdate(); // execute insert statement
            good = true;
        } catch (Exception e) {
            e.printStackTrace();
            good = false;
        } finally {
            pstmt.close();
            conn.close();
            good = false;
        }

        return good;
    }

    public ResultSet test() throws Exception {
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;
        //TODO fix columens and any variables
        String query = "select * from AFOBJECTS";
        logger.info(query);
        conn = getConnection();
        try {
            stmt = conn.createStatement(); // create a statement

            rs = stmt.executeQuery(query); // execute insert statement
            while (rs.next()) {
                logger.info(rs.getString(1));
                logger.info(rs.getString(2));
                logger.info(rs.getString(3));
                logger.info(rs.getString(4));
                logger.info(rs.getString(5));
            }


            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt.close();
            conn.close();
        }
        return rs;

    }


    public static void main(String[] args) throws Exception {
        DbObject dbo = new DbObject();
        dbo.test();


        //		dbo.runInsert("headerId", "docName");


    }
}


