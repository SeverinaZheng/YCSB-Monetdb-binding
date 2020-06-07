package monetdb;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import com.yahoo.ycsb.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.*;
import java.util.*;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class MonetdbClient extends DB {

  /** Count the number of times initialized to teardown on the last. */
  //private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** Cache for already prepared statements. */
  private static ConcurrentMap<StatementType, PreparedStatement> cachedStatements;

  /** The driver to get the connection to postgresql. */
  //private static Driver postgrenosqlDriver;

  /** The connection to the database. */
  private static Connection connection;

  public static final int Ok=0;
  public static final int ServerError=-1;
  public static final int HttpError=-2;
  public static final int NoMatchingRecord=-3;

  /** The class to use as the jdbc driver. */
  //public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "jdbc:monetdb://192.168.43.101:54321/test";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "monetdb";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "monetdb";

  /** The JDBC connection auto-commit property for the driver. */
  //public static final String JDBC_AUTO_COMMIT = "postgrenosql.autocommit";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  /*private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }*/

  //@Override
  public void init() throws DBException {
      String urls = CONNECTION_URL;
      String user = CONNECTION_USER;
      String passwd = CONNECTION_PASSWD;
      //boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

      try {
        /*Properties tmpProps = new Properties();
        tmpProps.setProperty("user", user);
        tmpProps.setProperty("password", passwd);*/
    	Connection con = DriverManager.getConnection(urls, user, passwd);
      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

        /*postgrenosqlDriver = new Driver();
        connection = postgrenosqlDriver.connect(urls, tmpProps);
        connection.setAutoCommit(autoCommit);*/

      } catch (Exception e) {
        System.err.println("Error during initialization: " + e);
      }
  }

  //@Override
  public void cleanup() throws DBException {
      try {
        cachedStatements.clear();
       /* if (!connection.getAutoCommit()){
          connection.commit();
        }*/
        connection.close();
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
      }
  }

  //@Override
  public int read(String tableName, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return  NoMatchingRecord;
      }

      if (result != null) {
        if (fields == null){
          while (resultSet.next()){
            String ans = resultSet.getString("YCSB_VALUE");
            JSONObject jsonObj;
            try{
               jsonObj = (JSONObject)(new JSONParser().parse(ans));
            }catch(ParseException e){
                System.out.println("Error in processing read of table " + tableName + ": " + e);
                return ServerError;
            }
            for(Object string :jsonObj.keySet()){
              String str = (String) string;
      				String value = (String)jsonObj.get(str);
      				result.put(str, new StringByteIterator(value));
      			}
          }
        } else {
          int i = 1;
          for (String field : fields) {
            String value = resultSet.getString(i);
            result.put(field, new StringByteIterator(value));
            i++;
          }
        }
      }
      resultSet.close();
      return Ok;

    } catch (SQLException e) {
      System.out.println("Error in processing read of table " + tableName + ": " + e);
      return ServerError;
    }
  }

  //@Override
  public int scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type);
      }
      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          int j = 1;
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }

          result.add(values);
        }
      }

      resultSet.close();
      return Ok;
    } catch (SQLException e) {
      System.out.println("Error in processing scan of table: " + tableName + ": " + e);
      return ServerError;
    }
  }

  //@Override
  public int update(String tableName, String key, HashMap<String,ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }


      updateStatement.setString(1, jsonObject.toString());
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Ok;
      }
      return ServerError;
    } catch (SQLException e) {
      System.out.println("Error in processing update to table: " + tableName + e);
      return ServerError;
    }
  }

  //@Override
  public int insert(String tableName, String key, HashMap<String,ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      insertStatement.setString(2, jsonObject.toString());
      insertStatement.setString(1, key);

      int result = insertStatement.executeUpdate();
      if (result == 1) {
        return Ok;
      }

      return ServerError;
    } catch (SQLException e) {
      System.out.println("Error in processing insert to table: " + tableName + ": " + e);
      return ServerError;
    }
  }

  //@Override
  public int delete(String tableName, String key) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type);
      }
      deleteStatement.setString(1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1){
        return Ok;
      }

      return ServerError;
    } catch (SQLException e) {
      System.out.println("Error in processing delete to table: " + tableName + e);
      return ServerError;
    }
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType)
      throws SQLException{
    PreparedStatement readStatement = connection.prepareStatement(createReadStatement(readType));
    PreparedStatement statement = cachedStatements.putIfAbsent(readType, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

  private String createReadStatement(StatementType readType){
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", " + COLUMN_NAME);
    } else {
      for (String field:readType.getFields()){
        read.append(",json.filter( " + COLUMN_NAME + ",'" + field+"')");
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException{
    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
    PreparedStatement statement = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createScanStatement(StatementType scanType){
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY);
    if (scanType.getFields() != null){
      for (String field:scanType.getFields()){
        scan.append(",json.filter( " + COLUMN_NAME + ",'" + field+"')");
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
    scan.append(PRIMARY_KEY);
    scan.append(" >= ?");
    scan.append(" ORDER BY ");
    scan.append(PRIMARY_KEY);
    scan.append(" LIMIT ?");

    return scan.toString();
  }

  public PreparedStatement createAndCacheUpdateStatement(StatementType updateType)
      throws SQLException{
    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
    PreparedStatement statement = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }

  private String createUpdateStatement(StatementType updateType){
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(COLUMN_NAME);
    update.append(" = ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException{
    PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(insertType));
    PreparedStatement statement = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (statement == null) {
      return insertStatement;
    }
    return statement;
  }

  private String createInsertStatement(StatementType insertType){
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    //insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException{
    PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(deleteType));
    PreparedStatement statement = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType){
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
