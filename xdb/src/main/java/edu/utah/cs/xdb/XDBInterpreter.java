/*
 * Copyright 2016 InitialD Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utah.cs.xdb;

import java.util.Properties;
import java.util.List;
import java.sql.*;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;

public class XDBInterpreter extends Interpreter {

    private Logger logger = LoggerFactory.getLogger(XDBInterpreter.class);
    
    static final char WHITESPACE = ' ';
    static final char NEWLINE = '\n';
    static final char TAB = '\t';
    static final String TABLE_MAGIC_TAG = "%table ";
    static final String EMPTY_COLUMN_VALUE = "";

    static String replaceReservedChars(boolean isTableReponseType, String str) {
        if (str == null) return EMPTY_COLUMN_VALUE;
        return (!isTableReponseType) ? str : str.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE);
    }

    static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432";
    static final String DEFAULT_JDBC_USER_PASSWORD = "";
    static final String DEFAULT_JDBC_USER = "gpadmin";
    static final String DEFAULT_JDBC_DRIVER_NAME = "org.postgresql.Driver";

    static final String POSTGRESQL_SERVER_URL = "postgresql.url";
    static final String POSTGRESQL_SERVER_USER = "postgresql.user";
    static final String POSTGRESQL_SERVER_PASSWORD = "postgresql.password";
    static final String POSTGRESQL_SERVER_DRIVER_NAME = "postgresql.driver.name";

	static {
		Interpreter.register(
			"xdb",
            "xdb",
            XDBInterpreter.class.getName(),
            new InterpreterPropertyBuilder()
                .add(POSTGRESQL_SERVER_URL, DEFAULT_JDBC_URL, "URL for PostgreSQL with XDB")
                .add(POSTGRESQL_SERVER_USER, DEFAULT_JDBC_USER, "PostgreSQL user name")
                .add(POSTGRESQL_SERVER_PASSWORD, DEFAULT_JDBC_USER_PASSWORD, "PostgreSQL user password")
                .add(POSTGRESQL_SERVER_DRIVER_NAME, DEFAULT_JDBC_DRIVER_NAME, "JDBC driver name")
                .build()
		);
	}

    private enum COLUMN_ACTION {
        COPY, CALC_LO_HI
    };

    private Connection jdbcConnection;
    private Statement currentStatement = null;
    private ResultSet currentResultSet = null;
    private StringBuilder currentMsg = null;
    private boolean allRelCINaN = true;
    private int nCols = 0;
    private boolean cancelled = false;
    private boolean withPlanOptimization = false;
    private COLUMN_ACTION[] columnActions;
    private Exception exceptionOnConnect;

    private Connection getJdbcConnection() {
        return jdbcConnection;
    }


    public XDBInterpreter(Properties properties) {
        super(properties);
    }

    @Override
    public void open() {
        
        logger.info("Open psql connection");

        close();
        
        try {
            Properties props = new Properties();
            props.setProperty("user",  getProperty(POSTGRESQL_SERVER_USER));
            props.setProperty("password", getProperty(POSTGRESQL_SERVER_PASSWORD));
            props.setProperty("readOnly", "true");

            String driverName = getProperty(POSTGRESQL_SERVER_DRIVER_NAME);
            String url = getProperty(POSTGRESQL_SERVER_URL);

            Class.forName(driverName);

            jdbcConnection = DriverManager.getConnection(url, props);

            exceptionOnConnect = null;
            logger.info("Sucessfully established connection to psql");

            getJdbcConnection().setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Cannot open connection", e);
            exceptionOnConnect = e;
            close();
        }
    }

    @Override
    public void close() {
        
        logger.info("Close psql connection!");

        try {
            if (getJdbcConnection() != null) {
                getJdbcConnection().close();
            } 
        } catch (SQLException e) {
            logger.error("Cannot close connection", e);
        } finally {
            exceptionOnConnect = null;
        }
    }

    private void cleanUpCurrentSQL() {
        if (null != currentMsg) currentMsg = null;
        if (null != currentResultSet) {
            try {
            currentResultSet.close();
            } catch (SQLException e) {
                logger.error("Cannot close ResultSet", e);
            } finally {
                currentResultSet = null;
            }
        }
        if (null != currentStatement) {
            try {
                currentStatement.close();
            } catch (SQLException e) {
                logger.error("Cannot close Statement", e);
            } finally {
                currentStatement = null;
            }
        }
        try {
            jdbcConnection.commit();
        } catch (SQLException e) {
            logger.warn("Cannot commit", e);
        }
    }

    private void constructColumnHeaders(ResultSetMetaData md) throws SQLException {
        nCols = md.getColumnCount();
        withPlanOptimization = md.getColumnName(1).equals("plan no.");
        
        int startCol;
        if (withPlanOptimization) {
            startCol =  2;
        }
        else {
            startCol = 1;
        }

        for (int i = startCol; i <= startCol + 2; ++i) {
            if (i > startCol) currentMsg.append(TAB);
            currentMsg.append(replaceReservedChars(true, md.getColumnName(i)));
        }
        
        columnActions = new COLUMN_ACTION[nCols - startCol - 2];
        String prevName = "";
        for (int i = startCol + 3; i <= nCols; ++i) {
            String myName = replaceReservedChars(true, md.getColumnName(i));
            if (myName.equals("rel. CI")) {
                currentMsg
                    .append(TAB).append(prevName + "_relCI")
                    .append(TAB).append(prevName + "_lo")
                    .append(TAB).append(prevName + "_hi");
                columnActions[i - startCol - 3] = COLUMN_ACTION.CALC_LO_HI;
            } else {
                currentMsg.append(TAB).append(myName);
                prevName = myName;
                columnActions[i - startCol - 3] = COLUMN_ACTION.COPY;
            }
        }
        currentMsg.append(NEWLINE);
    }

    private boolean appendNextLine() throws SQLException {
        if (currentResultSet.getString(1) == null) {
            /* skip the empty line in group by query */
            return true;
        }

        int startCol;
        if (withPlanOptimization) {
            if (currentResultSet.getString(5) == null) {
                /* skip the plan opt. lines */
                return true;
            }
            startCol = 2; 
        }
        else {
            startCol = 1;
        }

        for (int i = startCol; i <= startCol + 2; ++i) {
            if (i > startCol) currentMsg.append(TAB);
            currentMsg.append(replaceReservedChars(true, currentResultSet.getString(i)));
        }

        for (int i = startCol + 3; i <= nCols; ++i) {
            switch (columnActions[i - startCol - 3]) {
                case COPY:
                    currentMsg.append(TAB).append(replaceReservedChars(true, currentResultSet.getString(i))); 
                    break;

                case CALC_LO_HI:
                    BigDecimal agg = currentResultSet.getBigDecimal(i - 1);
                    
                    String rel_ci_str = currentResultSet.getString(i);
                    if (rel_ci_str.equals("NaN")) {
                      currentMsg.append(TAB).append("NaN")
                        .append(TAB).append("NaN")
                        .append(TAB).append("NaN");
                    }
                    else {
                      BigDecimal rel_ci = currentResultSet.getBigDecimal(i);
                      currentMsg.append(TAB).append(rel_ci);

                      BigDecimal lo = agg.multiply(BigDecimal.ONE.subtract(rel_ci));
                      currentMsg.append(TAB).append(lo.toPlainString());

                      BigDecimal hi = agg.multiply(BigDecimal.ONE.add(rel_ci));
                      currentMsg.append(TAB).append(hi.toPlainString());
                      allRelCINaN = false;
                    }
            }
        }
        currentMsg.append(NEWLINE);

        return false;
    }

    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) {
        try {
            boolean isFirstRow = false;

            if (null == currentMsg) {
                cancelled = false;
                isFirstRow = true;
                currentStatement = getJdbcConnection().createStatement();
                currentStatement.setFetchSize(1);
                currentResultSet = currentStatement.executeQuery(st);

                currentMsg = new StringBuilder();
                currentMsg.append(TABLE_MAGIC_TAG);
                allRelCINaN = true;
            }
            
            if (isFirstRow) {
                ResultSetMetaData md = currentResultSet.getMetaData();

                constructColumnHeaders(md);
            }
            
            for (;;) {
                if (cancelled || !currentResultSet.next()) {
                    InterpreterResult ret;
                    if (allRelCINaN) {
                      ret = new InterpreterResult(Code.SUCCESS,
                          "%html <h3>No sample is ever extractly because the selectivity might be too high. Try full join.</h3>");
                    }
                    else {
                      ret = new InterpreterResult(Code.SUCCESS, currentMsg.toString());
                    }

                    cleanUpCurrentSQL();
                    return ret;
                }

                if (!appendNextLine()) {
                    break; 
                }
            }

            return new InterpreterResult(Code.UPDATE_RESULT, currentMsg.toString());
        } catch (SQLException e) {
            logger.error("SQLException ", e);
            cleanUpCurrentSQL();
            return new InterpreterResult(Code.ERROR, e.getMessage());
        }
    }

    @Override
    public void cancel(InterpreterContext context) {
        cancelled = true;
    }

    @Override
    public FormType getFormType() { return FormType.NONE; }

    @Override public int getProgress(InterpreterContext context) {
        /* need to parse the online agg. query */ 
        return 0;
    }

/*    @Override
    public List<InterpreterCompletion> completion(String buf, int cursor) {
        return java.util.Collections.emptyList();
    } */
}
