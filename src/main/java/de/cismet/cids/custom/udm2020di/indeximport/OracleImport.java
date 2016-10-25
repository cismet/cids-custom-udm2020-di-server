/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport;

import lombok.Getter;

import oracle.jdbc.OracleConnection;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class OracleImport extends OracleExport {

    //~ Static fields/initializers ---------------------------------------------

    public static String EVAL_PARAM = "$messwert";

    //~ Instance fields --------------------------------------------------------

    @Getter protected OracleConnection targetConnection = null;
    protected PreparedStatement insertUniqueTagStmnt = null;
    protected PreparedStatement insertGenericGeomStmnt = null;
    protected PreparedStatement deleteGeomStmnt = null;
    protected final ScriptEngine engine;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new OracleImport object.
     *
     * @param   propertyFile  DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public OracleImport(final InputStream propertyFile) throws IOException,
        ClassNotFoundException,
        SQLException // throws IOException
    {
        super(propertyFile, true);
        this.log = Logger.getLogger(OracleImport.class);

        final ScriptEngineManager manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("js");

        String targetJdbcDriver = null;

        try {
            targetJdbcDriver = this.properties.getProperty("target.jdbc.driver");
            if ((this.sourceJdbcDriver == null) || !targetJdbcDriver.equals(this.sourceJdbcDriver)) {
                Class.forName(targetJdbcDriver);
            }
        } catch (ClassNotFoundException cnfe) {
            log.error("could not find JDBC Driver for target connection: " + targetJdbcDriver, cnfe);
            throw cnfe;
        }

        final String targetJdbcUrl = properties.getProperty("target.jdbc.url");
        final String targetJdbcUsername = properties.getProperty("target.jdbc.username");
        final String targetJdbcPassword = properties.getProperty("target.jdbc.password");
        final String targetJdbcSchema = properties.getProperty("target.jdbc.schema");
        final String oracleLogProperties = this.properties.getProperty("oracle.jdbc.logging.properties");

        this.targetConnection = this.createConnection(
                targetJdbcUrl,
                targetJdbcUsername,
                targetJdbcPassword,
                targetJdbcSchema,
                oracleLogProperties);

        // STATEMENTS ----------------------------------------------------------
        try {
            // prepare generic statements
            final String insertUniqueTagTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/insert-unique-tag.prs.sql"),
                    "UTF-8");
            this.insertUniqueTagStmnt = targetConnection.prepareStatement(insertUniqueTagTpl, new String[] { "ID" });

            final String insertGeomStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/insert-generic-geom.prs.sql"),
                    "UTF-8");
            this.insertGenericGeomStmnt = targetConnection.prepareStatement(
                    insertGeomStatementTpl,
                    new String[] { "ID" });

            final String deleteGeomStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/delete-geom.prs.sql"),
                    "UTF-8");
            this.deleteGeomStmnt = targetConnection.prepareStatement(deleteGeomStatementTpl);
        } catch (SQLException sqeex) {
            log.error("Could not prepare generic statements:" + sqeex.getMessage(), sqeex);
            throw sqeex;
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   targetJdbcUrl        DOCUMENT ME!
     * @param   targetJdbcUsername   DOCUMENT ME!
     * @param   targetJdbcPassword   DOCUMENT ME!
     * @param   targetJdbcSchema     DOCUMENT ME!
     * @param   oracleLogProperties  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected final OracleConnection createConnection(
            final String targetJdbcUrl,
            final String targetJdbcUsername,
            final String targetJdbcPassword,
            final String targetJdbcSchema,
            final String oracleLogProperties) throws SQLException {
        try {
            final OracleConnection connection;

            if ((oracleLogProperties != null) && !oracleLogProperties.isEmpty()) {
                try {
                    this.enableOracleLogging(oracleLogProperties, true);
                    if (log.isDebugEnabled()) {
                        log.debug("Oracle JDBC Logging is enabled");
                    }
                } catch (Exception ex) {
                    log.error("could not enable oracle logging with properties file '"
                                + oracleLogProperties + "'", ex);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Oracle JDBC Logging is disabled");
                }
            }
            connection = (OracleConnection)DriverManager.getConnection(
                    targetJdbcUrl,
                    targetJdbcUsername,
                    targetJdbcPassword);

            if (targetJdbcSchema != null) {
                connection.createStatement().execute("ALTER SESSION set current_schema=" + targetJdbcSchema);
            }

            connection.setAutoCommit(false);
            if (log.isDebugEnabled()) {
                log.debug("DefaultExecuteBatch of targetConnection: "
                            + connection.getDefaultExecuteBatch());
            }

            log.info("TARGET Connection established: " + targetJdbcUrl + "/" + targetJdbcSchema);
            return connection;
        } catch (SQLException sqeex) {
            log.error("Could not connection to target database: " + properties.getProperty("target.jdbc.url"), sqeex);
            throw sqeex;
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   key          DOCUMENT ME!
     * @param   name         DOCUMENT ME!
     * @param   description  DOCUMENT ME!
     * @param   taggroupkey  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertUniqueTag(final String key,
            final String name,
            final String description,
            final String taggroupkey) throws SQLException {
        assert insertUniqueTagStmnt != null : "Statement not yet prepared";

        this.insertUniqueTagStmnt.setString(1, key);
        this.insertUniqueTagStmnt.setString(2, name);
        this.insertUniqueTagStmnt.setString(3, description);
        this.insertUniqueTagStmnt.setString(4, taggroupkey);
        this.insertUniqueTagStmnt.executeUpdate();

        // does not work with MERGE Statements !!!!!
        // ResultSet generatedKeys = this.insertGenericGeom.getGeneratedKeys();
        // if (null != generatedKeys && generatedKeys.next()) {
        // return generatedKeys.getLong(1);
        // } else {
        // log.error("could not fetch generated key for inserted tag!");
        // return -1;
        // }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   X          DOCUMENT ME!
     * @param   Y          DOCUMENT ME!
     * @param   sourceSRS  DOCUMENT ME!
     * @param   targetSRS  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertGeomPoint(final float X, final float Y, final int sourceSRS, final int targetSRS)
            throws SQLException {
        assert insertGenericGeomStmnt != null : "Statement not yet prepared";

        this.insertGenericGeomStmnt.setString(
            1,
            new StringBuilder().append("POINT(").append(X).append(' ').append(Y).append(')').toString());
        this.insertGenericGeomStmnt.setFloat(2, sourceSRS);
        this.insertGenericGeomStmnt.setFloat(3, targetSRS);
        this.insertGenericGeomStmnt.executeUpdate();
        final ResultSet generatedKeys = this.insertGenericGeomStmnt.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedKeys) && generatedKeys.next()) {
            generatedKey = generatedKeys.getLong(1);
            generatedKeys.close();
        } else {
            log.error("could not fetch generated key for inserted geometry!");
        }

        return generatedKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   aggregationValue  DOCUMENT ME!
     * @param   expressionTpl     DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected float convertAggregationValue(final float aggregationValue, final String expressionTpl) {
        if ((aggregationValue != 0) && (expressionTpl != null) && !expressionTpl.isEmpty()) {
            final String expression = expressionTpl.replace(EVAL_PARAM, String.valueOf(aggregationValue));

            try {
                final Object result = this.engine.eval(expression);
                final float convertedAggregationValue = Float.parseFloat(String.valueOf(result));
                return convertedAggregationValue;
            } catch (Throwable ex) {
                log.error("could not evaluate expression '" + expression + "': " + ex.getMessage(), ex);
            }
        }

        return aggregationValue;
    }
}
