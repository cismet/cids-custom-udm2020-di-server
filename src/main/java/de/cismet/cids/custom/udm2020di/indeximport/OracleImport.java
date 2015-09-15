/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class OracleImport {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger log = Logger.getLogger(OracleImport.class);

    //~ Instance fields --------------------------------------------------------

    protected XmlMapper xmlMapper = new XmlMapper();
    protected ObjectMapper jsonMapper = new ObjectMapper();

    protected Properties properties = null;
    protected Connection sourceConnection;
    protected Connection targetConnection = null;
    protected PreparedStatement insertUniqueTag = null;
    protected PreparedStatement insertGenericGeom = null;

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
        final InputStreamReader isr = new InputStreamReader(propertyFile);
        final BufferedReader br = new BufferedReader(isr);

        properties = new Properties();
        try {
            properties.load(br);
        } catch (IOException ioex) {
            BasicConfigurator.configure();
            log.error("could not load properties file: " + ioex.getMessage(), ioex);
            throw ioex;
        }
        
        PropertyConfigurator.configure(properties);
        
        String sourceJdbcDriver = null;

        try {
            sourceJdbcDriver = properties.getProperty("source.jdbc.driver");
            Class.forName(sourceJdbcDriver);
        } catch (ClassNotFoundException cnfe) {
            log.error("could not find JDBC Driver for source connection: " + sourceJdbcDriver, cnfe);
            throw cnfe;
        }

        String targetJdbcDriver = null;

        try {
            targetJdbcDriver = properties.getProperty("target.jdbc.driver");
            if (!targetJdbcDriver.equals(sourceJdbcDriver)) {
                Class.forName(targetJdbcDriver);
            }
        } catch (ClassNotFoundException cnfe) {
            log.error("could not find JDBC Driver for target connection: " + targetJdbcDriver, cnfe);
            throw cnfe;
        }

        String sourceJdbcUrl = null;
        final String sourceJdbcUsername;
        final String sourceJdbcPassword;
        final String sourceJdbcSchema;

        try {
            sourceJdbcUrl = properties.getProperty("source.jdbc.url");
            sourceJdbcUsername = properties.getProperty("source.jdbc.username");
            sourceJdbcPassword = properties.getProperty("source.jdbc.password");
            sourceJdbcSchema = properties.getProperty("source.jdbc.schema");

            sourceConnection = DriverManager.getConnection(
                    sourceJdbcUrl,
                    sourceJdbcUsername,
                    sourceJdbcPassword);

            if (sourceJdbcSchema != null) {
                sourceConnection.createStatement().execute("ALTER SESSION set current_schema=" + sourceJdbcSchema);
            }
            log.info("SOURCE Connection established: " + sourceJdbcUrl + "/" + sourceJdbcSchema);
            
        } catch (SQLException sqeex) {
            log.error("Could not connection to source database: " + sourceJdbcUrl, sqeex);
            throw sqeex;
        }

        String targetJdbcUrl = null;
        final String targetJdbcUsername;
        final String targetJdbcPassword;
        final String targetJdbcSchema;

        try {
            targetJdbcUrl = properties.getProperty("target.jdbc.url");
            targetJdbcUsername = properties.getProperty("target.jdbc.username");
            targetJdbcPassword = properties.getProperty("target.jdbc.password");
            targetJdbcSchema = properties.getProperty("target.jdbc.schema");

            targetConnection = DriverManager.getConnection(
                    targetJdbcUrl,
                    targetJdbcUsername,
                    targetJdbcPassword);

            if (targetJdbcSchema != null) {
                targetConnection.createStatement().execute("ALTER SESSION set current_schema=" + targetJdbcSchema);
            }

            targetConnection.setAutoCommit(false);
            log.info("TARGET Connection established: " + targetJdbcUrl + "/" + targetJdbcSchema);
        } catch (SQLException sqeex) {
            log.error("Could not connection to target database: " + targetJdbcUrl, sqeex);
            throw sqeex;
        }

        try {
            // prepare generic statements
            final String getSitesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/insert-unique-tag.prs.sql"),
                    "UTF-8");
            this.insertUniqueTag = targetConnection.prepareStatement(getSitesStatementTpl, new String[] { "ID" });

            final String insertGeomStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/insert-generic-geom.prs.sql"),
                    "UTF-8");
            this.insertGenericGeom = targetConnection.prepareStatement(insertGeomStatementTpl, new String[] { "ID" });
        } catch (SQLException sqeex) {
            log.error("Could not prepare generic statements:" + sqeex.getMessage(), sqeex);
            throw sqeex;
        }
    }

    //~ Methods ----------------------------------------------------------------

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
        assert insertUniqueTag != null : "Statement not yet prepared";

        this.insertUniqueTag.setString(1, key);
        this.insertUniqueTag.setString(2, name);
        this.insertUniqueTag.setString(3, description);
        this.insertUniqueTag.setString(4, taggroupkey);
        this.insertUniqueTag.executeUpdate();

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
     * @param   xmlClob  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    protected String xmlClobToJsonString(final Clob xmlClob) throws IOException, SQLException {
        final JsonNode node = xmlMapper.readTree(xmlClob.getCharacterStream());
        return jsonMapper.writeValueAsString(node);
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
        assert insertGenericGeom != null : "Statement not yet prepared";

        this.insertGenericGeom.setString(
            1,
            new StringBuilder().append("POINT(").append(X).append(' ').append(Y).append(')').toString());
        this.insertGenericGeom.setFloat(2, sourceSRS);
        this.insertGenericGeom.setFloat(3, targetSRS);
        this.insertGenericGeom.executeUpdate();
        final ResultSet generatedKeys = this.insertGenericGeom.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedKeys) && generatedKeys.next()) {
            generatedKey = generatedKeys.getLong(1);
            generatedKeys.close();
        } else {
            log.error("could not fetch generated key for inserted geometry!");
        }

        this.insertGenericGeom.close();
        return generatedKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   connection         DOCUMENT ME!
     * @param   batchStatementStr  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected int[] executeBatchStatement(final Connection connection, final String batchStatementStr)
            throws SQLException {
        final String[] batchStatements = batchStatementStr.split(";");
        final Statement batchStatement = connection.createStatement();
        for (final String StatementStr : batchStatements) {
//            if (log.isDebugEnabled()) {
//                log.debug(StatementStr);
//            }
            batchStatement.addBatch(StatementStr);
        }

        final int[] ret = batchStatement.executeBatch();

        connection.commit();
        batchStatement.close();

        return ret;
    }
}
