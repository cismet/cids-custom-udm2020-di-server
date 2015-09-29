/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import oracle.jdbc.OracleConnection;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.Properties;
import java.util.logging.LogManager;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.serializers.ResultSetSerializer;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class OracleImport extends OracleExport {

    //~ Instance fields --------------------------------------------------------

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
        super(propertyFile, true);
        this.log = Logger.getLogger(OracleImport.class);

        String targetJdbcDriver = null;

        try {
            targetJdbcDriver = properties.getProperty("target.jdbc.driver");

            if ((this.sourceJdbcDriver == null) || !targetJdbcDriver.equals(this.sourceJdbcDriver)) {
                Class.forName(targetJdbcDriver);
            }
        } catch (ClassNotFoundException cnfe) {
            log.error("could not find JDBC Driver for target connection: " + targetJdbcDriver, cnfe);
            throw cnfe;
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

            final String oracleLogProperties = this.properties.getProperty("oracle.jdbc.logging.properties");
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
            targetConnection = DriverManager.getConnection(
                    targetJdbcUrl,
                    targetJdbcUsername,
                    targetJdbcPassword);

            if (targetJdbcSchema != null) {
                targetConnection.createStatement().execute("ALTER SESSION set current_schema=" + targetJdbcSchema);
            }

            targetConnection.setAutoCommit(false);
            if (log.isDebugEnabled()) {
                log.debug("DefaultExecuteBatch of targetConnection: "
                            + ((OracleConnection)targetConnection).getDefaultExecuteBatch());
            }

            log.info("TARGET Connection established: " + targetJdbcUrl + "/" + targetJdbcSchema);
        } catch (SQLException sqeex) {
            log.error("Could not connection to target database: " + targetJdbcUrl, sqeex);
            throw sqeex;
        }

        // STATEMENTS ----------------------------------------------------------
        try {
            // prepare generic statements
            final String insertUniqueTagTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/indeximport/insert-unique-tag.prs.sql"),
                    "UTF-8");
            this.insertUniqueTag = targetConnection.prepareStatement(insertUniqueTagTpl, new String[] { "ID" });

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

        return generatedKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public final Connection getTargerConnection() {
        return targetConnection;
    }
}
