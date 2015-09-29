/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.dataexport;

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

import de.cismet.cids.custom.udm2020di.serializers.ResultSetSerializer;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class OracleExport {

    //~ Static fields/initializers ---------------------------------------------

    protected static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static final SimpleModule DOUBLE_DESERIALIZER =
        new SimpleModule(
            "DoubleCustomDeserializer",
            new com.fasterxml.jackson.core.Version(1, 0, 0, null)).addDeserializer(
            Double.class,
            new JsonDeserializer<Double>() {

                @Override
                public Double deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException,
                    JsonProcessingException {
                    final String valueAsString = jp.getValueAsString();
                    if ((valueAsString == null) || valueAsString.isEmpty()) {
                        return null;
                    }

                    return Double.parseDouble(valueAsString.replaceAll(",", "\\."));
                }
            });

    public static final SimpleModule FLOAT_DESERIALIZER =
        new SimpleModule(
            "FloatCustomDeserializer",
            new com.fasterxml.jackson.core.Version(1, 0, 0, null)).addDeserializer(
            Float.class,
            new JsonDeserializer<Float>() {

                @Override
                public Float deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException,
                    JsonProcessingException {
                    final String valueAsString = jp.getValueAsString();
                    if ((valueAsString == null) || valueAsString.isEmpty()) {
                        return null;
                    }

                    return Float.parseFloat(valueAsString.replaceAll(",", "\\."));
                }
            });

    public static final XmlMapper XML_MAPPER = new XmlMapper();
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        // JACKSON CONFIG ------------------------------------------------------

        final SimpleModule jsonModule = new SimpleModule();
        jsonModule.addSerializer(new ResultSetSerializer());
        JSON_MAPPER.registerModule(jsonModule);
        JSON_MAPPER.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        // JSON_MAPPER.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        // final JacksonXmlModule xmlModule = new JacksonXmlModule();
        // xmlModule.setKeyDeserializers(new CaseInsensitiveKeyDeserializers());
        // xmlModule.setDefaultUseWrapper(false);
        // XML_MAPPER.registerModule(xmlModule);
        XML_MAPPER.setDateFormat(DATE_FORMAT);
        XML_MAPPER.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        XML_MAPPER.registerModule(DOUBLE_DESERIALIZER);
        XML_MAPPER.registerModule(FLOAT_DESERIALIZER);
        // XML_MAPPER.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }

    //~ Instance fields --------------------------------------------------------

    protected Logger log = Logger.getLogger(OracleExport.class);

    protected String sourceJdbcDriver = null;

    protected Properties properties = null;
    protected Connection sourceConnection;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new OracleImport object.
     *
     * @param   propertyFile  DOCUMENT ME!
     * @param   standalone    DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public OracleExport(final InputStream propertyFile, final boolean standalone) throws IOException,
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

        if (standalone) {
            PropertyConfigurator.configure(properties);
        }

        this.sourceJdbcDriver = properties.getProperty("source.jdbc.driver");
        if ((sourceJdbcDriver != null) && !sourceJdbcDriver.isEmpty()) {
            try {
                Class.forName(sourceJdbcDriver);
            } catch (ClassNotFoundException cnfe) {
                log.error("could not find JDBC Driver for source connection: " + sourceJdbcDriver, cnfe);
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
        } else {
            log.warn("no source connection specified!");
        }
    }

    //~ Methods ----------------------------------------------------------------

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
    protected final String xmlClobToJsonString(final Clob xmlClob) throws IOException, SQLException {
        final JsonNode node = XML_MAPPER.readTree(xmlClob.getCharacterStream());
        return JSON_MAPPER.writeValueAsString(node);
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
    protected final int[] executeBatchStatement(final Connection connection, final String batchStatementStr)
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

    /**
     * DOCUMENT ME!
     *
     * @param   oracleLogProperties  DOCUMENT ME!
     * @param   logDriver            DOCUMENT ME!
     *
     * @throws  MalformedObjectNameException    DOCUMENT ME!
     * @throws  NullPointerException            DOCUMENT ME!
     * @throws  AttributeNotFoundException      DOCUMENT ME!
     * @throws  InstanceNotFoundException       DOCUMENT ME!
     * @throws  MBeanException                  DOCUMENT ME!
     * @throws  ReflectionException             DOCUMENT ME!
     * @throws  InvalidAttributeValueException  DOCUMENT ME!
     * @throws  SecurityException               DOCUMENT ME!
     * @throws  IOException                     DOCUMENT ME!
     */
    protected final void enableOracleLogging(final String oracleLogProperties, final boolean logDriver)
            throws MalformedObjectNameException,
                NullPointerException,
                AttributeNotFoundException,
                InstanceNotFoundException,
                MBeanException,
                ReflectionException,
                InvalidAttributeValueException,
                SecurityException,
                IOException {
        oracle.jdbc.driver.OracleLog.setTrace(true);

        // compute the ObjectName
        final String loader = Thread.currentThread().getContextClassLoader().toString().replaceAll("[,=:\"]+", "");
        final javax.management.ObjectName name = new javax.management.ObjectName(
                "com.oracle.jdbc:type=diagnosability,name="
                        + loader);

        // get the MBean server
        final javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer();

        // find out if logging is enabled or not
        log.info("Oracle JDBC Driver LoggingEnabled = " + mbs.getAttribute(name, "LoggingEnabled"));

        // enable logging
        mbs.setAttribute(name, new javax.management.Attribute("LoggingEnabled", true));

        // File propFile = new File(oracleLogProperties);
        final LogManager logManager = LogManager.getLogManager();
        if (log.isDebugEnabled()) {
            log.debug("loading logging.properties file from " + this.getClass().getResource(oracleLogProperties));
        }
        logManager.readConfiguration(this.getClass().getResourceAsStream(oracleLogProperties));

        if (logDriver) {
            DriverManager.setLogWriter(new PrintWriter(System.err));
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   <T>        DOCUMENT ME!
     * @param   resultSet  DOCUMENT ME!
     * @param   type       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  JsonProcessingException  DOCUMENT ME!
     */
    public final <T> T deserializeResultSet(final ResultSet resultSet, final Class<T> type)
            throws JsonProcessingException {
        // deserialize to tree model
        final JsonNode resultSetNode = JSON_MAPPER.valueToTree(resultSet);

        // deserilaize to object
        final T mappedObject = JSON_MAPPER.treeToValue(resultSetNode, type);

        return mappedObject;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public final Connection getSourceConnection() {
        return sourceConnection;
    }
}
