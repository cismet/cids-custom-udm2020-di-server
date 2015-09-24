/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.boris;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.indeximport.boris.BorisImport;
import de.cismet.cids.custom.udm2020di.indeximport.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.types.Parameter;

import de.cismet.cids.server.actions.ServerAction;
import de.cismet.cids.server.actions.ServerActionParameter;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class BorisExportAction extends OracleExport implements ServerAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "borisExportAction";
    public static final String PARAM_STANDORTE = "standorte";
    public static final String PARAM_PARAMETER = "parameter";

    private static final Logger LOG = Logger.getLogger(BorisExportAction.class);

    //~ Instance fields --------------------------------------------------------

    protected final String decodeSampleValuesStatementTpl;
    protected final String exportBorisMesswerteStatementTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsvExportAction object.
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public BorisExportAction() throws IOException, ClassNotFoundException, SQLException {
        super(BorisImport.class.getResourceAsStream("boris.properties"), false);
        this.log = Logger.getLogger(BorisExportAction.class);
        log.info("new BorisExportAction created");

        this.decodeSampleValuesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/boris/decode-boris-messwerte.tpl.sql"),
                "UTF-8");

        this.exportBorisMesswerteStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/boris/export-boris-messwerte.tpl.sql"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   standortPks  DOCUMENT ME!
     * @param   parameters   DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createExportBorisMesswerteStatement(
            final Collection<String> standortPks,
            final Collection<Parameter> parameters) {
        if (log.isDebugEnabled()) {
            log.debug("creating export statements for " + standortPks.size() + "standorte and "
                        + parameters.size() + parameters);
        }

        final StringBuilder standorteBuilder = new StringBuilder();
        final Iterator<String> standortPksIterator = standortPks.iterator();
        while (standortPksIterator.hasNext()) {
            standorteBuilder.append('\'').append(standortPksIterator.next()).append('\'');
            if (standortPksIterator.hasNext()) {
                standorteBuilder.append(',');
            }
        }

        final StringBuilder decodeBuilder = new StringBuilder();
        final StringBuilder parameterBuilder = new StringBuilder();
        final Iterator<Parameter> parametersIterator = parameters.iterator();
        while (parametersIterator.hasNext()) {
            final Parameter parameter = parametersIterator.next();

            // decode stuff
            String tmpDecodeString = this.decodeSampleValuesStatementTpl.replace(
                    "%PARAMETER_PK%",
                    parameter.getParameterPk());
            tmpDecodeString = tmpDecodeString.replace("%PARAMETER_NAME%", parameter.getParameterName());
            decodeBuilder.append(tmpDecodeString);
            if (parametersIterator.hasNext()) {
                decodeBuilder.append(", \n");
            }

            parameterBuilder.append('\'').append(parameter.getParameterPk()).append('\'');
            if (parametersIterator.hasNext()) {
                parameterBuilder.append(',');
            }
        }

        String exportBorisMesswerteStatement = exportBorisMesswerteStatementTpl.replace(
                "%MESSWERT_DECODE_STATEMENTS%",
                decodeBuilder);
        exportBorisMesswerteStatement = exportBorisMesswerteStatement.replace(
                "%MESSWERT_PARAMETER_PKS%",
                parameterBuilder);
        exportBorisMesswerteStatement = exportBorisMesswerteStatement.replace("%STANDORT_PKS%", standorteBuilder);
        if (log.isDebugEnabled()) {
            log.debug(exportBorisMesswerteStatement);
        }
        return exportBorisMesswerteStatement;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   resultSet  DOCUMENT ME!
     * @param   zip        DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    protected Object createCsv(final ResultSet resultSet, final boolean zip) throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final StringBuilder csvBuilder = new StringBuilder();

        final int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            final String columnName = metaData.getColumnName(i);
            csvBuilder.append('\"');
            csvBuilder.append(columnName.replace('\"', '\'').replace('\n', ' '));
            csvBuilder.append('\"');

            if (i < columnCount) {
                csvBuilder.append(", ");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("CSV Header: " + csvBuilder.toString());
        }
        csvBuilder.append(System.getProperty("line.separator"));

        int numResults = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                final String value = resultSet.getString(i);
                if ((value != null) && (value.length() > 0)) {
                    csvBuilder.append('\"');
                    csvBuilder.append(value.replace('\"', '\'').replace('\n', ' '));
                    csvBuilder.append('\"');
                }
                if (i < columnCount) {
                    csvBuilder.append(", ");
                }
            }
            csvBuilder.append(System.getProperty("line.separator"));
            numResults++;
        }

        LOG.info(numResults + " resources exported from Database");
        resultSet.close();

        if (zip) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("zipping output");
            }
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            final ZipOutputStream zipStream = new ZipOutputStream(output);
            zipStream.putNextEntry(new ZipEntry("boris-export.csv"));
            zipStream.write(csvBuilder.toString().getBytes("UTF-8"));
            zipStream.closeEntry();
            zipStream.close();

            return output.toByteArray();
        } else {
            return csvBuilder.toString();
        }
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        Statement exportBorisMesswerteStatement = null;
        ResultSet exportBorisMesswerteResult = null;
        try {
            Object result = null;

            Collection<String> standortPks = null;
            Collection<Parameter> parameters = null;

            for (final ServerActionParameter param : params) {
                if (param.getKey().equalsIgnoreCase(PARAM_STANDORTE)) {
                    standortPks = (Collection<String>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_PARAMETER)) {
                    parameters = (Collection<Parameter>)param.getValue();
                }
            }

            if ((standortPks != null) && (parameters != null)) {
                final String exportBorisMesswerte = this.createExportBorisMesswerteStatement(standortPks, parameters);

                exportBorisMesswerteStatement = this.sourceConnection.createStatement();
                exportBorisMesswerteResult = exportBorisMesswerteStatement.executeQuery(exportBorisMesswerte);

                result = this.createCsv(exportBorisMesswerteResult, false);
                exportBorisMesswerteStatement.close();
            } else {
                log.error("no PARAM_STANDORTE and PARAM_PARAMETER server action parameters provided,"
                            + "returning null");
            }

            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            if (exportBorisMesswerteResult != null) {
                try {
                    exportBorisMesswerteResult.close();
                } catch (Exception e) {
                    log.error("could not close exportBorisMesswerteResult", e);
                }
            }

            if (exportBorisMesswerteStatement != null) {
                try {
                    exportBorisMesswerteStatement.close();
                } catch (Exception e) {
                    log.error("could not close exportBorisMesswerteStatement", e);
                }
            }

            throw new RuntimeException(ex);
        }
    }

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        try {
            final Collection<String> standortPks = Arrays.asList(new String[] { "1000066", "1000067" });

            final Collection<Parameter> parameter = Arrays.asList(
                    new Parameter[] {
                        new Parameter("BCU1", "Kü ü ü pfer"),
                        new Parameter("BZN1", "Zinn"),
                        new Parameter("BPB1", "Blei"),
                    });

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<Collection<String>>(PARAM_STANDORTE, standortPks),
                    new ServerActionParameter<Collection<Parameter>>(PARAM_PARAMETER, parameter)
                };

            BasicConfigurator.configure();
            final BorisExportAction borisExportAction = new BorisExportAction();

            final Object result = borisExportAction.execute(null, serverActionParameters);
            final Path csvFile = Files.write(Paths.get("boris-export.csv"), result.toString().getBytes("UTF-8"));
            Logger.getLogger(BorisExportAction.class)
                    .info("CSV Export written to "
                        + csvFile.toAbsolutePath().toString());
        } catch (Throwable ex) {
            Logger.getLogger(BorisExportAction.class).fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
