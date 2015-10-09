/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.eprtr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import oracle.jdbc.OraclePreparedStatement;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.ParameterMapping;
import de.cismet.cids.custom.udm2020di.types.ParameterMappings;
import de.cismet.cids.custom.udm2020di.types.eprtr.Installation;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class EprtrImport extends OracleImport {

    //~ Instance fields --------------------------------------------------------

    protected final String getInstallationsStatementTpl;
    protected final PreparedStatement getReleasesStmnt;
    protected final OraclePreparedStatement insertInstallationStmnt;
    protected final PreparedStatement deleteInstallationStmnt;
    protected final OraclePreparedStatement insertReleaseStmnt;
    protected final OraclePreparedStatement insertInstallationTagsRelStmnt;
    protected final OraclePreparedStatement getTagsStnmt;
    protected final OraclePreparedStatement updateInstallationJsonStnmt;
    protected final ParameterMappings parameterMappings = new ParameterMappings();
    protected final Map<String, String> reflistMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new EprtrImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public EprtrImport() throws Exception {
        this(EprtrImport.class.getResourceAsStream("eprtr.properties"));
    }

    /**
     * Creates a new EprtrImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public EprtrImport(final InputStream propertiesFile) throws Exception {
        super(propertiesFile);
        this.log = Logger.getLogger(EprtrImport.class);

        getInstallationsStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-installations.prs.sql"),
                "UTF-8");

        final String getEprtrReleasesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-releases.prs.sql"),
                "UTF-8");

        getReleasesStmnt = this.sourceConnection.prepareStatement(getEprtrReleasesTpl);

        final String insertEprtrInstallationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/insert-eprtr-installation.prs.sql"),
                "UTF-8");
        insertInstallationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertEprtrInstallationTpl,
                new String[] { "ID" });

        final String deleteEprtrInstallationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/delete-eprtr-installation.prs.sql"),
                "UTF-8");
        deleteInstallationStmnt = this.targetConnection.prepareStatement(deleteEprtrInstallationTpl);

        final String insertEprtrSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/insert-eprtr-releases.prs.sql"),
                "UTF-8");
        insertReleaseStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertEprtrSampleValuesTpl,
                new String[] { "ID" });

        final String insertEprtrInstallationTagsRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/insert-eprtr-installation-tags-relation.prs.sql"),
                "UTF-8");
        insertInstallationTagsRelStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertEprtrInstallationTagsRelTpl);

        final String getTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/eprtr/get-eprtr-tags.prs.sql"),
                "UTF-8");
        getTagsStnmt = (OraclePreparedStatement)this.targetConnection.prepareStatement(getTagsTpl);

        final String updateInstallationJsonTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/update-eprtr-installation-json.prs.sql"),
                "UTF-8");
        updateInstallationJsonStnmt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                updateInstallationJsonTpl);

        // load and cache mappings
        final String selectEprtrParameterMappingsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-parameter-mappings.sql"),
                "UTF-8");

        final Statement selectEprtrParameterMappings = this.targetConnection.createStatement();
        final ResultSet mappingsResultSet = selectEprtrParameterMappings.executeQuery(selectEprtrParameterMappingsTpl);

        final ParameterMapping[] parameterMappingArray = this.deserializeResultSet(
                mappingsResultSet,
                ParameterMapping[].class);
        for (final ParameterMapping parameterMapping : parameterMappingArray) {
            this.parameterMappings.put(parameterMapping.getParameterPk(),
                parameterMapping);
        }

        mappingsResultSet.close();
        selectEprtrParameterMappings.close();

        if (log.isDebugEnabled()) {
            log.debug(this.parameterMappings.size() + " parameter mappings cached");
        }

        this.reflistMap = this.initEprtrReflist();
    }

    /**
     * Creates a new EprtrImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public EprtrImport(final Path propertiesFile) throws Exception {
        this(Files.newInputStream(propertiesFile, StandardOpenOption.READ));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   referenceKey  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String getReferenceValue(final String referenceKey) {
        final String referenceValue = this.reflistMap.get(referenceKey);
        return (referenceValue != null) ? referenceValue : referenceKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    protected final Map<String, String> initEprtrReflist() throws IOException, SQLException {
        final long startTime = System.currentTimeMillis();

        final HashMap<String, String> eprtrReflist = new HashMap<String, String>();

        final String selectEprtrReflistTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-reflist.sql"),
                "UTF-8");
        final Statement selectEprtrReflistStmnt = this.targetConnection.createStatement();
        final ResultSet eprtrReflistResultSet = selectEprtrReflistStmnt.executeQuery(selectEprtrReflistTpl);

        while (eprtrReflistResultSet.next()) {
            eprtrReflist.put(
                eprtrReflistResultSet.getString(1),
                eprtrReflistResultSet.getString(2));
        }

        selectEprtrReflistStmnt.close();
        eprtrReflistResultSet.close();

        if (log.isDebugEnabled()) {
            log.debug(eprtrReflist.size() + " reference value mappings cached in "
                        + ((System.currentTimeMillis() - startTime) / 1000) + "s");
        }

        return eprtrReflist;
    }

    /**
     * DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    public void doBootstrap() throws IOException, SQLException {
        final long startTime = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("Cleaning and Bootstrapping EPRTR Tables");
        }

        final String truncateEprtrTablesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/truncate-eprtr-tables.sql"),
                "UTF-8");
        this.executeBatchStatement(targetConnection, truncateEprtrTablesTpl);

        final String insertEprtrTaggroupsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/bootstrap/insert-eprtr-taggroups.sql"),
                "UTF-8");
        final Statement insertEprtrTaggroups = this.targetConnection.createStatement();
        insertEprtrTaggroups.execute(insertEprtrTaggroupsTpl);

        final String insertEprtrTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/bootstrap/insert-eprtr-tags.sql"),
                "UTF-8");
        final Statement insertEprtrTagsStmnt = this.targetConnection.createStatement();
        insertEprtrTagsStmnt.execute(insertEprtrTagsTpl);

        this.targetConnection.commit();
        insertEprtrTaggroups.close();

        log.info("EPRTR Tables successfully bootstrapped in "
                    + ((System.currentTimeMillis() - startTime) / 1000) + "s");
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    public int doImport() throws SQLException, IOException {
        final Statement getInstallationsStatement = this.sourceConnection.createStatement();
        // getInstallationsStatement.closeOnCompletion();
        long startTime = System.currentTimeMillis();
        log.info("fetching EPRTR installations from Source Connection " + this.sourceConnection.getSchema());
        final ResultSet installationsResultSet = getInstallationsStatement.executeQuery(getInstallationsStatementTpl);
        if (log.isDebugEnabled()) {
            log.debug("EPRTR installations fetched in " + ((System.currentTimeMillis() - startTime) / 1000) + "s");
        }
        int i = 0;

        while (installationsResultSet.next()) {
            try {
                startTime = System.currentTimeMillis();
                ++i;

                String tmpStr = installationsResultSet.getNString("STANDORT_PK");
                final String installationSrcPk = tmpStr;
                if (log.isDebugEnabled()) {
                    log.debug("processing EPRTR Installation #" + (i) + ": " + installationSrcPk);
                }

                // key
                final String installationKey = "EPRTR." + tmpStr;

                tmpStr = installationsResultSet.getNString("STANDORTBEZEICHNUNG");
                String installationName = ((tmpStr != null) && !tmpStr.isEmpty())
                    ? tmpStr : installationsResultSet.getNString("STANDORTNUMMER");
                installationName = ((installationName != null) && !installationName.isEmpty())
                    ? installationName : installationsResultSet.getNString("STANDORT_PK");
                // description
                final String installationDescription = new StringBuilder().append("STANDORTBEZEICHNUNG: ")
                            .append(installationsResultSet.getNString("STANDORTBEZEICHNUNG"))
                            .append('\n')
                            .append("STANDORTNUMMER: ")
                            .append(installationsResultSet.getNString("STANDORTNUMMER"))
                            .append('\n')
                            .append("STANDORT PK in EPRTR: ")
                            .append(installationsResultSet.getNString("STANDORT_PK"))
                            .append('\n')
                            .toString();

                // INSITUT TAG for Catalogue
                tmpStr = installationsResultSet.getNString("INSTITUT");
                final String installationInstitutTagKey = Integer.toHexString(tmpStr.hashCode());
                // -> INSERT INSTITUT TAG
                this.insertUniqueTag(installationInstitutTagKey, tmpStr, tmpStr, "EPRTR.INSTITUT");

                // LITERATUR TAG for Catalogue
                tmpStr = installationsResultSet.getNString("LITERATUR");
                final String installationLiteraturTagKey = Integer.toHexString(tmpStr.hashCode());
                // -> INSERT LITERATUR TAG
                this.insertUniqueTag(installationLiteraturTagKey, tmpStr, tmpStr, "EPRTR.LITERATUR");

                // GEOM
                final float installationHochwert = installationsResultSet.getFloat("HOCHWERT");     // X
                final float installationRechtswert = installationsResultSet.getFloat("RECHTSWERT"); // Y
                // -> INSERT GEOM and GET ID!
                final long installationGeomId = this.insertGeomPoint(
                        installationRechtswert,
                        installationHochwert,
                        31287,
                        4326);
                if (installationGeomId == -1) {
                    --i;
                    continue;
                }

                // SRC JSON CONTENT final String installationSrcContent =
                // this.xmlClobToJsonString(installationsResultSet.getClob("STANDORT_XML"));

                // -> INSERT SITE
                final long eprtrInstallationId = insertInstallation(
                        installationKey,
                        installationName,
                        installationDescription,
                        installationInstitutTagKey,
                        installationLiteraturTagKey,
                        installationGeomId,
                        installationSrcPk,
                        null);
                if (eprtrInstallationId == -1) {
                    --i;
                    continue;
                }

                // PARSE AND UPDATE JSON final ObjectNode jsonObject =
                // (ObjectNode)XML_MAPPER.readTree(installationsResultSet.getClob("STANDORT_XML")
                // .getCharacterStream());

                final Installation eprtrInstallation = XML_MAPPER.readValue(installationsResultSet.getClob(
                            "STANDORT_XML").getCharacterStream(),
                        Installation.class);

                // -> SAMPLE VALUES AND TAGS
                // AggregationValues -> collection impl. that stores only maximum/minimum values!
                final Collection<AggregationValue> aggregationValues = new AggregationValues();
                final Collection<Long> releaseIds = getAndInsertSampleValues(
                        eprtrInstallationId,
                        installationSrcPk,
                        aggregationValues);

                // set unique aggregation values FIXME:       eprtrInstallation.setAggregationValues(new
                // ArrayList<AggregationValue>(aggregationValues)); installation with at least on supported sample
                // value?
                if (!releaseIds.isEmpty()) {
                    this.insertEprtrInstallationTagsRelation(eprtrInstallationId);

                    final ObjectNode jsonObject = (ObjectNode)JSON_MAPPER.valueToTree(eprtrInstallation);
                    this.updateSrcJson(eprtrInstallationId, jsonObject);
                } else {
                    log.warn("removing EPRTR Installation #" + (--i) + " '" + installationSrcPk
                                + "': no supported sample values found!");
                    this.deleteInstallationStmnt.setLong(1, eprtrInstallationId);
                    this.deleteInstallationStmnt.executeUpdate();

                    if (installationGeomId != -1) {
                        this.deleteGeomStmnt.setLong(1, installationGeomId);
                        this.deleteGeomStmnt.executeUpdate();
                    }
                }

                // save the installation
                this.targetConnection.commit();

                if (log.isDebugEnabled()) {
                    log.info("EPRTR Installation #" + (i) + ": " + installationSrcPk
                                + " with " + releaseIds.size()
                                + " aggregated sample values processed and imported in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (Throwable t) {
                log.error("rolling back EPRTR Installation #" + (i) + ": "
                            + " due to error: " + t.getMessage(), t);
                try {
                    this.targetConnection.rollback();
                } catch (SQLException sx) {
                    log.error("could not rollback target connection", sx);
                }

                --i;
            }

            // test mode:
            // break;
        }
        if (log.isDebugEnabled()) {
            // clean up
            log.debug("closing connections ....");
        }
        this.getReleasesStmnt.close();
        this.insertGenericGeomStmnt.close();
        this.insertUniqueTagStmnt.close();
        this.deleteGeomStmnt.close();
        this.insertInstallationStmnt.close();
        this.deleteInstallationStmnt.close();
        this.insertReleaseStmnt.close();
        this.insertInstallationTagsRelStmnt.close();
        this.updateInstallationJsonStnmt.close();
        this.getTagsStnmt.close();

        this.sourceConnection.close();
        this.targetConnection.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   eprtrInstallationId  DOCUMENT ME!
     * @param   jsonNode             DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     * @throws  IOException              DOCUMENT ME!
     */
    protected void updateSrcJson(final long eprtrInstallationId, final ObjectNode jsonNode) throws SQLException,
        JsonProcessingException,
        IOException {
        getTagsStnmt.setLong(1, eprtrInstallationId);
        final ResultSet getTagsResult = getTagsStnmt.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("tags", getTagsResult);

        try {
            // final String jsonString = this.JSON_MAPPER.writeValueAsString(jsonNode);
            // updateInstallationJson.setClob(1, new StringReader(jsonString));
            // updateInstallationJson.setString(1, jsonString);
            // updateInstallationJson.setCharacterStream(1, new StringReader(jsonString), jsonString.length());

            final Clob srcContentClob = this.targetConnection.createClob();
            final Writer clobWriter = srcContentClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobWriter, jsonNode);
            updateInstallationJsonStnmt.setClob(1, srcContentClob);
            updateInstallationJsonStnmt.setLong(2, eprtrInstallationId);

            updateInstallationJsonStnmt.executeUpdate();
            clobWriter.close();
        } catch (Exception ex) {
            log.error("could not deserialize and update JSON of Eprtr Installation "
                        + eprtrInstallationId + ": " + ex.getMessage(),
                ex);
            getTagsResult.close();
            throw ex;
        }

        getTagsResult.close();
        if (log.isDebugEnabled()) {
            log.debug("JSON Content of EPRTR Installation " + eprtrInstallationId + " successfully updated");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   eprtrInstallationId  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertEprtrInstallationTagsRelation(final long eprtrInstallationId) throws SQLException {
        this.insertInstallationTagsRelStmnt.setLong(1, eprtrInstallationId);
        this.insertInstallationTagsRelStmnt.setLong(2, eprtrInstallationId);
        this.insertInstallationTagsRelStmnt.setLong(3, eprtrInstallationId);
        this.insertInstallationTagsRelStmnt.setLong(4, eprtrInstallationId);

        this.insertInstallationTagsRelStmnt.executeUpdate();
        if (log.isDebugEnabled()) {
            log.debug("InstallationTagsRelation created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   eprtrInstallationId  DOCUMENT ME!
     * @param   installationSrcPk    DOCUMENT ME!
     * @param   aggregationValues    jsonObject DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertSampleValues(final long eprtrInstallationId,
            final String installationSrcPk,
            final Collection<AggregationValue> aggregationValues) throws SQLException, IOException {
        final Collection<Long> sampeValueIds = new HashSet<Long>();
        int i = 0;
        int added = 0;

        // <- GET AGGREGATED SAMPLE VALUES
        this.getReleasesStmnt.setString(1, installationSrcPk);
        final ResultSet sampleValuesResultSet = this.getReleasesStmnt.executeQuery();
        // build the batch insert statements
        while (sampleValuesResultSet.next()) {
            final String PARAMETER_PK = sampleValuesResultSet.getString("PARAMETER_PK");
            i++;
            if (this.parameterMappings.containsKey(PARAMETER_PK)) {
                final AggregationValue aggregationValue = new AggregationValue();

                // aggregation parameter mapping!
                final ParameterMapping parameterMapping = this.parameterMappings.getAggregationMapping(PARAMETER_PK);

                // NAME
                // log.debug(mappedParameters[0]);
                this.insertReleaseStmnt.setStringAtName("NAME", parameterMapping.getDisplayName());
                aggregationValue.setName(parameterMapping.getDisplayName());
                // this.insertSampleValues.setString(1, mappedParameters[0]); if (log.isDebugEnabled()) {
                // log.debug("["+added+"] " + mappedParameters[1]); }

                // SITE
                this.insertReleaseStmnt.setLongAtName("SITE", eprtrInstallationId);

                // POLLUTANT
                this.insertReleaseStmnt.setStringAtName("POLLUTANT", parameterMapping.getPollutantTagKey());
                aggregationValue.setPollutantKey(parameterMapping.getPollutantTagKey());
                // this.insertSampleValues.setString(2, mappedParameters[1]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + mappedParameters[2]);
// }
                // POLLUTANT_GROUP
                this.insertReleaseStmnt.setStringAtName("POLLUTANT_GROUP", parameterMapping.getPollutantGroupKey());
                aggregationValue.setPollutantgroupKey(parameterMapping.getPollutantGroupKey());
                // this.insertSampleValues.setString(3, mappedParameters[2]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getDate("MIN_DATE"));
// }
                final Date minDate = sampleValuesResultSet.getDate("MIN_DATE");
                this.insertReleaseStmnt.setDateAtName("MIN_DATE", minDate);
                aggregationValue.setMinDate(minDate);
                // this.insertSampleValues.setDate(4, sampleValuesResultSet.getDate("MIN_DATE")); if
                // (log.isDebugEnabled()) { log.debug("["+added+"] " + sampleValuesResultSet.getDate("MAX_DATE")); }

                final Date maxDate = sampleValuesResultSet.getDate("MAX_DATE");
                this.insertReleaseStmnt.setDateAtName("MAX_DATE", maxDate);
                aggregationValue.setMaxDate(maxDate);

                // this.insertSampleValues.setDate(5, sampleValuesResultSet.getDate("MAX_DATE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MIN_VALUE"));
// }
                final float minValue = sampleValuesResultSet.getFloat("MIN_VALUE");
                this.insertReleaseStmnt.setFloatAtName("MIN_VALUE", minValue);
                aggregationValue.setMinValue(minValue);
                // this.insertSampleValues.setFloat(6, sampleValuesResultSet.getFloat("MIN_VALUE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MAX_VALUE"));
// }
                final float maxValue = sampleValuesResultSet.getFloat("MAX_VALUE");
                this.insertReleaseStmnt.setFloatAtName("MAX_VALUE", maxValue);
                aggregationValue.setMaxValue(maxValue);
                // this.insertSampleValues.setFloat(7, sampleValuesResultSet.getFloat("MAX_VALUE"));

                // fill the list and eliminate duplicates
                aggregationValues.add(aggregationValue);

                // FIXME: define POJOs
                final String srcContentJson = this.xmlClobToJsonString(sampleValuesResultSet.getClob("MESSWERTE_XML"));
                // SRC_CONTENT
                // log.debug(srcContentJson);
                this.insertReleaseStmnt.setStringAtName("SRC_CONTENT", srcContentJson);
                // this.insertSampleValues.setString(8, srcContentJson);

                // FIXME: Execute Batch does not work with large updates!!!!!
                // this.insertSampleValues.addBatch();

                this.insertReleaseStmnt.executeUpdate();
                final ResultSet generatedKeys = this.insertReleaseStmnt.getGeneratedKeys();
                if ((null != generatedKeys)) {
                    while (generatedKeys.next()) {
                        sampeValueIds.add(generatedKeys.getLong(1));
                    }
                    generatedKeys.close();
                    added++;
                } else {
                    log.error("could not fetch generated key for inserted samples values for EPRTR SITE "
                                + installationSrcPk);
                }
            }
        }

        sampleValuesResultSet.close();

        if (added > 0) {
// FIXME: Execute Batch does not work with large updates!!!!!
//            if (log.isDebugEnabled()) {
//                log.debug("adding " + added + " of " + i + " sample values for EPRTR Installation " + installationSrcPk);
//            }
//            this.insertSampleValues.executeBatch();

//            final ResultSet generatedKeys = this.insertSampleValues.getGeneratedKeys();
//            if ((null != generatedKeys)) {
//                while (generatedKeys.next()) {
//                    sampeValueIds.add(generatedKeys.getLong(1));
//                }
//                generatedKeys.close();
//                if (log.isDebugEnabled()) {
//                    log.debug(added + " of " + i + " sample values added for EPRTR Installation " + installationSrcPk
//                    + ", " + sampeValueIds.size() + " IDs generated");
//                }
//            } else {
//                log.error("could not fetch generated key for inserted samples values for EPRTR SITE " + installationSrcPk);
//            }

            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " sample values added for EPRTR Installation " + installationSrcPk
                            + ", " + sampeValueIds.size() + " IDs generated");
            }
        } else {
            log.warn("no supported sample values found in " + i + " available sample values for EPRTR SITE "
                        + installationSrcPk);
        }

        return sampeValueIds;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   installationKey              DOCUMENT ME!
     * @param   installationName             DOCUMENT ME!
     * @param   installationDescription      DOCUMENT ME!
     * @param   installationLiteraturTagKey  DOCUMENT ME!
     * @param   installationInstitutTagKey   DOCUMENT ME!
     * @param   installationGeomId           DOCUMENT ME!
     * @param   installationSrcPk            DOCUMENT ME!
     * @param   installationSrcContent       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertInstallation(final String installationKey,
            final String installationName,
            final String installationDescription,
            final String installationLiteraturTagKey,
            final String installationInstitutTagKey,
            final long installationGeomId,
            final String installationSrcPk,
            final String installationSrcContent) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("inserting EPRTR Installation " + installationKey + ": '" + installationName + "'");
        }
        final long startTime = System.currentTimeMillis();

        this.insertInstallationStmnt.setString(1, installationKey);
        this.insertInstallationStmnt.setString(2, installationName);
        this.insertInstallationStmnt.setString(3, installationDescription);
        this.insertInstallationStmnt.setLong(4, installationGeomId);
        this.insertInstallationStmnt.setString(5, installationLiteraturTagKey);
        this.insertInstallationStmnt.setString(6, installationInstitutTagKey);
        this.insertInstallationStmnt.setString(7, installationSrcPk);
        this.insertInstallationStmnt.setString(8, installationSrcContent);

        this.insertInstallationStmnt.executeUpdate();
        final ResultSet generatedInstallationKeysRs = this.insertInstallationStmnt.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedInstallationKeysRs) && generatedInstallationKeysRs.next()) {
            generatedKey = generatedInstallationKeysRs.getLong(1);
        } else {
            log.error("could not fetch generated key for inserted EPRTR SITE!");
        }
        if (log.isDebugEnabled()) {
            // this.insertInstallation.close();
            log.debug("EPRTR installation " + installationKey + " inserted in "
                        + (System.currentTimeMillis() - startTime)
                        + "ms, new ID is "
                        + generatedKey);
        }
        return generatedKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        final long startTime = System.currentTimeMillis();
        final Logger logger = Logger.getLogger(EprtrImport.class);

        EprtrImport eprtrImport = null;
        try {
            if (args.length > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("loading EPRTR properties from: " + args[0]);
                }
                eprtrImport = new EprtrImport(FileSystems.getDefault().getPath(args[0]));
            } else {
                eprtrImport = new EprtrImport();
            }

            eprtrImport.doBootstrap();
            logger.info("EPRTR Indeximport successfully initialized and bootstrapped in "
                        + ((System.currentTimeMillis() - startTime) / 1000) + "s");

            /*
             * logger.info("Starting EPRTR Import ......"); final int installations = eprtrImport.doImport();
             *
             * logger.info(installations + " EPRTR Installations successfully imported in "         +
             * ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
             *
             */
        } catch (Exception ex) {
            logger.error("could not create EPRTR import instance: " + ex.getMessage(), ex);
        } finally {
            try {
                if (eprtrImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing source connection");
                    }
                    eprtrImport.sourceConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close source connection", ex);
                System.exit(1);
            }

            try {
                if (eprtrImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing target connection");
                    }
                    eprtrImport.targetConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close target connection", ex);
                System.exit(1);
            }
        }
    }
}
