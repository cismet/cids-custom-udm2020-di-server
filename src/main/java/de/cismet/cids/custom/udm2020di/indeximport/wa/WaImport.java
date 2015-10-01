/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.wa;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.ParameterMapping;
import de.cismet.cids.custom.udm2020di.types.ParameterMappings;
import de.cismet.cids.custom.udm2020di.types.boris.Standort;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class WaImport extends OracleImport {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String WAOW = "waow";
    protected static final String WAGW = "wagw";

    //~ Instance fields --------------------------------------------------------

    protected final String waSource;
    protected final String getStationsQry;
    protected final PreparedStatement getSampleValuesStmnt;
    protected final PreparedStatement insertStationStmnt;
    protected final PreparedStatement deleteStationStmnt;
    protected final OraclePreparedStatement insertSampleValuesStmnt;
    protected final PreparedStatement insertStationValuesRelStmnt;
    protected final PreparedStatement insertStationTagsRelStmnt;
    protected final PreparedStatement getTagsStmnt;
    protected final PreparedStatement updateStationJsonStmnt;
    protected final ParameterMappings parameterMappings = new ParameterMappings();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisImport object.
     *
     * @param   waSource  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public WaImport(final String waSource) throws Exception {
        this(WaImport.class.getResourceAsStream(waSource + ".properties"), waSource);
    }

    /**
     * Creates a new WA(OW|GW) Import object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     * @param   waSource        DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public WaImport(final InputStream propertiesFile, final String waSource) throws Exception {
        super(propertiesFile);
        this.waSource = waSource;
        this.log = Logger.getLogger(WaImport.class);

        getStationsQry = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/select-"
                            + waSource
                            + "-stations.prs.sql"),
                "UTF-8");

        final String getAggregatedSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/select-"
                            + waSource
                            + "-aggregated-sample-values.prs.sql"),
                "UTF-8");

        getSampleValuesStmnt = this.sourceConnection.prepareStatement(getAggregatedSampleValuesTpl);

        final String insertStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-site.prs.sql"),
                "UTF-8");
        insertStationStmnt = this.targetConnection.prepareStatement(insertStationTpl, new String[] { "ID" });

        final String deleteBorisStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/delete-"
                            + waSource
                            + "-site.prs.sql"),
                "UTF-8");
        deleteStationStmnt = this.targetConnection.prepareStatement(deleteBorisStationTpl);

        final String insertSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-aggregated-sample-values.prs.sql"),
                "UTF-8");
        insertSampleValuesStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertSampleValuesTpl,
                new String[] { "ID" });
//        insertSampleValuesStmnt.setExecuteBatch(200);
//        log.debug ("insertBorisSampleValues Statement Execute Batch Value " +
//                   insertSampleValuesStmnt.getExecuteBatch());

        final String insertStationSampleValuesRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-site-sample-values-relation.prs.sql"),
                "UTF-8");
        insertStationValuesRelStmnt = this.targetConnection.prepareStatement(
                insertStationSampleValuesRelTpl,
                new String[] { "ID" });

        final String insertStationTagsRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-site-tags-relation.prs.sql"),
                "UTF-8");
        insertStationTagsRelStmnt = this.targetConnection.prepareStatement(insertStationTagsRelTpl);

        final String getTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/"
                            + waSource
                            + "/get-"
                            + waSource
                            + "-tags.prs.sql"),
                "UTF-8");
        getTagsStmnt = this.targetConnection.prepareStatement(getTagsTpl);

        final String updateStationJsonTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/update-"
                            + waSource
                            + "-site-json.prs.sql"),
                "UTF-8");
        updateStationJsonStmnt = this.targetConnection.prepareStatement(updateStationJsonTpl);

        // load and cache mappings
        final String selectParameterMappingsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/select-"
                            + waSource
                            + "-parameter-mappings.sql"),
                "UTF-8");

        final Statement selectParameterMappings = this.targetConnection.createStatement();
        final ResultSet mappingsResultSet = selectParameterMappings.executeQuery(selectParameterMappingsTpl);

        final ParameterMapping[] parameterMappingArray = this.deserializeResultSet(
                mappingsResultSet,
                ParameterMapping[].class);
        for (final ParameterMapping parameterMapping : parameterMappingArray) {
            this.parameterMappings.put(parameterMapping.getParameterPk(),
                parameterMapping);
        }

        if (log.isDebugEnabled()) {
            log.debug(this.parameterMappings.size() + " parameter mappings cached");
        }
    }

    /**
     * Creates a new BorisImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     * @param   waSource        DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public WaImport(final Path propertiesFile, final String waSource) throws Exception {
        this(Files.newInputStream(propertiesFile, StandardOpenOption.READ), waSource);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    public void doBootstrap() throws IOException, SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Cleaning and Bootstrapping " + this.waSource + " Tables");
        }

        final String truncateWaTablesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/truncate-"
                            + waSource
                            + "-tables.sql"),
                "UTF-8");
        this.executeBatchStatement(targetConnection, truncateWaTablesTpl);

        final String insertWaTaggroupsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-taggroups.sql"),
                "UTF-8");

        final Statement insertWaTaggroups = this.targetConnection.createStatement();
        insertWaTaggroups.execute(insertWaTaggroupsTpl);

        this.targetConnection.commit();
        insertWaTaggroups.close();

        log.info("BORIS Tables successfully bootstrapped");
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
        final Statement getWAGWStatement = this.sourceConnection.createStatement();
        // getWAGWStatement.closeOnCompletion();
        long startTime = System.currentTimeMillis();
        log.info("fetching " + this.waSource + " stations from Source Connection " + this.sourceConnection.getSchema());
        final ResultSet stationsResultSet = getWAGWStatement.executeQuery(getStationsQry);
        if (log.isDebugEnabled()) {
            log.debug(this.waSource + " stations fetched in " + ((System.currentTimeMillis() - startTime) / 1000)
                        + "s");
        }
        int i = 0;

        while (stationsResultSet.next()) {
            try {
                startTime = System.currentTimeMillis();
                ++i;

                String tmpStr = stationsResultSet.getNString("STANDORT_PK");
                final String siteSrcPk = tmpStr;
                if (log.isDebugEnabled()) {
                    log.debug("processing " + this.waSource + " Station #" + (i) + ": " + siteSrcPk);
                }

                // key
                final String siteKey = this.waSource + '.' + tmpStr;

                tmpStr = stationsResultSet.getNString("STANDORTBEZEICHNUNG");
                String siteName = ((tmpStr != null) && !tmpStr.isEmpty())
                    ? tmpStr : stationsResultSet.getNString("STANDORTNUMMER");
                siteName = ((siteName != null) && !siteName.isEmpty()) ? siteName
                                                                       : stationsResultSet.getNString("STANDORT_PK");
                // description
                final String siteDescription = new StringBuilder().append("STANDORTBEZEICHNUNG: ")
                            .append(stationsResultSet.getNString("STANDORTBEZEICHNUNG"))
                            .append('\n')
                            .append("STANDORTNUMMER: ")
                            .append(stationsResultSet.getNString("STANDORTNUMMER"))
                            .append('\n')
                            .append("STANDORT PK in BORIS: ")
                            .append(stationsResultSet.getNString("STANDORT_PK"))
                            .append('\n')
                            .toString();

                // INSITUT TAG for Catalogue
                tmpStr = stationsResultSet.getNString("INSTITUT");
                final String siteInstitutTagKey = Integer.toHexString(tmpStr.hashCode());
                // -> INSERT INSTITUT TAG
                this.insertUniqueTag(siteInstitutTagKey, tmpStr, tmpStr, "BORIS.INSTITUT");

                // LITERATUR TAG for Catalogue
                tmpStr = stationsResultSet.getNString("LITERATUR");
                final String siteLiteraturTagKey = Integer.toHexString(tmpStr.hashCode());
                // -> INSERT LITERATUR TAG
                this.insertUniqueTag(siteLiteraturTagKey, tmpStr, tmpStr, "BORIS.LITERATUR");

                // GEOM
                final float siteHochwert = stationsResultSet.getFloat("HOCHWERT");     // X
                final float siteRechtswert = stationsResultSet.getFloat("RECHTSWERT"); // Y
                // -> INSERT GEOM and GET ID!
                final long siteGeomId = this.insertGeomPoint(siteRechtswert, siteHochwert, 31287, 4326);
                if (siteGeomId == -1) {
                    --i;
                    continue;
                }

                // SRC JSON CONTENT
                // final String siteSrcContent = this.xmlClobToJsonString(stationsResultSet.getClob("STANDORT_XML"));

                // -> INSERT SITE
                final long borisStationId = insertStation(
                        siteKey,
                        siteName,
                        siteDescription,
                        siteInstitutTagKey,
                        siteLiteraturTagKey,
                        siteGeomId,
                        siteSrcPk,
                        null);
                if (borisStationId == -1) {
                    --i;
                    continue;
                }

                // PARSE AND UPDATE JSON final ObjectNode jsonObject =
                // (ObjectNode)XML_MAPPER.readTree(stationsResultSet.getClob("STANDORT_XML") .getCharacterStream());

                final Standort borisStandort = XML_MAPPER.readValue(stationsResultSet.getClob("STANDORT_XML")
                                .getCharacterStream(),
                        Standort.class);

                // -> SAMPLE VALUES AND TAGS
                final List<AggregationValue> aggregationValues = new ArrayList<AggregationValue>();
                borisStandort.setAggregationValues(aggregationValues);
                final Collection<Long> sampeValueIds = getAndInsertSampleValues(siteSrcPk, aggregationValues);

                // site with at least on supported sample value?
                if (!sampeValueIds.isEmpty()) {
                    this.insertStationValuesRelation(borisStationId, sampeValueIds);
                    this.insertBorisStationTagsRelation(borisStationId);

                    final ObjectNode jsonObject = (ObjectNode)JSON_MAPPER.valueToTree(borisStandort);
                    this.updateSrcJson(borisStationId, jsonObject);
                } else {
                    log.warn("removing " + this.waSource + " Station #" + (--i) + " '" + siteSrcPk
                                + "': no supported sample values found!");
                    this.deleteStationStmnt.setLong(1, borisStationId);
                    this.deleteStationStmnt.executeUpdate();
                }

                // save the site
                this.targetConnection.commit();

                if (log.isDebugEnabled()) {
                    log.info(this.waSource + " Station #" + (i) + ": " + siteSrcPk
                                + " with " + sampeValueIds.size()
                                + " aggregated sample values processed and imported in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (Throwable t) {
                log.error("rolling back " + this.waSource + " Station #" + (i) + ": "
                            + " due to error: " + t.getMessage(),
                    t);
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
        this.getSampleValuesStmnt.close();

        this.insertGenericGeom.close();
        this.insertUniqueTag.close();

        this.insertStationStmnt.close();
        this.deleteStationStmnt.close();
        this.insertSampleValuesStmnt.close();
        this.insertStationValuesRelStmnt.close();
        this.insertStationTagsRelStmnt.close();
        this.updateStationJsonStmnt.close();
        this.getTagsStmnt.close();

        this.sourceConnection.close();
        this.targetConnection.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisStationId  DOCUMENT ME!
     * @param   jsonNode        DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     * @throws  IOException              DOCUMENT ME!
     */
    protected void updateSrcJson(final long borisStationId, final ObjectNode jsonNode) throws SQLException,
        JsonProcessingException,
        IOException {
        getTagsStmnt.setLong(1, borisStationId);
        final ResultSet getTagsResult = getTagsStmnt.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("tags", getTagsResult);

        try {
            // final String jsonString = this.JSON_MAPPER.writeValueAsString(jsonNode);
            // updateStationJsonStmnt.setClob(1, new StringReader(jsonString));
            // updateStationJsonStmnt.setString(1, jsonString);
            // updateStationJsonStmnt.setCharacterStream(1, new StringReader(jsonString), jsonString.length());

            final Clob srcContentClob = this.targetConnection.createClob();
            final Writer clobWriter = srcContentClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobWriter, jsonNode);
            updateStationJsonStmnt.setClob(1, srcContentClob);
            updateStationJsonStmnt.setLong(2, borisStationId);

            updateStationJsonStmnt.executeUpdate();
            clobWriter.close();
        } catch (Exception ex) {
            log.error("could not deserialize and update JSON of " + this.waSource + " Station "
                        + borisStationId + ": " + ex.getMessage(),
                ex);
            getTagsResult.close();
            throw ex;
        }

        getTagsResult.close();
        if (log.isDebugEnabled()) {
            log.debug("JSON Content of " + this.waSource + " Station " + borisStationId + " successfully updated");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisStationId  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertBorisStationTagsRelation(final long borisStationId) throws SQLException {
        this.insertStationTagsRelStmnt.setLong(1, borisStationId);
        this.insertStationTagsRelStmnt.setLong(2, borisStationId);
        this.insertStationTagsRelStmnt.setLong(3, borisStationId);
        this.insertStationTagsRelStmnt.setLong(4, borisStationId);

        this.insertStationTagsRelStmnt.executeUpdate();
        if (log.isDebugEnabled()) {
            log.debug("StationTagsRelation created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisStationId  DOCUMENT ME!
     * @param   sampeValueIds   DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertStationValuesRelation(final long borisStationId, final Collection<Long> sampeValueIds)
            throws SQLException {
        for (final long sampeValueId : sampeValueIds) {
            this.insertStationValuesRelStmnt.setLong(1, borisStationId);
            this.insertStationValuesRelStmnt.setLong(2, sampeValueId);
            this.insertStationValuesRelStmnt.addBatch();
        }

        this.insertStationValuesRelStmnt.executeBatch();
        if (log.isDebugEnabled()) {
            // this.insertStationValuesRelStmnt.close();
            log.debug(sampeValueIds.size() + " Station-Values-Relations created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   siteSrcPk          DOCUMENT ME!
     * @param   aggregationValues  jsonObject DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertSampleValues(final String siteSrcPk,
            final List<AggregationValue> aggregationValues) throws SQLException, IOException {
        final Collection<Long> sampeValueIds = new HashSet<Long>();
        int i = 0;
        int added = 0;

        // <- GET AGGREGATED SAMPLE VALUES
        this.getSampleValuesStmnt.setString(1, siteSrcPk);
        final ResultSet sampleValuesResultSet = this.getSampleValuesStmnt.executeQuery();
        // build the batch insert statements
        while (sampleValuesResultSet.next()) {
            final String PARAMETER_PK = sampleValuesResultSet.getString("PARAMETER_PK");
            i++;
            if (this.parameterMappings.containsKey(PARAMETER_PK)) {
                final AggregationValue aggregationValue = new AggregationValue();
                aggregationValues.add(aggregationValue);

                final ParameterMapping parameterMapping = this.parameterMappings.get(PARAMETER_PK);
                // NAME
                // log.debug(mappedParameters[0]);
                this.insertSampleValuesStmnt.setStringAtName("NAME", parameterMapping.getDisplayName());
                aggregationValue.setName(parameterMapping.getDisplayName());
                // this.insertSampleValuesStmnt.setString(1, mappedParameters[0]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + mappedParameters[1]);
// }
                // POLLUTANT
                this.insertSampleValuesStmnt.setStringAtName("POLLUTANT", parameterMapping.getPollutantTagKey());
                aggregationValue.setPollutantKey(parameterMapping.getPollutantTagKey());
                // this.insertSampleValuesStmnt.setString(2, mappedParameters[1]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + mappedParameters[2]);
// }
                // POLLUTANT_GROUP
                this.insertSampleValuesStmnt.setStringAtName(
                    "POLLUTANT_GROUP",
                    parameterMapping.getPollutantGroupKey());
                aggregationValue.setPollutantgroupKey(parameterMapping.getPollutantGroupKey());
                // this.insertSampleValuesStmnt.setString(3, mappedParameters[2]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getDate("MIN_DATE"));
// }
                final Date minDate = sampleValuesResultSet.getDate("MIN_DATE");
                this.insertSampleValuesStmnt.setDateAtName("MIN_DATE", minDate);
                aggregationValue.setMinDate(minDate);
                // this.insertSampleValuesStmnt.setDate(4, sampleValuesResultSet.getDate("MIN_DATE")); if
                // (log.isDebugEnabled()) { log.debug("["+added+"] " + sampleValuesResultSet.getDate("MAX_DATE")); }

                final Date maxDate = sampleValuesResultSet.getDate("MAX_DATE");
                this.insertSampleValuesStmnt.setDateAtName("MAX_DATE", maxDate);
                aggregationValue.setMaxDate(maxDate);

                // this.insertSampleValuesStmnt.setDate(5, sampleValuesResultSet.getDate("MAX_DATE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MIN_VALUE"));
// }
                final float minValue = sampleValuesResultSet.getFloat("MIN_VALUE");
                this.insertSampleValuesStmnt.setFloatAtName("MIN_VALUE", minValue);
                aggregationValue.setMinValue(minValue);
                // this.insertSampleValuesStmnt.setFloat(6, sampleValuesResultSet.getFloat("MIN_VALUE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MAX_VALUE"));
// }
                final float maxValue = sampleValuesResultSet.getFloat("MAX_VALUE");
                this.insertSampleValuesStmnt.setFloatAtName("MAX_VALUE", maxValue);
                aggregationValue.setMaxValue(maxValue);
                // this.insertSampleValuesStmnt.setFloat(7, sampleValuesResultSet.getFloat("MAX_VALUE"));

                // FIXME: define POJOs
                final String srcContentJson = this.xmlClobToJsonString(sampleValuesResultSet.getClob("MESSWERTE_XML"));
                // SRC_CONTENT
                // log.debug(srcContentJson);
                this.insertSampleValuesStmnt.setStringAtName("SRC_CONTENT", srcContentJson);
                // this.insertSampleValuesStmnt.setString(8, srcContentJson);

                // FIXME: Execute Batch does not work with large updates!!!!!
                // this.insertSampleValuesStmnt.addBatch();

                this.insertSampleValuesStmnt.executeUpdate();
                final ResultSet generatedKeys = this.insertSampleValuesStmnt.getGeneratedKeys();
                if ((null != generatedKeys)) {
                    while (generatedKeys.next()) {
                        sampeValueIds.add(generatedKeys.getLong(1));
                    }
                    generatedKeys.close();
                    added++;
                } else {
                    log.error("could not fetch generated key for inserted samples values for " + this.waSource
                                + " Station " + siteSrcPk);
                }
            }
        }

        sampleValuesResultSet.close();

        if (added > 0) {
// FIXME: Execute Batch does not work with large updates!!!!!
//            if (log.isDebugEnabled()) {
//                log.debug("adding " + added + " of " + i + " sample values for BORIS Station " + siteSrcPk);
//            }
//            this.insertSampleValuesStmnt.executeBatch();

//            final ResultSet generatedKeys = this.insertSampleValuesStmnt.getGeneratedKeys();
//            if ((null != generatedKeys)) {
//                while (generatedKeys.next()) {
//                    sampeValueIds.add(generatedKeys.getLong(1));
//                }
//                generatedKeys.close();
//                if (log.isDebugEnabled()) {
//                    log.debug(added + " of " + i + " sample values added for BORIS Station " + siteSrcPk
//                    + ", " + sampeValueIds.size() + " IDs generated");
//                }
//            } else {
//                log.error("could not fetch generated key for inserted samples values for BORIS SITE " + siteSrcPk);
//            }

            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " sample values added for " + this.waSource + " Station " + siteSrcPk
                            + ", " + sampeValueIds.size() + " IDs generated");
            }
        } else {
            log.warn("no supported sample values found in " + i + " available sample values for " + this.waSource
                        + " SITE "
                        + siteSrcPk);
        }

        return sampeValueIds;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   siteKey              DOCUMENT ME!
     * @param   siteName             DOCUMENT ME!
     * @param   siteDescription      DOCUMENT ME!
     * @param   siteLiteraturTagKey  DOCUMENT ME!
     * @param   siteInstitutTagKey   DOCUMENT ME!
     * @param   siteGeomId           DOCUMENT ME!
     * @param   siteSrcPk            DOCUMENT ME!
     * @param   siteSrcContent       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertStation(final String siteKey,
            final String siteName,
            final String siteDescription,
            final String siteLiteraturTagKey,
            final String siteInstitutTagKey,
            final long siteGeomId,
            final String siteSrcPk,
            final String siteSrcContent) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("inserting " + this.waSource + " Station " + siteKey + ": '" + siteName + "'");
        }
        final long startTime = System.currentTimeMillis();

        this.insertStationStmnt.setString(1, siteKey);
        this.insertStationStmnt.setString(2, siteName);
        this.insertStationStmnt.setString(3, siteDescription);
        this.insertStationStmnt.setLong(4, siteGeomId);
        this.insertStationStmnt.setString(5, siteLiteraturTagKey);
        this.insertStationStmnt.setString(6, siteInstitutTagKey);
        this.insertStationStmnt.setString(7, siteSrcPk);
        this.insertStationStmnt.setString(8, siteSrcContent);

        this.insertStationStmnt.executeUpdate();
        final ResultSet generatedStationKeysRs = this.insertStationStmnt.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedStationKeysRs) && generatedStationKeysRs.next()) {
            generatedKey = generatedStationKeysRs.getLong(1);
        } else {
            log.error("could not fetch generated key for inserted " + this.waSource + " Station!");
        }
        if (log.isDebugEnabled()) {
            // this.insertStationStmnt.close();
            log.debug(this.waSource + " station " + siteKey + " inserted in " + (System.currentTimeMillis() - startTime)
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
        final Logger logger = Logger.getLogger(WaImport.class);
        final String waSource = WAOW;
        WaImport borisImport = null;
        try {
            if (args.length > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("loading " + waSource + " properties from: " + args[0]);
                }
                borisImport = new WaImport(FileSystems.getDefault().getPath(args[0]),
                        waSource);
            } else {
                borisImport = new WaImport(waSource);
            }

            final long startTime = System.currentTimeMillis();
            logger.info("Starting " + waSource + " Import ......");

            borisImport.doBootstrap();
            final int stations = borisImport.doImport();

            logger.info(stations + " " + waSource + " Stations successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            logger.error("could not create " + waSource + " import instance: " + ex.getMessage(), ex);
        } finally {
            try {
                if (borisImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing source connection");
                    }
                    borisImport.sourceConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close source connection", ex);
                System.exit(1);
            }

            try {
                if (borisImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing target connection");
                    }
                    borisImport.targetConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close target connection", ex);
                System.exit(1);
            }
        }
    }
}
