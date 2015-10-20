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
import java.util.HashSet;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.ParameterMapping;
import de.cismet.cids.custom.udm2020di.types.ParameterMappings;
import de.cismet.cids.custom.udm2020di.types.wa.GwMessstelle;
import de.cismet.cids.custom.udm2020di.types.wa.Messstelle;
import de.cismet.cids.custom.udm2020di.types.wa.OwMessstelle;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class WaImport extends OracleImport {

    //~ Static fields/initializers ---------------------------------------------

    public static final String WAOW = "waow";
    public static final String WAGW = "wagw";

    //~ Instance fields --------------------------------------------------------

    protected final String waSource;
    protected final String getStationsQry;
    protected final PreparedStatement getSampleValuesStmnt;
    protected final OraclePreparedStatement insertStationStmnt;
    protected final PreparedStatement deleteStationStmnt;
    protected final OraclePreparedStatement insertSampleValuesStmnt;
    protected final OraclePreparedStatement insertStationTagsRelStmnt;
    protected final PreparedStatement getTagsStmnt;
    protected final OraclePreparedStatement updateStationJsonStmnt;
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
                            + "-sample-values.prs.sql"),
                "UTF-8");

        getSampleValuesStmnt = this.sourceConnection.prepareStatement(getAggregatedSampleValuesTpl);

        final String insertStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-station.prs.sql"),
                "UTF-8");
        insertStationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertStationTpl,
                new String[] { "ID" });

        final String deleteBorisStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/delete-"
                            + waSource
                            + "-station.prs.sql"),
                "UTF-8");
        deleteStationStmnt = this.targetConnection.prepareStatement(deleteBorisStationTpl);

        final String insertSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-sample-values.prs.sql"),
                "UTF-8");
        insertSampleValuesStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertSampleValuesTpl,
                new String[] { "ID" });

        final String insertStationTagsRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/"
                            + waSource
                            + "/insert-"
                            + waSource
                            + "-station-tags-relation.prs.sql"),
                "UTF-8");
        insertStationTagsRelStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertStationTagsRelTpl);

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
                            + "-station-json.prs.sql"),
                "UTF-8");
        updateStationJsonStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                updateStationJsonTpl);

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
            log.debug("Cleaning and Bootstrapping " + this.waSource.toUpperCase() + " Tables");
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
                            + "/bootstrap/insert-"
                            + waSource
                            + "-taggroups.sql"),
                "UTF-8");

        final Statement insertWaTaggroups = this.targetConnection.createStatement();
        insertWaTaggroups.execute(insertWaTaggroupsTpl);

        this.targetConnection.commit();
        insertWaTaggroups.close();

        log.info(waSource + " Tables successfully bootstrapped");
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
        final Statement getStationsStmnt = this.sourceConnection.createStatement();
        long startTime = System.currentTimeMillis();
        log.info("fetching " + this.waSource.toUpperCase() + " stations from Source Connection "
                    + this.sourceConnection.getSchema());
        final ResultSet stationsResultSet = getStationsStmnt.executeQuery(getStationsQry);
        if (log.isDebugEnabled()) {
            log.debug(this.waSource + " stations fetched in " + ((System.currentTimeMillis() - startTime) / 1000)
                        + "s");
        }
        int i = 0;

        while (stationsResultSet.next() && (i < 25)) {
            try {
                startTime = System.currentTimeMillis();
                ++i;

                String tmpStr = stationsResultSet.getNString("MESSSTELLE_PK");
                final String stationSrcPk = tmpStr;
                if (log.isDebugEnabled()) {
                    log.debug("processing " + this.waSource.toUpperCase() + " Station #" + (i) + ": " + stationSrcPk);
                }

                // key
                final String stationKey = this.waSource.toUpperCase() + '.' + tmpStr;

                tmpStr = stationsResultSet.getNString("MESSSTELLE_NAME");
                final String stationName = ((tmpStr != null) && !tmpStr.isEmpty()) ? tmpStr : stationSrcPk;

                // description
                final String stationDescription = new StringBuilder().append("MESSSTELLEN NAME: ")
                            .append(stationName)
                            .append('\n')
                            .append("MESSSTELLEN TYP: ")
                            .append(stationsResultSet.getNString("MESSSTELLE_TYP"))
                            .append('\n')
                            .toString();

                // ZUSTAENDIGE_STELLE TAG for Catalogue
                tmpStr = stationsResultSet.getNString("ZUSTAENDIGE_STELLE");
                String stationResponsiblePartyTagKey = null;

                if ((tmpStr != null) && !tmpStr.isEmpty()) {
                    stationResponsiblePartyTagKey = Integer.toHexString(tmpStr.hashCode());
                    // -> INSERT ZUSTAENDIGE_STELLE TAG
                    this.insertUniqueTag(
                        stationResponsiblePartyTagKey,
                        tmpStr,
                        tmpStr,
                        this.waSource.toUpperCase()
                                + ".ZUSTAENDIGE_STELLE");
                }

                String stationWaterTagKey = null;
                if (this.waSource.equalsIgnoreCase(WAGW)) {
                    // GWK TAG for Catalogue
                    tmpStr = stationsResultSet.getNString("GWK_NAME");
                    if ((tmpStr != null) && !tmpStr.isEmpty()) {
                        stationWaterTagKey = Integer.toHexString(tmpStr.hashCode());
                        // -> INSERT GWK TAG
                        this.insertUniqueTag(stationWaterTagKey, tmpStr, tmpStr,
                            this.waSource.toUpperCase()
                                    + ".GWK");
                    }
                } else {
                    // GWK GEWAESSER for Catalogue
                    tmpStr = stationsResultSet.getNString("GEWAESSER_NAME");
                    if ((tmpStr != null) && !tmpStr.isEmpty()) {
                        stationWaterTagKey = Integer.toHexString(tmpStr.hashCode());
                        // -> INSERT GEWAESSER TAG
                        this.insertUniqueTag(
                            stationWaterTagKey,
                            tmpStr,
                            tmpStr,
                            this.waSource.toUpperCase()
                                    + ".GEWAESSER");
                    }
                }

                // GEOM
                final float stationRechtswert = stationsResultSet.getFloat("XKOORDINATE"); // X
                final float stationHochwert = stationsResultSet.getFloat("YKOORDINATE");   // Y
                // -> INSERT GEOM and GET ID!
                final long stationGeomId = this.insertGeomPoint(stationRechtswert, stationHochwert, 31287, 4326);
                if (stationGeomId == -1) {
                    --i;
                    continue;
                }

                // -> INSERT STATION
                final long waStationId = insertStation(
                        stationKey,
                        stationName,
                        stationDescription,
                        stationResponsiblePartyTagKey,
                        stationWaterTagKey,
                        stationGeomId,
                        stationSrcPk,
                        null);
                if (waStationId == -1) {
                    --i;
                    continue;
                }

                // PARSE AND UPDATE JSON final ObjectNode jsonObject =
                final Messstelle waMessstelle;
                if (this.waSource.equalsIgnoreCase(WAGW)) {
                    waMessstelle = XML_MAPPER.readValue(stationsResultSet.getClob("MESSSTELLE_XML")
                                    .getCharacterStream(),
                            GwMessstelle.class);
                } else {
                    waMessstelle = XML_MAPPER.readValue(stationsResultSet.getClob("MESSSTELLE_XML")
                                    .getCharacterStream(),
                            OwMessstelle.class);
                }

                // -> SAMPLE VALUES AND TAGS
                // AggregationValues -> collection impl. that stores only maximum/minimum values!
                final Collection<AggregationValue> aggregationValues = new AggregationValues();
                final Collection<Long> sampeValueIds = getAndInsertSampleValues(
                        waStationId,
                        stationSrcPk,
                        aggregationValues);

                // set unique aggregation values
                waMessstelle.setAggregationValues(new ArrayList<AggregationValue>(aggregationValues));

                // station with at least on supported sample value?
                if (!sampeValueIds.isEmpty()) {
                    this.insertWaStationTagsRelation(waStationId);

                    final ObjectNode jsonObject = (ObjectNode)JSON_MAPPER.valueToTree(waMessstelle);
                    this.updateSrcJson(waStationId, jsonObject);
                } else {
                    log.warn("removing " + this.waSource.toUpperCase() + " Station #" + (--i) + " '" + stationSrcPk
                                + "': no supported sample values found!");
                    this.deleteStationStmnt.setLong(1, waStationId);
                    this.deleteStationStmnt.executeUpdate();

                    if (stationGeomId != -1) {
                        this.deleteGeomStmnt.setLong(1, stationGeomId);
                        this.deleteGeomStmnt.executeUpdate();
                    }
                }

                // save the station
                this.targetConnection.commit();

                if (log.isDebugEnabled()) {
                    log.info(this.waSource + " Station #" + (i) + ": " + stationSrcPk
                                + " with " + sampeValueIds.size()
                                + " sample values and " + aggregationValues.size()
                                + " aggregated sample values processed and imported in "
                                + ((System.currentTimeMillis() - startTime) / 1000) + "s");
                }
            } catch (Throwable t) {
                log.error("rolling back " + this.waSource.toUpperCase() + " Station #" + (i) + ": "
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
        this.insertGenericGeomStmnt.close();
        this.insertUniqueTagStmnt.close();
        this.deleteGeomStmnt.close();
        this.insertStationStmnt.close();
        this.deleteStationStmnt.close();
        this.insertSampleValuesStmnt.close();
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
     * @param   waStationId  DOCUMENT ME!
     * @param   jsonNode     DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     * @throws  IOException              DOCUMENT ME!
     */
    protected void updateSrcJson(final long waStationId, final ObjectNode jsonNode) throws SQLException,
        JsonProcessingException,
        IOException {
        getTagsStmnt.setLong(1, waStationId);
        final ResultSet getTagsResult = getTagsStmnt.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("tags", getTagsResult);

        try {
            final Clob srcContentClob = this.targetConnection.createClob();
            final Writer clobWriter = srcContentClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobWriter, jsonNode);
            updateStationJsonStmnt.setClob(1, srcContentClob);
            updateStationJsonStmnt.setLong(2, waStationId);
            updateStationJsonStmnt.setLong(3, waStationId);
            updateStationJsonStmnt.setLong(4, waStationId);

            updateStationJsonStmnt.executeUpdate();
            clobWriter.close();
        } catch (Exception ex) {
            log.error("could not deserialize and update JSON of " + this.waSource.toUpperCase() + " Station "
                        + waStationId + ": " + ex.getMessage(),
                ex);
            getTagsResult.close();
            throw ex;
        }

        getTagsResult.close();
        if (log.isDebugEnabled()) {
            log.debug("JSON Content of " + this.waSource.toUpperCase() + " Station " + waStationId
                        + " successfully updated");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   waStationId  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertWaStationTagsRelation(final long waStationId) throws SQLException {
        this.insertStationTagsRelStmnt.setLongAtName("STATION_ID", waStationId);
        // this.insertStationTagsRelStmnt.setLong(2, waStationId); this.insertStationTagsRelStmnt.setLong(3,
        // waStationId); this.insertStationTagsRelStmnt.setLong(4, waStationId);

        this.insertStationTagsRelStmnt.executeUpdate();
        if (log.isDebugEnabled()) {
            log.debug("StationTagsRelation created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   waStationId        DOCUMENT ME!
     * @param   stationSrcPk       DOCUMENT ME!
     * @param   aggregationValues  jsonObject DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException                DOCUMENT ME!
     * @throws  IOException                 DOCUMENT ME!
     * @throws  CloneNotSupportedException  DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertSampleValues(final long waStationId,
            final String stationSrcPk,
            final Collection<AggregationValue> aggregationValues) throws SQLException,
        IOException,
        CloneNotSupportedException {
        final long currentTime = System.currentTimeMillis();
        final Collection<Long> sampeValueIds = new HashSet<Long>();
        int i = 0;
        int added = 0;
        if (log.isDebugEnabled()) {
            // <- GET SAMPLE VALUES
            log.debug("searching and inserting sample values for "
                        + this.waSource.toUpperCase() + " station " + stationSrcPk);
        }
        this.getSampleValuesStmnt.setString(1, stationSrcPk);
        final ResultSet sampleValuesResultSet = this.getSampleValuesStmnt.executeQuery();
        if (log.isDebugEnabled()) {
            log.debug("sample values for " + this.waSource.toUpperCase() + " station "
                        + stationSrcPk + " fetched in "
                        + ((System.currentTimeMillis() - currentTime) / 1000) + "s.");
        }

        // insert statements
        while (sampleValuesResultSet.next()) {
            final String PARAMETER_PK = sampleValuesResultSet.getString("PARAMETER_PK");
            i++;
            if (this.parameterMappings.containsKey(PARAMETER_PK)) {
                final AggregationValue aggregationValue = new AggregationValue();

                // aggregation parameter mapping!
                final ParameterMapping parameterMapping = this.parameterMappings.getAggregationMapping(PARAMETER_PK);

                // NAME
                // log.debug(mappedParameters[0]);
                this.insertSampleValuesStmnt.setStringAtName("NAME", parameterMapping.getDisplayName());
                aggregationValue.setName(parameterMapping.getDisplayName());

                // STATION
                this.insertSampleValuesStmnt.setLongAtName("STATION", waStationId);

                // POLLUTANT
                this.insertSampleValuesStmnt.setLongAtName(
                    "POLLUTANT_ID",
                    parameterMapping.getPollutantTagId());
                aggregationValue.setPollutantKey(parameterMapping.getPollutantTagKey());
// }
                // POLLUTANT_GROUP
                this.insertSampleValuesStmnt.setLongAtName(
                    "POLLUTANT_GROUP_ID",
                    parameterMapping.getPollutantGroupTagId());
                aggregationValue.setPollutantgroupKey(parameterMapping.getPollutantGroupKey());
                // this.insertSampleValuesStmnt.setString(3, mappedParameters[2]);

                // SAMPLE_DATE
                final Date sampleDate = sampleValuesResultSet.getDate("SAMPLE_DATE");
                // this.insertSampleValuesStmnt.setDateAtName("MIN_DATE", sampleDate);
                aggregationValue.setMinDate(sampleDate);
                this.insertSampleValuesStmnt.setDateAtName("MAX_DATE", sampleDate);
                aggregationValue.setMaxDate(sampleDate);

                // SAMPLE_VALUE
                float sampleValue = sampleValuesResultSet.getFloat("SAMPLE_VALUE");
                // convert if necessary
                if ((parameterMapping.getParameterAggregationExpression() != null)
                            && !parameterMapping.getParameterAggregationExpression().isEmpty()) {
                    sampleValue = convertAggregationValue(
                            sampleValue,
                            parameterMapping.getParameterAggregationExpression());
//                    if (log.isDebugEnabled()) {
//                        log.debug("sample value '" + parameterMapping.getDisplayName()
//                                    + "' converted to " + sampleValue + ' ' + parameterMapping.getUnit());
//                    }
                }
                // ignore min value: samples values in index db not aggregated!
                // this.insertSampleValuesStmnt.setFloatAtName("MIN_VALUE", sampleValue);
                aggregationValue.setMinValue(sampleValue);
                this.insertSampleValuesStmnt.setFloatAtName("MAX_VALUE", sampleValue);
                aggregationValue.setMaxValue(sampleValue);

                // PROBE_PK
                aggregationValue.setProbePk(sampleValuesResultSet.getString("PROBE_PK"));
                if ((parameterMapping.getUnit() != null) && !parameterMapping.getUnit().isEmpty()) {
                    aggregationValue.setUnit(parameterMapping.getUnit());
                }

                // fill the list and eliminate duplicates
                aggregationValues.add(aggregationValue);

                // FIXME: define POJOs final String srcContentJson =
                // this.xmlClobToJsonString(sampleValuesResultSet.getClob("MESSWERTE_XML"));

                // SRC_CONTENT
                // this.insertSampleValuesStmnt.setStringAtName("SRC_CONTENT", srcContentJson);

                this.insertSampleValuesStmnt.executeUpdate();

                final ResultSet generatedKeys = this.insertSampleValuesStmnt.getGeneratedKeys();
                if ((null != generatedKeys)) {
                    while (generatedKeys.next()) {
                        sampeValueIds.add(generatedKeys.getLong(1));
                    }
                    generatedKeys.close();
                    added++;
                } else {
                    log.error("could not fetch generated key for inserted samples values for "
                                + this.waSource.toUpperCase()
                                + " Station " + stationSrcPk);
                }
            }
        }

        sampleValuesResultSet.close();

        if (added > 0) {
            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " sample values added for " + this.waSource.toUpperCase() + " Station "
                            + stationSrcPk
                            + ", " + sampeValueIds.size() + " IDs generated in "
                            + ((System.currentTimeMillis() - currentTime) / 1000) + "s.");
            }
        } else {
            log.warn("no supported sample values found in " + i + " available sample values for " + this.waSource
                        + " Station " + stationSrcPk + " in "
                        + ((System.currentTimeMillis() - currentTime) / 1000) + "s.");
        }

        return sampeValueIds;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   stationKey                     DOCUMENT ME!
     * @param   stationName                    DOCUMENT ME!
     * @param   stationDescription             DOCUMENT ME!
     * @param   stationResponsiblePartyTagKey  DOCUMENT ME!
     * @param   stationWaterTagKey             DOCUMENT ME!
     * @param   stationGeomId                  DOCUMENT ME!
     * @param   stationSrcPk                   DOCUMENT ME!
     * @param   stationSrcContent              DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertStation(final String stationKey,
            final String stationName,
            final String stationDescription,
            final String stationResponsiblePartyTagKey,
            final String stationWaterTagKey,
            final long stationGeomId,
            final String stationSrcPk,
            final String stationSrcContent) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("inserting " + this.waSource.toUpperCase() + " Station " + stationKey + ": '" + stationName
                        + "'");
        }
        final long startTime = System.currentTimeMillis();

        this.insertStationStmnt.setStringAtName("KEY", stationKey);
        this.insertStationStmnt.setStringAtName("NAME", stationName);
        this.insertStationStmnt.setStringAtName("DESCRIPTION", stationDescription);
        this.insertStationStmnt.setLongAtName("GEOMETRY", stationGeomId);
        this.insertStationStmnt.setStringAtName("ZUSTAENDIGE_STELLE", stationResponsiblePartyTagKey);
        this.insertStationStmnt.setStringAtName("GEW_NAME", stationWaterTagKey);
        this.insertStationStmnt.setStringAtName("SRC_MESSSTELLE_PK", stationSrcPk);
        this.insertStationStmnt.setStringAtName("SRC_CONTENT", stationSrcContent);

        this.insertStationStmnt.executeUpdate();
        final ResultSet generatedStationKeysRs = this.insertStationStmnt.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedStationKeysRs) && generatedStationKeysRs.next()) {
            generatedKey = generatedStationKeysRs.getLong(1);
        } else {
            log.error("could not fetch generated key for inserted " + this.waSource.toUpperCase() + " Station!");
        }
        if (log.isDebugEnabled()) {
            log.debug(this.waSource + " Station " + stationKey + " inserted in "
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
        final Logger logger = Logger.getLogger(WaImport.class);
        final String waSource = WAOW;
        WaImport waImport = null;
        try {
            if (args.length > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("loading " + waSource.toUpperCase() + " properties from: " + args[0]);
                }
                waImport = new WaImport(FileSystems.getDefault().getPath(args[0]),
                        waSource);
            } else {
                waImport = new WaImport(waSource);
            }

            final long startTime = System.currentTimeMillis();
            logger.info("Starting " + waSource.toUpperCase() + " Import ......");

            waImport.doBootstrap();
            final int stations = waImport.doImport();

            logger.info(stations + " " + waSource.toUpperCase() + " Stations successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            logger.error("could not create " + waSource.toUpperCase() + " import instance: " + ex.getMessage(), ex);
        } finally {
            try {
                if (waImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing source connection");
                    }
                    waImport.sourceConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close source connection", ex);
                System.exit(1);
            }

            try {
                if (waImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing target connection");
                    }
                    waImport.targetConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close target connection", ex);
                System.exit(1);
            }
        }
    }
}
