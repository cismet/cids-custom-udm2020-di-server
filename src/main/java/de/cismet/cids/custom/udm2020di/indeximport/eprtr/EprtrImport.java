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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.ParameterMapping;
import de.cismet.cids.custom.udm2020di.types.ParameterMappings;
import de.cismet.cids.custom.udm2020di.types.eprtr.Activity;
import de.cismet.cids.custom.udm2020di.types.eprtr.Address;
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
    protected final OraclePreparedStatement getReleasesStmnt;
    protected final OraclePreparedStatement insertInstallationStmnt;
    protected final OraclePreparedStatement deleteInstallationStmnt;
    protected final OraclePreparedStatement insertReleaseStmnt;
    protected final OraclePreparedStatement insertInstallationTagsRelStmnt;
    protected final OraclePreparedStatement getTagsStnmt;
    protected final OraclePreparedStatement updateInstallationJsonStnmt;
    protected final ParameterMappings parameterMappings = new ParameterMappings();
    protected final Map<String, String> reflistMap = new HashMap<String, String>();
    protected final List<String> productsReflist = new ArrayList<String>();
    protected final Random random = new Random();

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

        getReleasesStmnt = (OraclePreparedStatement)this.sourceConnection.prepareStatement(
                getEprtrReleasesTpl);

        final String insertEprtrInstallationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/insert-eprtr-installation.prs.sql"),
                "UTF-8");
        insertInstallationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertEprtrInstallationTpl,
                new String[] { "ID" });

        final String deleteEprtrInstallationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/delete-eprtr-installation.prs.sql"),
                "UTF-8");
        deleteInstallationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                deleteEprtrInstallationTpl);

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

        this.initEprtrReflists();
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
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    protected final void initEprtrReflists() throws IOException, SQLException {
        final long startTime = System.currentTimeMillis();

        final String selectEprtrReflistTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-reflist.sql"),
                "UTF-8");
        final Statement selectEprtrReflistStmnt = this.targetConnection.createStatement();
        final ResultSet eprtrReflistResultSet = selectEprtrReflistStmnt.executeQuery(selectEprtrReflistTpl);

        while (eprtrReflistResultSet.next()) {
            this.reflistMap.put(
                eprtrReflistResultSet.getString(1),
                eprtrReflistResultSet.getString(2));
        }

        eprtrReflistResultSet.close();
        selectEprtrReflistStmnt.close();

        final String selectProductsReflistTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/eprtr/select-eprtr-reflist-products.sql"),
                "UTF-8");
        final Statement selectEprtrProductsStmnt = this.targetConnection.createStatement();
        final ResultSet eprtrProductsResultSet = selectEprtrProductsStmnt.executeQuery(selectProductsReflistTpl);

        while (eprtrProductsResultSet.next()) {
            this.productsReflist.add(
                eprtrProductsResultSet.getString(2));
        }

        eprtrProductsResultSet.close();
        selectEprtrProductsStmnt.close();

        if (log.isDebugEnabled()) {
            log.debug(this.reflistMap.size() + " reference value mappings cached in "
                        + ((System.currentTimeMillis() - startTime) / 1000) + "s");
        }
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
        long startTime = System.currentTimeMillis();
        log.info("fetching EPRTR installations from Source Connection " + this.sourceConnection.getSchema());
        final ResultSet installationsResultSet = getInstallationsStatement.executeQuery(getInstallationsStatementTpl);
        if (log.isDebugEnabled()) {
            log.debug("EPRTR installations fetched in " + ((System.currentTimeMillis() - startTime) / 1000) + "s");
        }
        int i = 0;

        while (installationsResultSet.next() && (i < 25)) {
            try {
                startTime = System.currentTimeMillis();
                ++i;

                final long installationSrcPk = installationsResultSet.getLong("ERAS_ID");
                if (log.isDebugEnabled()) {
                    log.debug("processing EPRTR Installation #" + (i) + ": " + installationSrcPk);
                }

                // key
                final String installationKey = "EPRTR." + installationSrcPk;

                String tmpStr = installationsResultSet.getNString("NAME");
                final String installationName = ((tmpStr != null) && !tmpStr.isEmpty())
                    ? tmpStr : String.valueOf(installationSrcPk);
                // description
                final String installationDescription = installationName;

                // NACE CODE TAG for Catalogue
                tmpStr = installationsResultSet.getNString("NACE_ID");
                final String installationNaceTagKey = tmpStr;
                final String installationNaceTagName = this.getReferenceValue(installationNaceTagKey);
                // -> INSERT NACE TAG
                this.insertUniqueTag(
                    installationNaceTagKey,
                    installationNaceTagName,
                    installationNaceTagName,
                    "k73nnukdrw9ipc");

                // RIVER_CATCHMENT TAG for Catalogue
                tmpStr = installationsResultSet.getNString("RIVER_CATCHMENT");
                final String installationCatchmentTagKey = tmpStr;
                final String installationCatchmentTagName = this.getReferenceValue(installationCatchmentTagKey);
                // -> INSERT RIVER_CATCHMENT TAG
                this.insertUniqueTag(
                    installationCatchmentTagKey,
                    installationCatchmentTagName,
                    installationNaceTagName,
                    "6ear7vjaxz728j");

                // GEOM
                final float installationLon = installationsResultSet.getFloat("LONGITUDE");
                final float installationLat = installationsResultSet.getFloat("LATITUDE");
                // -> INSERT GEOM and GET ID!
                final long installationGeomId = this.insertGeomPoint(
                        installationLon,
                        installationLat,
                        4326,
                        4326);
                if (installationGeomId == -1) {
                    --i;
                    continue;
                }

                // -> INSERT Installation
                final long eprtrInstallationId = insertInstallation(
                        installationKey,
                        installationName,
                        installationDescription,
                        installationNaceTagKey,
                        installationCatchmentTagKey,
                        installationGeomId,
                        installationSrcPk);
                if (eprtrInstallationId == -1) {
                    --i;
                    continue;
                }

                final Installation eprtrInstallation = XML_MAPPER.readValue(installationsResultSet.getClob(
                            "INSTALLATION_XML").getCharacterStream(),
                        Installation.class);
                this.updateInstallationReferenceValues(eprtrInstallation);

                // -> SAMPLE VALUES AND TAGS
                // AggregationValues -> collection impl. that stores only maximum/minimum values!
                final Collection<AggregationValue> aggregationValues = new AggregationValues();
                final Collection<Long> releaseIds = getAndInsertReleases(
                        eprtrInstallationId,
                        installationSrcPk,
                        aggregationValues);

                // set unique aggregation values
                eprtrInstallation.setAggregationValues(new ArrayList<AggregationValue>(aggregationValues));

                if (!releaseIds.isEmpty()) {
                    this.insertEprtrInstallationTagsRelation(eprtrInstallationId);

                    final ObjectNode jsonObject = (ObjectNode)JSON_MAPPER.valueToTree(eprtrInstallation);
                    final ObjectNode jsonObjectConfidential = (ObjectNode)JSON_MAPPER.valueToTree(
                            this.inferRestrictedActivities(eprtrInstallation));

                    this.updateSrcJson(eprtrInstallationId, jsonObject, jsonObjectConfidential);
                } else {
                    log.warn("removing EPRTR Installation #" + (--i) + " '" + installationSrcPk
                                + "': no supported releases found!");
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
                                + " aggregated releases processed and imported in "
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
     * TODO.
     *
     * @param   installation  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  CloneNotSupportedException  DOCUMENT ME!
     */
    protected Installation inferRestrictedActivities(final Installation installation)
            throws CloneNotSupportedException {
        final Installation restrictedInstallation = new Installation();
        restrictedInstallation.setErasId(installation.getErasId());
        final List<Activity> activities = installation.getActivities();
        if ((activities != null) && !activities.isEmpty()) {
            final ArrayList<Activity> restrictedActivities = new ArrayList<Activity>(activities.size());
            for (final Activity activity : activities) {
                final Activity restrictedActivity = (Activity)activity.clone();
                restrictedActivity.setProduct(
                    this.productsReflist.get(random.nextInt(this.productsReflist.size())));
                restrictedActivity.setProductionVolume(random.nextInt(5000000));
                restrictedActivity.setOperatingHours(random.nextInt(8760));
                restrictedActivities.add(restrictedActivity);
            }

            restrictedInstallation.setActivities(restrictedActivities);
        }

        return restrictedInstallation;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   eprtrInstallationId     DOCUMENT ME!
     * @param   jsonNode                DOCUMENT ME!
     * @param   jsonObjectConfidential  DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     * @throws  IOException              DOCUMENT ME!
     */
    protected void updateSrcJson(
            final long eprtrInstallationId,
            final ObjectNode jsonNode,
            final ObjectNode jsonObjectConfidential) throws SQLException, JsonProcessingException, IOException {
        getTagsStnmt.setLongAtName("INSTALLATION_ID", eprtrInstallationId);
        final ResultSet getTagsResult = getTagsStnmt.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("tags", getTagsResult);

        try {
            final Clob srcContentClob = this.targetConnection.createClob();
            final Writer clobWriter = srcContentClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobWriter, jsonNode);

            final Clob srcContentConfidentialClob = this.targetConnection.createClob();
            final Writer clobConfidentialWriter = srcContentConfidentialClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobConfidentialWriter, jsonObjectConfidential);

            updateInstallationJsonStnmt.setClob(1, srcContentClob);
            updateInstallationJsonStnmt.setClob(2, srcContentConfidentialClob);
            updateInstallationJsonStnmt.setLong(3, eprtrInstallationId);
            updateInstallationJsonStnmt.setLong(4, eprtrInstallationId);
            updateInstallationJsonStnmt.setLong(5, eprtrInstallationId);

            updateInstallationJsonStnmt.executeUpdate();

            clobWriter.close();
            clobConfidentialWriter.close();
        } catch (Exception ex) {
            log.error("could not deserialize and update JSON of EPRTR Installation "
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
        this.insertInstallationTagsRelStmnt.setLongAtName("INSTALLATION_ID", eprtrInstallationId);
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
     * @throws  SQLException                DOCUMENT ME!
     * @throws  IOException                 DOCUMENT ME!
     * @throws  CloneNotSupportedException  DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertReleases(final long eprtrInstallationId,
            final long installationSrcPk,
            final Collection<AggregationValue> aggregationValues) throws SQLException,
        IOException,
        CloneNotSupportedException {
        final Collection<Long> sampeValueIds = new HashSet<Long>();
        int i = 0;
        int added = 0;

        // <- GET AGGREGATED SAMPLE VALUES
        this.getReleasesStmnt.setLongAtName("INSTALLATION_ERAS_ID", installationSrcPk);
        final ResultSet releasesResultSet = this.getReleasesStmnt.executeQuery();

        while (releasesResultSet.next()) {
            final String POLLUTANT_KEY = releasesResultSet.getString("POLLUTANT_KEY");
            i++;
            if (this.parameterMappings.containsKey(POLLUTANT_KEY)) {
                final AggregationValue aggregationValue = new AggregationValue();

                // aggregation parameter mapping!
                final ParameterMapping parameterMapping = this.parameterMappings.getAggregationMapping(POLLUTANT_KEY);
                aggregationValue.setUnit(parameterMapping.getUnit());

                // KEY and SCR_ID
                final long RELEASE_ID = releasesResultSet.getLong("RELEASE_ID");
                this.insertReleaseStmnt.setStringAtName("KEY", String.valueOf(RELEASE_ID));
                this.insertReleaseStmnt.setStringAtName("SRC_RELEASE_ID", String.valueOf(RELEASE_ID));

                // NAME
                // log.debug(mappedParameters[0]);
                this.insertReleaseStmnt.setStringAtName("NAME", parameterMapping.getDisplayName());
                aggregationValue.setName(parameterMapping.getDisplayName());

                // RELEASE_TYPE
                final String RELEASE_TYPE = releasesResultSet.getString("RELEASE_TYPE");
                this.insertReleaseStmnt.setStringAtName("RELEASE_TYPE", RELEASE_TYPE);
                aggregationValue.setReleaseType(RELEASE_TYPE);

                // NOTIFICATION_PERIOD
                final String NOTIFICATION_PERIOD = releasesResultSet.getString("NOTIFICATION_PERIOD");
                this.insertReleaseStmnt.setStringAtName("NOTIFICATION_PERIOD", NOTIFICATION_PERIOD);

                // INSTALLATION
                this.insertReleaseStmnt.setLongAtName("INSTALLATION", eprtrInstallationId);

                // POLLUTANT
                this.insertReleaseStmnt.setLongAtName("POLLUTANT_ID", parameterMapping.getPollutantTagId());
                aggregationValue.setPollutantKey(parameterMapping.getPollutantTagKey());

                // POLLUTANT_GROUP
                this.insertReleaseStmnt.setLongAtName("POLLUTANT_GROUP_ID", parameterMapping.getPollutantGroupTagId());
                aggregationValue.setPollutantgroupKey(parameterMapping.getPollutantGroupKey());

                // MIN_DATE
                final Date minDate = releasesResultSet.getDate("START_DATE");
                this.insertReleaseStmnt.setDateAtName("MIN_DATE", minDate);
                aggregationValue.setMinDate(minDate);

                // MAX_DATE
                final Date maxDate = releasesResultSet.getDate("END_DATE");
                this.insertReleaseStmnt.setDateAtName("MAX_DATE", maxDate);
                aggregationValue.setMaxDate(maxDate);

                // VALUE
                final float QUANTITY_RELEASED = releasesResultSet.getFloat("QUANTITY_RELEASED");
                this.insertReleaseStmnt.setFloatAtName("VALUE", QUANTITY_RELEASED);
                aggregationValue.setMinValue(QUANTITY_RELEASED);
                aggregationValue.setMaxValue(QUANTITY_RELEASED);

                // SRC_CONTENT
                final String srcContentJson = this.xmlClobToJsonString(releasesResultSet.getClob("ACTIVITIES_XML"));
                this.insertReleaseStmnt.setStringAtName("SRC_CONTENT", srcContentJson);

                // fill the list and eliminate duplicates
                aggregationValues.add(aggregationValue);

                this.insertReleaseStmnt.executeUpdate();
                final ResultSet generatedKeys = this.insertReleaseStmnt.getGeneratedKeys();
                if ((null != generatedKeys)) {
                    while (generatedKeys.next()) {
                        sampeValueIds.add(generatedKeys.getLong(1));
                    }
                    generatedKeys.close();
                    added++;
                } else {
                    log.error("could not fetch generated key for inserted releases for EPRTR Installation "
                                + installationSrcPk);
                }
            }
        }

        releasesResultSet.close();

        if (added > 0) {
            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " releases added for EPRTR Installation " + installationSrcPk
                            + ", " + sampeValueIds.size() + " IDs generated");
            }
        } else {
            log.warn("no supported releases found in " + i + " available releases for EPRTR Installation "
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
     * @param   installationNaceTagKey       DOCUMENT ME!
     * @param   installationCatchmentTagKey  DOCUMENT ME!
     * @param   installationGeomId           DOCUMENT ME!
     * @param   installationSrcPk            DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertInstallation(final String installationKey,
            final String installationName,
            final String installationDescription,
            final String installationNaceTagKey,
            final String installationCatchmentTagKey,
            final long installationGeomId,
            final long installationSrcPk) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("inserting EPRTR Installation " + installationKey + ": '" + installationName + "'");
        }
        final long startTime = System.currentTimeMillis();

        this.insertInstallationStmnt.setStringAtName("KEY", installationKey);
        this.insertInstallationStmnt.setStringAtName("NAME", installationName);
        this.insertInstallationStmnt.setStringAtName("DESCRIPTION", installationDescription);
        this.insertInstallationStmnt.setLongAtName("GEOMETRY", installationGeomId);
        this.insertInstallationStmnt.setStringAtName("NACE_CLASS", installationNaceTagKey);
        this.insertInstallationStmnt.setStringAtName("RIVER_CATCHMENT", installationCatchmentTagKey);
        this.insertInstallationStmnt.setLongAtName("SRC_ERAS_ID", installationSrcPk);

        this.insertInstallationStmnt.executeUpdate();
        final ResultSet generatedInstallationKeysRs = this.insertInstallationStmnt.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedInstallationKeysRs) && generatedInstallationKeysRs.next()) {
            generatedKey = generatedInstallationKeysRs.getLong(1);
        } else {
            log.error("could not fetch generated key for inserted EPRTR Installation!");
        }

        if (log.isDebugEnabled()) {
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
     * @param  installation  DOCUMENT ME!
     */
    protected void updateInstallationReferenceValues(final Installation installation) {
        if ((installation.getNaceClass() != null)
                    && !installation.getNaceClass().isEmpty()) {
            installation.setNaceClass(this.getReferenceValue(installation.getNaceClass()));
        }

        if ((installation.getRiverCatchment() != null)
                    && !installation.getRiverCatchment().isEmpty()) {
            installation.setRiverCatchment(this.getReferenceValue(installation.getRiverCatchment()));
        }

        if (installation.getAddresses() != null) {
            for (final Address address : installation.getAddresses()) {
                address.setCity(this.getReferenceValue(address.getCity()));
                address.setDistrict(this.getReferenceValue(address.getDistrict()));
                address.setRegion(this.getReferenceValue(address.getRegion()));
            }
        } else {
            log.warn("EPRTR Installation '" + installation.getName()
                        + "' without addresses!");
        }

        if (installation.getReleaseParameters() != null) {
            final HashMap<String, Parameter> uniqueReleaseParameters = new HashMap<String, Parameter>();
            for (final Parameter parameter : installation.getReleaseParameters()) {
                final String key = parameter.getParameterPk();
                if (!uniqueReleaseParameters.containsKey(key)) {
                    parameter.setParameterName(this.getReferenceValue(key));
                    uniqueReleaseParameters.put(key, parameter);
                }
            }

            installation.setReleaseParameters(
                new ArrayList<Parameter>(uniqueReleaseParameters.values()));
        } else {
            log.warn("EPRTR Installation '" + installation.getName()
                        + "' without release parameters!");
        }
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

            logger.info("Starting EPRTR Import ......");
            final int installations = eprtrImport.doImport();

            logger.info(installations + " EPRTR Installations successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
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
