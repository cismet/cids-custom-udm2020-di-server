/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.boris;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaObjectNode;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.cismet.cids.custom.udm2020di.serversearch.CustomMaxValuesSearch;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class BorisCustomSearch extends AbstractCidsServerSearch implements CustomMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String DOMAIN = "UDM2020-DI";

    protected static final Logger log = Logger.getLogger(BorisCustomSearch.class);

    //~ Instance fields --------------------------------------------------------

    protected Map<String, Float> maxValues;

    protected final String maxSampleValueConditionTpl;
    protected final String borisCustomSearchTpl;

    private Collection<Integer> objectIds;

    private int classId = -1;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisCustomSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public BorisCustomSearch() throws IOException {
        this.maxSampleValueConditionTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/boris/max-sample-value-condition.tpl.sql"),
                "UTF-8");

        this.borisCustomSearchTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/boris/boris-custom-search.tpl.sql"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   classId    DOCUMENT ME!
     * @param   objectIds  DOCUMENT ME!
     * @param   maxValues  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createBorisCustomSearchStatement(
            final int classId,
            final Collection<Integer> objectIds,
            final Map<String, Float> maxValues) {
        String borisCustomSearchStatement;

        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            objectIdsBuilder.append('\'').append(objectIdsIterator.next()).append('\'');
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        final StringBuilder maxValuesBuilder = new StringBuilder();
        final Iterator<Entry<String, Float>> maxValuesIterator = maxValues.entrySet().iterator();
        while (maxValuesIterator.hasNext()) {
            final Entry<String, Float> maxValue = maxValuesIterator.next();

            String maxSampleValueConditionStatement;
            maxSampleValueConditionStatement = this.maxSampleValueConditionTpl.replace("%TAG_KEY%", maxValue.getKey());
            maxSampleValueConditionStatement = maxSampleValueConditionStatement.replace(
                    "%MAX_VALUE%",
                    String.valueOf(maxValue.getValue()));
            maxValuesBuilder.append(maxSampleValueConditionStatement);
            if (maxValuesIterator.hasNext()) {
                maxValuesBuilder.append(" \n OR ");
            }
        }

        borisCustomSearchStatement = this.borisCustomSearchTpl.replace("%CLASS_ID%", String.valueOf(classId));
        borisCustomSearchStatement = borisCustomSearchStatement.replace("%BORIS_SITE_IDS%", objectIdsBuilder);
        borisCustomSearchStatement = borisCustomSearchStatement.replace(
                "%MAX_SAMPLE_VALUE_CONDITIONS%",
                maxValuesBuilder);

        return borisCustomSearchStatement;
    }

    /**
     * Get the value of maxValues.
     *
     * @return  the value of maxValues
     */
    @Override
    public Map<String, Float> getMaxValues() {
        return maxValues;
    }

    /**
     * Set the value of maxValues.
     *
     * @param  maxValues  new value of maxValues
     */
    @Override
    public void setMaxValues(final Map<String, Float> maxValues) {
        this.maxValues = maxValues;
    }

    /**
     * Get the value of objectIds.
     *
     * @return  the value of objectIds
     */
    @Override
    public Collection<Integer> getObjectIds() {
        return objectIds;
    }

    /**
     * Set the value of objectIds.
     *
     * @param  objectIds  new value of objectIds
     */
    @Override
    public void setObjectIds(final Collection<Integer> objectIds) {
        this.objectIds = objectIds;
    }

    /**
     * Get the value of classId.
     *
     * @return  the value of classId
     */
    @Override
    public int getClassId() {
        return classId;
    }

    /**
     * Set the value of classId.
     *
     * @param  classId  new value of classId
     */
    @Override
    public void setClassId(final int classId) {
        this.classId = classId;
    }

    @Override
    public Collection<MetaObjectNode> performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        log.info("performing search for " + this.objectIds.size() + " objects of class #"
                    + this.classId + " and " + this.maxValues.size() + " max values");

        if (this.objectIds.isEmpty() || this.maxValues.isEmpty() || (this.classId == -1)) {
            log.warn("missing parameters, returning empty collection");
        }

        final Collection<MetaObjectNode> result = new ArrayList<MetaObjectNode>();
        final String borisCustomSearchStatement = this.createBorisCustomSearchStatement(classId, objectIds, maxValues);
        if (log.isDebugEnabled()) {
            log.debug(borisCustomSearchStatement);
        }

        final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
        if (metaService != null) {
            try {
                final ArrayList<ArrayList> resultSet = metaService.performCustomSearch(borisCustomSearchStatement);

                for (final ArrayList row : resultSet) {
                    final int classID = (Integer)row.get(0);
                    final int objectID = (Integer)row.get(1);
                    final String name = (String)row.get(2);

                    final MetaObjectNode node = new MetaObjectNode(DOMAIN, objectID, classID, name);

                    result.add(node);
                }
            } catch (RemoteException ex) {
                log.error(ex.getMessage(), ex);
            }
        } else {
            log.error("active local server " + DOMAIN + "not found"); // NOI18N
        }

        log.info(result.size() + " objects found in "
                    + (System.currentTimeMillis() + startTime) + "ms");
        return result;
    }
}
