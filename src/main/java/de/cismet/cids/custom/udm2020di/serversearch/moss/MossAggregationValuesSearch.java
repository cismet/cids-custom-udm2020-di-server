/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.moss;

import Sirius.server.middleware.interfaces.domainserver.MetaService;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.math.BigDecimal;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import de.cismet.cids.custom.udm2020di.serversearch.PostFilterAggregationValuesSearch;
import de.cismet.cids.custom.udm2020di.serversearch.boris.BorisAggregationValuesSearch;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.moss.Moss;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public class MossAggregationValuesSearch extends AbstractCidsServerSearch implements PostFilterAggregationValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Date DATE = Moss.MIN_DATE;

    protected static final String DOMAIN = "UDM2020-DI";

    protected static final Logger LOGGER = Logger.getLogger(MossAggregationValuesSearch.class);

    //~ Instance fields --------------------------------------------------------

    @Getter
    @Setter
    protected Collection<Integer> objectIds;

    protected final String getAggregationValuesTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValuesTagSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public MossAggregationValuesSearch() throws IOException {
        this(IOUtils.toString(
                BorisAggregationValuesSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/get-postfilter-aggregation-values.tpl.sql"),
                "UTF-8"));
    }

    /**
     * Creates a new PostFilterAggregationValuesSearch object.
     *
     * @param  getAggregationValuesTpl  DOCUMENT ME!
     */
    protected MossAggregationValuesSearch(final String getAggregationValuesTpl) {
        this.getAggregationValuesTpl = getAggregationValuesTpl;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   objectIds  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createAggregationValuesSearchStatement(
            final Collection<Integer> objectIds) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("building sql statement for " + objectIds.size() + " object ids");
        }

        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            objectIdsBuilder.append(objectIdsIterator.next());
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        final String getAggregationValuesSearchStatement = this.getAggregationValuesTpl.replace(
                "%OBJECT_IDS%",
                objectIdsBuilder);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getAggregationValuesSearchStatement);
        }

        return getAggregationValuesSearchStatement;
    }

    @Override
    public Collection performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        final AggregationValues aggregationValues = new AggregationValues();

        if ((this.objectIds != null) && !this.objectIds.isEmpty()) {
            LOGGER.info("performing search for aggregation values of "
                        + this.objectIds.size() + " different objects.");

            final String getAggregationValuesSearchStatement = this.createAggregationValuesSearchStatement(
                    this.objectIds);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(getAggregationValuesSearchStatement);
            }

            final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
            if (metaService != null) {
                try {
                    final ArrayList<ArrayList> result = metaService.performCustomSearch(
                            getAggregationValuesSearchStatement);
                    if (!result.isEmpty() && !result.get(0).isEmpty()) {
                        if (result.size() > 1) {
                            LOGGER.warn("statement returned more than two rows, that should not happen!");
                        } else if (result.get(0).size() != 16) {
                            LOGGER.warn("invalid resultset: expected 16 values but got " + result.get(0).size());
                        }

                        final ArrayList<BigDecimal> resultList = result.get(0);
                        final Iterator<BigDecimal> iterator = resultList.iterator();

                        aggregationValues.add(new AggregationValue(
                                "Al",
                                "mg/kg",
                                null,
                                null,
                                "Al",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "As",
                                "mg/kg",
                                null,
                                null,
                                "As",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Cd",
                                "mg/kg",
                                null,
                                null,
                                "Cd",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Co",
                                "mg/kg",
                                null,
                                null,
                                "Co",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Cr",
                                "mg/kg",
                                null,
                                null,
                                "Cr",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Cu",
                                "mg/kg",
                                null,
                                null,
                                "Cu",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Fe",
                                "mg/kg",
                                null,
                                null,
                                "Fe",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Mo",
                                "mg/kg",
                                null,
                                null,
                                "Mo",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Ni",
                                "mg/kg",
                                null,
                                null,
                                "Ni",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Pb",
                                "mg/kg",
                                null,
                                null,
                                "Pb",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "S",
                                "mg/kg",
                                null,
                                null,
                                "S",
                                "DNM",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "V",
                                "mg/kg",
                                null,
                                null,
                                "V",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Sb",
                                "mg/kg",
                                null,
                                null,
                                "Sb",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Zn",
                                "mg/kg",
                                null,
                                null,
                                "Zn",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "Hg",
                                "mg/kg",
                                null,
                                null,
                                "Hg",
                                "MET",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                        aggregationValues.add(new AggregationValue(
                                "N ges.",
                                "mg/kg",
                                null,
                                null,
                                "N",
                                "DNM",
                                DATE,
                                DATE,
                                iterator.next().floatValue(),
                                iterator.next().floatValue()));
                    }

                    if (aggregationValues.isEmpty()) {
                        LOGGER.warn("no aggregation values tags found!");
                    } else {
                        LOGGER.info(aggregationValues.size() + " aggregation values found and processed for "
                                    + this.objectIds.size() + " objects in "
                                    + (System.currentTimeMillis() - startTime) + "ms");
                    }
                } catch (RemoteException ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
            } else {
                LOGGER.error("active local server " + DOMAIN + " not found"); // NOI18N
            }
        } else {
            LOGGER.warn("missing parameters, returning empty collection");
        }

        return aggregationValues;
    }
}
