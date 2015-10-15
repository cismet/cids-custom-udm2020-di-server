/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.boris;

import Sirius.server.middleware.interfaces.domainserver.MetaService;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import org.openide.util.Exceptions;

import java.io.IOException;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.serversearch.PostFilterTagsSearch;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.AggregationValuesBean;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public class AggregationValuesTagSearch extends AbstractCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String DOMAIN = "UDM2020-DI";

    //~ Instance fields --------------------------------------------------------

    @Getter
    @Setter
    protected Collection<Integer> objectIds;

    protected Logger log;

    protected String getAggregationValuesTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValuesTagSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public AggregationValuesTagSearch() throws IOException {
        this.log = Logger.getLogger(PostFilterTagsSearch.class);
        this.getAggregationValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/boris/get-postfilter-aggregation-values.tpl"),
                "UTF-8");
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
        if (log.isDebugEnabled()) {
            log.debug("building sql statement for " + objectIds.size() + "object ids");
        }

        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            // objectIdsBuilder.append('\'').append(objectIdsIterator.next()).append('\'');
            objectIdsBuilder.append(objectIdsIterator.next());
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        final String getAggregationValuesSearchStatement = this.getAggregationValuesTpl.replace(
                "%OBJECT_IDS%",
                objectIdsBuilder);
        if (log.isDebugEnabled()) {
            log.debug(getAggregationValuesSearchStatement);
        }

        return getAggregationValuesSearchStatement;
    }

    @Override
    public Collection performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        final AggregationValues aggregationValues = new AggregationValues();

        if ((this.objectIds != null) && !this.objectIds.isEmpty()) {
            log.info("performing search for aggregation values of "
                        + this.objectIds.size() + " different objects.");

            final String getAggregationValuesSearchStatement = this.createAggregationValuesSearchStatement(
                    this.objectIds);
            if (log.isDebugEnabled()) {
                log.debug(getAggregationValuesSearchStatement);
            }

//OracleExport.JSON_MAPPER.readValue(DOMAIN, null)

            final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
            if (metaService != null) {
                try {
                    final ArrayList<ArrayList> resultSet = metaService.performCustomSearch(
                            getAggregationValuesSearchStatement);

                    if (resultSet.isEmpty()) {
                        log.warn("no aggregation values tags found!");
                    } else {
                        for (final ArrayList row : resultSet) {
                            final String jsonContent = row.get(0).toString();
                            final AggregationValuesBean aggregationValuesBean = OracleExport.JSON_MAPPER.readValue(
                                    jsonContent,
                                    AggregationValuesBean.class);
                            aggregationValues.addAll(aggregationValuesBean.getAggregationValues());
                        }

                        log.info(resultSet.size() + " aggregation values found and processed in "
                                    + (System.currentTimeMillis() - startTime) + "ms");

                        return Arrays.asList(resultSet);
                    }
                } catch (RemoteException ex) {
                    log.error(ex.getMessage(), ex);
                } catch (IOException ex) {
                    log.error(ex.getMessage(), ex);
                }
            } else {
                log.error("active local server " + DOMAIN + " not found"); // NOI18N
            }
        } else {
            log.warn("missing parameters, returning empty collection");
        }

        return aggregationValues;
    }
}
