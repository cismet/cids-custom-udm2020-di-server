/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.boris;

import Sirius.server.middleware.impls.domainserver.DomainServerImpl;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.rmi.RemoteException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.serversearch.PostFilterTagsSearch;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
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

public class PostFilterAggregationValuesSearch extends AbstractCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String DOMAIN = "UDM2020-DI";

    protected static final Logger LOG = Logger.getLogger(PostFilterTagsSearch.class);

    //~ Instance fields --------------------------------------------------------

    @Getter
    @Setter
    protected Collection<Integer> objectIds;

    protected String getAggregationValuesTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValuesTagSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public PostFilterAggregationValuesSearch() throws IOException {
        this.getAggregationValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/boris/get-postfilter-aggregation-values.tpl.sql"),
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("building sql statement for " + objectIds.size() + "object ids");
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(getAggregationValuesSearchStatement);
        }

        return getAggregationValuesSearchStatement;
    }

    @Override
    public Collection performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        final AggregationValues aggregationValues = new AggregationValues();

        if ((this.objectIds != null) && !this.objectIds.isEmpty()) {
            LOG.info("performing search for aggregation values of "
                        + this.objectIds.size() + " different objects.");

            final String getAggregationValuesSearchStatement = this.createAggregationValuesSearchStatement(
                    this.objectIds);
            if (LOG.isDebugEnabled()) {
                LOG.debug(getAggregationValuesSearchStatement);
            }

            // final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
            // if (metaService != null) {
            Statement aggregationValuesSearchStmnt = null;
            try {
                final Connection connection = DomainServerImpl.getServerInstance().getConnectionPool().getConnection();
                aggregationValuesSearchStmnt = connection.createStatement();
                final ResultSet aggregationValuesSearchResult = aggregationValuesSearchStmnt.executeQuery(
                        getAggregationValuesSearchStatement);
                while (aggregationValuesSearchResult.next()) {
                    final AggregationValuesBean aggregationValuesBean = OracleExport.JSON_MAPPER.readValue(
                            aggregationValuesSearchResult.getClob(1).getCharacterStream(),
                            AggregationValuesBean.class);
                    aggregationValues.addAll(aggregationValuesBean.getAggregationValues());
                }

                aggregationValuesSearchResult.close();
                aggregationValuesSearchStmnt.close();

                if (aggregationValues.isEmpty()) {
                    LOG.warn("no aggregation values tags found!");
                } else {
                    LOG.info(aggregationValues.size() + " aggregation values found and processed for "
                                + this.objectIds.size() + " objects in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (RemoteException ex) {
                LOG.error(ex.getMessage(), ex);
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
            } catch (SQLException ex) {
                LOG.error(ex.getMessage(), ex);
                try {
                    if (aggregationValuesSearchStmnt != null) {
                        aggregationValuesSearchStmnt.close();
                    }
                } catch (SQLException sx) {
                    LOG.error(sx.getMessage(), sx);
                }
            }
//            } else {
//                LOG.error("active local server " + DOMAIN + " not found"); // NOI18N
//            }
        } else {
            LOG.warn("missing parameters, returning empty collection");
        }

        return new ArrayList<AggregationValue>(aggregationValues);
    }
}
