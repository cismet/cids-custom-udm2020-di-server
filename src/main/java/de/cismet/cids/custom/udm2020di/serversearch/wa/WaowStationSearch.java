/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.wa;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import de.cismet.cids.custom.udm2020di.serversearch.AbstractMaxValuesSearch;
import de.cismet.cidsx.server.search.RestApiCidsServerSearch;
import org.openide.util.lookup.ServiceProvider;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@ServiceProvider(service = RestApiCidsServerSearch.class)
public class WaowStationSearch extends AbstractMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(WaowStationSearch.class);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisCustomSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public WaowStationSearch() throws IOException {
        this.searchTpl = IOUtils.toString(WaowStationSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/waow/waow-station-search.tpl.sql"),
                "UTF-8");
        this.maxSampleValueConditionTpl = IOUtils.toString(WaowStationSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/waow/max-sample-value-condition.tpl.sql"),
                "UTF-8");
    }
}
