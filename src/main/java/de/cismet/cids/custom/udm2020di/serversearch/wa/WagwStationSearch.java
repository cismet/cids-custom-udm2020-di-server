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

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class WagwStationSearch extends AbstractMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(WagwStationSearch.class);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisCustomSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public WagwStationSearch() throws IOException {
        this.searchTpl = IOUtils.toString(WagwStationSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/wagw/wagw-station-search.tpl.sql"),
                "UTF-8");
        this.maxSampleValueConditionTpl = IOUtils.toString(WagwStationSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/wagw/max-sample-value-condition.tpl.sql"),
                "UTF-8");
    }
}
