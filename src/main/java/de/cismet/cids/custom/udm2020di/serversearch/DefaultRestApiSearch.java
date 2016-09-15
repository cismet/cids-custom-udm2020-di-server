/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import Sirius.server.middleware.types.MetaObjectNode;
import Sirius.server.newuser.permission.Policy;

import com.vividsolutions.jts.geom.Geometry;

import lombok.Getter;
import lombok.Setter;

import org.openide.util.lookup.ServiceProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import de.cismet.cids.dynamics.CidsBeanJsonDeserializer;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

import de.cismet.cidsx.base.types.Type;

import de.cismet.cidsx.server.api.types.SearchInfo;
import de.cismet.cidsx.server.api.types.SearchParameterInfo;
import de.cismet.cidsx.server.api.types.legacy.CidsClassFactory;
import de.cismet.cidsx.server.search.RestApiCidsServerSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé <pascal.dihe@cismet.de>
 * @version  $Revision$, $Date$
 */
@ServiceProvider(service = RestApiCidsServerSearch.class)
public class DefaultRestApiSearch extends AbstractCidsServerSearch implements RestApiCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    public static final SearchInfo SEARCH_INFO;

    static {
        SEARCH_INFO = new SearchInfo();
        SEARCH_INFO.setKey(DefaultRestApiSearch.class.getName());
        SEARCH_INFO.setName(DefaultRestApiSearch.class.getSimpleName());
        SEARCH_INFO.setDescription(
            "Meta Object Node Universal Search for SWITCH-ON pure REST clients");

        final List<SearchParameterInfo> parameterDescription = new LinkedList<SearchParameterInfo>();
        SearchParameterInfo searchParameterInfo;

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("themes");
        searchParameterInfo.setType(Type.INTEGER);
        searchParameterInfo.setArray(true);
        searchParameterInfo.setDescription("list of class ids of search themes (e.g. EPRTR, BORIS_SITE)");
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("pollutants");
        searchParameterInfo.setType(Type.STRING);
        searchParameterInfo.setArray(true);
        searchParameterInfo.setDescription("list of pollutant (tag) keys");
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("geometry");
        searchParameterInfo.setType(Type.STRING);
        searchParameterInfo.setArray(false);
        searchParameterInfo.setDescription("WKT Search Geometry");
        parameterDescription.add(searchParameterInfo);

        SEARCH_INFO.setParameterDescription(parameterDescription);

        final SearchParameterInfo resultParameterInfo = new SearchParameterInfo();
        resultParameterInfo.setKey("return");
        resultParameterInfo.setDescription("Collection of Object Nodes");
        resultParameterInfo.setArray(true);
        resultParameterInfo.setType(Type.NODE);
        SEARCH_INFO.setResultDescription(resultParameterInfo);
    }

    //~ Instance fields --------------------------------------------------------

    @Getter @Setter private int[] themes;

    @Getter @Setter private String[] pollutants;

    @Getter @Setter private String geometry;

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection<MetaObjectNode> performServerSearch() throws SearchException {
        /*public MetaObjectNode(
         *  int id,  String name, String description,  String domain,  int objectId,  int classId,  boolean isLeaf,
         * Policy policy,  int iconFactory,  String icon,  boolean derivePermissionsFromClass,  String artificialId,
         * Geometry cashedGeometry,  String lightweightJson)
         */
        final ArrayList<MetaObjectNode> nodes = new ArrayList<MetaObjectNode>(themes.length);

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        int i = 0;
        for (final String pollutant : pollutants) {
            stringBuilder.append("{\"pollutant\":\"").append(pollutant).append("\"}");
            if (i < (pollutants.length - 1)) {
                stringBuilder.append(',');
            }
        }
        stringBuilder.append("]");
        final String lightweightJson = stringBuilder.toString();

        final String wktGeometry =
            "SRID=4326;POLYGON((8.61328125 51.23440735163459,7.734374999999999 48.922499263758255,12.480468749999998 48.28319289548349,13.095703125 49.83798245308484,11.074218749999998 51.890053935216926,8.61328125 51.23440735163459))";
        final Geometry geometry = CidsBeanJsonDeserializer.fromEwkt(wktGeometry);

        i = 0;
        for (final int classId : themes) {
            final Policy policy = CidsClassFactory.getFactory().createPolicy("STANDARD");

            final MetaObjectNode node = new MetaObjectNode(
                    themes[0],
                    "TEST NODE #"
                            + i,
                    "Description of TEST NODE #"
                            + i,
                    "UDM2020-DI",
                    i,
                    classId,
                    false,
                    policy,
                    0,
                    null,
                    true,
                    null,
                    geometry,
                    lightweightJson);

            nodes.add(node);
            i++;
        }

        return nodes;
    }

    @Override
    public SearchInfo getSearchInfo() {
        return SEARCH_INFO;
    }
}
