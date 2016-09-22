/**
 * *************************************************
 *
 * cismet GmbH, Saarbruecken, Germany
 *
 *              ... and it just works.
 *
 ***************************************************
 */
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
import java.util.Arrays;
import java.util.Iterator;

/**
 * DOCUMENT ME!
 *
 * @author Pascal Dihé <pascal.dihe@cismet.de>
 * @version $Revision$, $Date$
 */
@ServiceProvider(service = RestApiCidsServerSearch.class)
public class DefaultRestApiSearch extends AbstractCidsServerSearch implements RestApiCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------
    protected static final String DOMAIN = "UDM2020-DI";

    protected static Logger LOGGER = Logger.getLogger(DefaultRestApiSearch.class);
    
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
        searchParameterInfo.setDescription("list of class name (table names) of search themes (e.g. EPRTR, BORIS_SITE)");
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
    @Getter
    @Setter
    private int[] themes;

    @Getter
    @Setter
    private String[] pollutants;

    @Getter
    @Setter
    private String geometry;
    
    protected final String defaultSearchStatementTpl;

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
        stringBuilder.append("{\"pollutants\": [{\n"
                + "                \"name\": \"PCP\",\n"
                + "                \"id\": 1\n"
                + "            }, {\n"
                + "                \"name\": \"Cd\",\n"
                + "                \"id\": 2\n"
                + "            }, {\n"
                + "                \"name\": \"Heptachlor\",\n"
                + "                \"id\": 3\n"
                + "            }, {\n"
                + "                \"name\": \"Xylole\",\n"
                + "                \"id\": 4\n"
                + "            }, {\n"
                + "                \"name\": \"Lindan\",\n"
                + "                \"id\": 5\n"
                + "            }, {\n"
                + "                \"name\": \"PER\",\n"
                + "                \"id\": 6\n"
                + "            }, {\n"
                + "                \"name\": \"Cl\",\n"
                + "                \"id\": 7\n"
                + "            }, {\n"
                + "                \"name\": \"Ca\",\n"
                + "                \"id\": 8\n"
                + "            }, {\n"
                + "                \"name\": \"HFKW\",\n"
                + "                \"id\": 9\n"
                + "            }, {\n"
                + "                \"name\": \"AOX\",\n"
                + "                \"id\": 10\n"
                + "            }]\n"
                + "    }");

        /*int i = 0;
        for (final String pollutant : pollutants) {
            stringBuilder.append("{\"pollutant\":\"").append(pollutant).append("\"}");
            if (i < (pollutants.length - 1)) {
                stringBuilder.append(',');
            }
        }
        stringBuilder.append("]");*/
        final String lightweightJson = stringBuilder.toString();

        final String wktGeometry
                = "SRID=4326;POLYGON((8.61328125 51.23440735163459,7.734374999999999 48.922499263758255,12.480468749999998 48.28319289548349,13.095703125 49.83798245308484,11.074218749999998 51.890053935216926,8.61328125 51.23440735163459))";
        final Geometry geometry = CidsBeanJsonDeserializer.fromEwkt(wktGeometry);

        int i = 0;
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
    
    protected String createDefaultSearchStatement() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("building default search sql statement for " + this.themes.length 
                    + " themes and " + this.pollutants.length + " pollutants");
        }

        final StringBuilder classIdsBuilder = new StringBuilder();
        for(int i = 0; i < this.themes.length; i++) {
             classIdsBuilder.append(this.themes[i]);
             
             if (i < this.themes.length-1) {
                classIdsBuilder.append(',');
            }
        }
        
        final StringBuilder tagKeysBuilder = new StringBuilder();
        for(int i = 0; i < this.pollutants.length; i++) {
             tagKeysBuilder.append('\'').append(this.pollutants[i]).append('\'');
             
             if (i < this.themes.length-1) {
                tagKeysBuilder.append(',');
            }
        }

        
        
        
        final String defaultSearchStatement = this.defaultSearchStatementTpl
                .replace("%GEOMETRY%", this.geometry)
                .replace("%CLASS_IDS%",classIdsBuilder.toString())
                .replace("%TAG_KEYS%",classIdsBuilder.toString());
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(defaultSearchStatement);
        }

        return defaultSearchStatement;
    }
}
