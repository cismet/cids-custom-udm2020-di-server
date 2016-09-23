/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaObjectNode;
import Sirius.server.sql.SQLTools;

import com.vividsolutions.jts.geom.Geometry;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import org.openide.util.lookup.ServiceProvider;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import de.cismet.cids.nodepermissions.NoNodePermissionProvidedException;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.QueryPostProcessor;
import de.cismet.cids.server.search.SearchException;

import de.cismet.cidsx.base.types.Type;

import de.cismet.cidsx.server.api.types.SearchInfo;
import de.cismet.cidsx.server.api.types.SearchParameterInfo;
import de.cismet.cidsx.server.search.RestApiCidsServerSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé <pascrow.dihe@cismet.de>
 * @version  $Revision$, $Date$
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
        searchParameterInfo.setType(Type.STRING);
        searchParameterInfo.setArray(true);
        searchParameterInfo.setDescription(
            "list of class name (table names) of search themes (e.g. EPRTR, BORIS_SITE)");
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

    protected final String defaultSearchStatementTpl;
    @Getter @Setter private String[] themes;

    @Getter @Setter private String[] pollutants;

    @Getter @Setter private String geometry;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new DefaultRestApiSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public DefaultRestApiSearch() throws IOException {
        this.defaultSearchStatementTpl = IOUtils.toString(
                DefaultRestApiSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/default-search-statement.tpl.sql"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection<MetaObjectNode> performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();

        if ((this.geometry != null) && !this.geometry.isEmpty()
                    && (this.pollutants != null)
                    && (this.pollutants.length != 0)
                    && (this.themes != null)
                    && (this.themes.length != 0)) {
            LOGGER.info("performing default search for " + themes.length + " themes and "
                        + this.pollutants.length + " pollutants with geometry (length: " + this.geometry.length()
                        + ")");
        } else {
            final String message = "cannot perform default search, missing parameters: "
                        + "geometry = " + ((this.geometry != null) ? this.geometry.length() : "null")
                        + this.geometry
                        + ", themes = " + ((this.themes != null) ? this.themes.length : "null")
                        + ", pollutants = " + ((this.pollutants != null) ? this.pollutants.length : "null");
            LOGGER.error(message); // NOI18N
            throw new SearchException(message);
        }

        final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
        if (metaService == null) {
            final String message = "active local server " + DOMAIN + "not found";
            LOGGER.error(message); // NOI18N
            throw new SearchException(message);
        }

        try {
            final String defaultSearchStatement = this.createDefaultSearchStatement();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(defaultSearchStatement);
            }

            final ArrayList<MetaObjectNode> resultNodes = new ArrayList<MetaObjectNode>();
            final ArrayList<MetaObjectNode> filteredNodes = new ArrayList<MetaObjectNode>();

            final ArrayList<ArrayList> resultSet = metaService.performCustomSearch(
                    defaultSearchStatement,
                    new QueryPostProcessor() {

                        @Override
                        public ArrayList<ArrayList> postProcess(final ArrayList<ArrayList> resultSet) {
                            for (final ArrayList row : resultSet) {
                                // Cashed Geometry
                                Geometry cachedGeometry = null;
                                try {
                                    final Object cachedGeometryTester = row.get(3);

                                    if (cachedGeometryTester != null) {
                                        cachedGeometry = SQLTools.getGeometryFromResultSetObject(
                                                cachedGeometryTester);
                                        row.set(3, cachedGeometry);
                                    }
                                } catch (Exception e) {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.warn(
                                            "cachedGeometry was not in the resultset for object "
                                                    + row.get(0),
                                            e); // NOI18N
                                    }
                                }
                            }
                            return resultSet;
                        }
                    });

            for (final ArrayList row : resultSet) {
                // FIXME: yet another hack to circumvent odd type behaviour
                final int cid = ((Number)row.get(0)).intValue();
                final int oid = ((Number)row.get(1)).intValue();
                String name = null;
                try {
                    name = (String)row.get(2);
                } catch (final Exception e) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("no name present for object " + row.get(0), e); // NOI18N
                    }
                }

                // Cached Geometry
                final Geometry cachedGeometry = (Geometry)row.get(3);

                // Lightweight Json
                String lightweightJson = null;
                try {
                    final Object tester = row.get(4);
                    if ((tester != null) && (tester instanceof String)) { // NOI18N
                        lightweightJson = (String)tester;                 // NOI18N
                    }
                } catch (Exception e) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(
                            "lightweightJson was not in the result set for object "
                                    + row.get(0),
                            e);                                           // NOI18N
                    }
                }

                try {
                    final MetaObjectNode resultNode = new MetaObjectNode(
                            DOMAIN,
                            getUser(),
                            oid,
                            cid,
                            name,
                            cachedGeometry,
                            lightweightJson);

                    resultNodes.add(resultNode);
                } catch (NoNodePermissionProvidedException noNodePermissionProvidedException) {
                    filteredNodes.add(noNodePermissionProvidedException.getMon());
                }
            }

            if (filteredNodes.size() > 0) {
                LOGGER.warn(filteredNodes.size() + " Objcets filtered due to insufficient permissions");
            }

            LOGGER.info(resultNodes.size() + " object found during default search for " + themes.length + " themes and "
                        + this.pollutants.length + " pollutants with geometry (length: " + this.geometry.length()
                        + ")");

            return resultNodes;
        } catch (final Throwable t) {
            final String message = "Could not perform default search for " + themes.length + " themes and "
                        + this.pollutants.length + " pollutants with geometry (length: "
                        + this.geometry.length() + "): " + t.getMessage();
            LOGGER.error(message, t);              // NOI18N
            throw new SearchException(message, t); // NOI18N
        }

//        int i = 0;
//        for (final int classId : themes) {
//            final Policy policy = CidsClassFactory.getFactory().createPolicy("STANDARD");
//
//            final MetaObjectNode node = new MetaObjectNode(
//                    themes[0],
//                    "TEST NODE #"
//                            + i,
//                    "Description of TEST NODE #"
//                            + i,
//                    "UDM2020-DI",
//                    i,
//                    classId,
//                    false,
//                    policy,
//                    0,
//                    null,
//                    true,
//                    null,
//                    geometry,
//                    lightweightJson);
//
//            nodes.add(node);
//            i++;
//        }
//
//        return nodes;
    }

    @Override
    public SearchInfo getSearchInfo() {
        return SEARCH_INFO;
    }

    /**
     * Many think that ; is statement terminator in SQL on Oracle. It isn't. The ; at an end of statement is used by the
     * client (for example SQLPlus) to tell where the statement ends and then sends the statement but not the ';' to the
     * Oracle server.
     *
     * @return  DOCUMENT ME!
     */
    protected String createDefaultSearchStatement() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("building default search sql statement for " + this.themes.length
                        + " themes and " + this.pollutants.length + " pollutants");
        }

        final StringBuilder classNamesBuilder = new StringBuilder();
        for (int i = 0; i < this.themes.length; i++) {
            classNamesBuilder.append('\'').append(this.themes[i]).append('\'');
            if (i < (this.themes.length - 1)) {
                classNamesBuilder.append(',');
            }
        }

        final StringBuilder tagKeysBuilder = new StringBuilder();
        for (int i = 0; i < this.pollutants.length; i++) {
            tagKeysBuilder.append('\'').append(this.pollutants[i]).append('\'');

            if (i < (this.pollutants.length - 1)) {
                tagKeysBuilder.append(',');
            }
        }

        final String defaultSearchStatement = this.defaultSearchStatementTpl.replace("%GEOMETRY%", this.geometry)
                    .replace("%CLASS_NAMES%", classNamesBuilder.toString())
                    .replace("%TAG_KEYS%", tagKeysBuilder.toString());

        return defaultSearchStatement;
    }
}
