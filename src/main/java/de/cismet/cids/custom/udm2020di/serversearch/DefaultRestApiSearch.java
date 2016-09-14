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

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;
import de.cismet.cidsx.base.types.Type;
import de.cismet.cidsx.server.api.types.SearchInfo;
import de.cismet.cidsx.server.api.types.SearchParameterInfo;
import de.cismet.cidsx.server.search.RestApiCidsServerSearch;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Pascal Dihé <pascal.dihe@cismet.de>
 */
@ServiceProvider(service = RestApiCidsServerSearch.class)
public class DefaultRestApiSearch extends AbstractCidsServerSearch implements RestApiCidsServerSearch {

    public static final SearchInfo SEARCH_INFO;

    static {
        SEARCH_INFO = new SearchInfo();
        SEARCH_INFO.setKey(DefaultRestApiSearch.class.getName());
        SEARCH_INFO.setName(DefaultRestApiSearch.class.getSimpleName());
        SEARCH_INFO.setDescription(
                "Meta Object Node Universal Search for SWITCH-ON pure REST clients");

        final List<SearchParameterInfo> parameterDescription 
                = new LinkedList<SearchParameterInfo>();
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

    @Override
    public Collection performServerSearch() throws SearchException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SearchInfo getSearchInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
