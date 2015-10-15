/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaObject;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * Search for Postfilter Tags.
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class PostFilterTagsSearch extends AbstractCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String DOMAIN = "UDM2020-DI";

    protected static final Logger log = Logger.getLogger(PostFilterTagsSearch.class);

    //~ Instance fields --------------------------------------------------------

    protected final String getPostfilterTagsTpl;

    @Getter
    @Setter
    protected Map<Integer, Collection<Integer>> objectIdMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new PostFilterTagsSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public PostFilterTagsSearch() throws IOException {
        this.getPostfilterTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/get-postfilter-tags.tpl.sql"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Parametrise the sql template.
     *
     * @param   objectIdMap  classIds
     *
     * @return  DOCUMENT ME!
     */
    protected String createPostfilterTagsSearchStatement(final Map<Integer, Collection<Integer>> objectIdMap) {
        final StringBuilder searchStatementBuilder = new StringBuilder();

        int i = 0;
        for (final Integer classId : objectIdMap.keySet()) {
            i++;
            final Collection<Integer> objectIds = objectIdMap.get(classId);
            final StringBuilder objectIdsBuilder = new StringBuilder();
            final Iterator<Integer> objectIdsIterator = objectIds.iterator();
            while (objectIdsIterator.hasNext()) {
                // objectIdsBuilder.append('\'').append(objectIdsIterator.next()).append('\'');
                objectIdsBuilder.append(objectIdsIterator.next());
                if (objectIdsIterator.hasNext()) {
                    objectIdsBuilder.append(',');
                }
            }

            String postfilterTagsSearchStatement = this.getPostfilterTagsTpl.replace(
                    "%CLASS_ID%",
                    classId.toString());
            postfilterTagsSearchStatement = postfilterTagsSearchStatement.replace(
                    "%OBJECT_IDS%",
                    objectIdsBuilder);
            searchStatementBuilder.append(postfilterTagsSearchStatement);

            if (i < objectIdMap.size()) {
                searchStatementBuilder.append('\n');
                searchStatementBuilder.append(" UNION ");
                searchStatementBuilder.append('\n');
            }
        }

        return searchStatementBuilder.toString();
    }

    @Override
    public Collection performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();

        if ((this.objectIdMap != null) && !this.objectIdMap.isEmpty()) {
            log.info("performing search for object tags of "
                        + this.objectIdMap.size() + " different classes.");

            final String postfilterTagsSearchStatement = this.createPostfilterTagsSearchStatement(this.objectIdMap);
            if (log.isDebugEnabled()) {
                log.debug(postfilterTagsSearchStatement);
            }

            final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
            if (metaService != null) {
                try {
                    final MetaObject[] resultSet = metaService.getMetaObject(this.getUser(),
                            postfilterTagsSearchStatement);

                    if (resultSet.length == 0) {
                        log.warn("no post filter tags found!");
                    } else {
                        log.info(resultSet.length + " post filter tags found in "
                                    + (System.currentTimeMillis() - startTime) + "ms");

                        return Arrays.asList(resultSet);
                    }
                } catch (RemoteException ex) {
                    log.error(ex.getMessage(), ex);
                }
            } else {
                log.error("active local server " + DOMAIN + " not found"); // NOI18N
            }
        } else {
            log.warn("missing parameters, returning empty collection");
        }

        return new ArrayList();
    }
}
