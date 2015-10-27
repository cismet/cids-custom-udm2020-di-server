/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.moss;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import de.cismet.cids.custom.udm2020di.serversearch.AbstractMaxValuesSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class MossSearch extends AbstractMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(MossSearch.class);
    protected static final HashMap<String, String> PARAMETER_MAPPING = new HashMap<String, String>();

    static {
        PARAMETER_MAPPING.put("Al", "AL_CONV");
        PARAMETER_MAPPING.put("As", "AS_CONV");
        PARAMETER_MAPPING.put("Cd", "CD_CONV");
        PARAMETER_MAPPING.put("Co", "CO_CONV");
        PARAMETER_MAPPING.put("Cr", "CR_CONV");
        PARAMETER_MAPPING.put("Cu", "CU_CONV");
        PARAMETER_MAPPING.put("Fe", "FE_CONV");
        PARAMETER_MAPPING.put("Mo", "MO_CONV");
        PARAMETER_MAPPING.put("Ni", "NI_CONV");
        PARAMETER_MAPPING.put("Pb", "PB_CONV");
        PARAMETER_MAPPING.put("S", "S_CONV");
        PARAMETER_MAPPING.put("V", "V_CONV");
        PARAMETER_MAPPING.put("Sb", "SB_CONV");
        PARAMETER_MAPPING.put("Sn", "ZN_CONV");
        PARAMETER_MAPPING.put("Hg", "HG_CONV");
        PARAMETER_MAPPING.put("N", "N_TOTAL");
    }

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MossSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public MossSearch() throws IOException {
        this.searchTpl = IOUtils.toString(MossSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/moss-search.tpl.sql"),
                "UTF-8");
        this.maxSampleValueConditionTpl = IOUtils.toString(MossSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/max-sample-value-condition.tpl.sql"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    protected String createSearchStatement(
            final int classId,
            final Collection<Integer> objectIds,
            final Map<String, Float> maxValues,
            final Date minDate,
            final Date maxDate) {
        // object ids
        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            objectIdsBuilder.append(objectIdsIterator.next());
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        // max values
        final StringBuilder maxValuesBuilder = new StringBuilder();
        final Iterator<Map.Entry<String, Float>> maxValuesIterator = maxValues.entrySet().iterator();
        while (maxValuesIterator.hasNext()) {
            final Map.Entry<String, Float> maxValue = maxValuesIterator.next();
            String maxSampleValueConditionStatement;
            maxSampleValueConditionStatement = this.maxSampleValueConditionTpl.replace(
                    "%PARAMETER_NAME%",
                    PARAMETER_MAPPING.get(maxValue.getKey()));
            maxSampleValueConditionStatement = maxSampleValueConditionStatement.replace(
                    "%MAX_VALUE%",
                    String.valueOf(maxValue.getValue()));
            maxValuesBuilder.append(maxSampleValueConditionStatement);
            if (maxValuesIterator.hasNext()) {
                maxValuesBuilder.append(" \n AND ");
            }
        }

        String customSearchStatement = this.searchTpl.replace(
                "%CLASS_ID%",
                String.valueOf(classId));
        customSearchStatement = customSearchStatement.replace(
                "%OBJECT_IDS%",
                objectIdsBuilder);
        customSearchStatement = customSearchStatement.replace(
                "%MAX_SAMPLE_VALUE_CONDITIONS%",
                maxValuesBuilder);

        return customSearchStatement;
    }
}
