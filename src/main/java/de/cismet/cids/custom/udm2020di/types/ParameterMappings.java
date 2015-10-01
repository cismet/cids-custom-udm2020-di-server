/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types;

import java.util.HashMap;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class ParameterMappings extends HashMap<String, ParameterMapping> {

    //~ Instance fields --------------------------------------------------------

    /** Stores source-level parameter mappings (aggregation of related parameters). */
    protected final HashMap<String, String> parameterMappingMappings = new HashMap<String, String>();

    //~ Methods ----------------------------------------------------------------

    @Override
    public ParameterMapping put(final String key, final ParameterMapping value) {
        if ((value != null)
                    && (value.getParameterAggregationPk() != null)
                    && !value.getParameterAggregationPk().isEmpty()
                    && (value.getParameterPk() != null)
                    && !value.getParameterPk().isEmpty()
                    && !parameterMappingMappings.containsKey(value.getParameterAggregationPk())) {
            parameterMappingMappings.put(key, value.getParameterAggregationPk());
        }

        return super.put(key, value); // To change body of generated methods, choose Tools | Templates.
    }

    /**
     * DOCUMENT ME!
     *
     * @param   key  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public ParameterMapping getAggregationMapping(final String key) {
        if (parameterMappingMappings.containsKey(key)
                    && super.containsKey(parameterMappingMappings.get(key))) {
            return super.get(this.parameterMappingMappings.get(key));
        } else {
            return super.get(key);
        }
    }
}
