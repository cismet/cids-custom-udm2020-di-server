/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cismet.cids.custom.udm2020di.serializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class FlexibleDoubleDeserializer extends JsonDeserializer<Double> {

    //~ Methods ----------------------------------------------------------------

    @Override
    public Double deserialize(final JsonParser parser, final DeserializationContext context) throws IOException,
        JsonProcessingException {
        final String valueAsString = parser.getValueAsString();
        if ((valueAsString == null) || valueAsString.isEmpty()) {
            return null;
        }

        return Double.parseDouble(valueAsString.replaceAll(",", "\\."));
    }
}
