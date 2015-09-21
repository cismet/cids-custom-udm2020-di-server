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
package de.cismet.cids.custom.udm2020di.tools;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.module.SimpleKeyDeserializers;

import java.io.IOException;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class CaseInsensitiveKeyDeserializers extends SimpleKeyDeserializers {

    //~ Static fields/initializers ---------------------------------------------

    private static final CaseInsensitiveKeyDeserializer DESERIALIZER = new CaseInsensitiveKeyDeserializer();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CaseInsensitiveKeyDeserializers object.
     */
    public CaseInsensitiveKeyDeserializers() {
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public KeyDeserializer findKeyDeserializer(final JavaType type,
            final DeserializationConfig config,
            final BeanDescription beanDesc) {
        return DESERIALIZER;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    private static class CaseInsensitiveKeyDeserializer extends KeyDeserializer {

        //~ Methods ------------------------------------------------------------

        @Override
        public Object deserializeKey(final String key, final DeserializationContext ctxt) throws IOException {
            return key.toLowerCase();
        }
    }
}
