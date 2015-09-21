/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.boris;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
public class Probenparameter {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(
        isAttribute = true,
        localName = "parameterpk"
    )
    @JsonProperty("parameterpk")
    private String parameterPk;
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "parametergruppepk"
    )
    @JsonProperty("parametergruppepk")
    private String parametergruppePk;
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "parametername"
    )
    @JsonProperty("parametername")
    private String parameterName;
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "parametergruppename"
    )
    @JsonProperty("parametergruppename")
    private String parametergruppeName;

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getParameterPk() {
        return parameterPk;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  parameterPk  DOCUMENT ME!
     */
    public void setParameterPk(final String parameterPk) {
        this.parameterPk = parameterPk;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getParametergruppePk() {
        return parametergruppePk;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  parametergruppePk  DOCUMENT ME!
     */
    public void setParametergruppePk(final String parametergruppePk) {
        this.parametergruppePk = parametergruppePk;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getParameterName() {
        return parameterName;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  parameterName  DOCUMENT ME!
     */
    public void setParameterName(final String parameterName) {
        this.parameterName = parameterName;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getParametergruppeName() {
        return parametergruppeName;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  parametergruppeName  DOCUMENT ME!
     */
    public void setParametergruppeName(final String parametergruppeName) {
        this.parametergruppeName = parametergruppeName;
    }
}
