/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.eprtr;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.serializers.FlexibleFloatDeserializer;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@NoArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Installation {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private long id;

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JsonProperty("erasid")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "erasid"
    )
    private long erasId;

    @JsonProperty("naceclass")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "naceclass"
    )
    private String naceClass;

    @JsonProperty("rivercatchment")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "rivercatchment"
    )
    private String riverCatchment;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float longitude;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float latitude;

    private List<Activity> activities;

    private List<Address> addresses;
}
