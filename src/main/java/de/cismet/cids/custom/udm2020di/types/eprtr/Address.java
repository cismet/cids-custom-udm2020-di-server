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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlRootElement;

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
public class Address {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String type;

    @JacksonXmlProperty(isAttribute = true)
    private String region;

    @JacksonXmlProperty(isAttribute = true)
    private String district;

    @JacksonXmlProperty(isAttribute = true)
    private String city;

    @JacksonXmlProperty(isAttribute = true)
    private int postcode;

    @JsonProperty("streetname")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "streetname"
    )
    private String streetName;
}
