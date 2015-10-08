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
public class Activity {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JsonProperty("notificationperiod")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "notificationperiod"
    )
    private String notificationPeriod;

    @JacksonXmlProperty(isAttribute = true)
    private String code;

    @JacksonXmlProperty(isAttribute = true)
    private String mnemonic;

    @JsonProperty("primaryactivity")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "primaryactivity"
    )
    private String primaryActivity;
}
