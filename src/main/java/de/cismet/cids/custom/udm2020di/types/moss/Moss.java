/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.moss;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.Tag;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Moss {

    //~ Instance fields --------------------------------------------------------

    @JsonProperty("id")
    @JacksonXmlProperty(isAttribute = true)
    private long id = -1;

    @JsonProperty("type")
    @JacksonXmlProperty(isAttribute = true)
    private String type = null;

    @JsonProperty("sampleid")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "sampleid"
    )
    private String sampleId = null;

    @JsonProperty("labno")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "labno"
    )
    private String labNo = null;

    @JacksonXmlProperty(localName = "aggregationvalues")
    @JsonProperty("aggregationvalues")
    @Setter(AccessLevel.NONE)
    private List<AggregationValue> aggregationValues = new ArrayList<AggregationValue>();

    @JacksonXmlProperty(localName = "probenparameter")
    @JsonProperty("probenparameter")
    @Setter(AccessLevel.NONE)
    private List<Parameter> probenparameter = new ArrayList<Parameter>();

    @JacksonXmlProperty
    @Setter(AccessLevel.NONE)
    private List<Tag> tags = new ArrayList<Tag>();

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param  tags  DOCUMENT ME!
     */
    public void setTags(final List<Tag> tags) {
        this.tags = tags;
        if ((this.tags != null) && !this.tags.isEmpty()) {
            Collections.sort(this.tags);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param  aggregationValues  DOCUMENT ME!
     */
    public void setAggregationValues(final List<AggregationValue> aggregationValues) {
        this.aggregationValues = aggregationValues;
        if ((this.aggregationValues != null) && !this.aggregationValues.isEmpty()) {
            Collections.sort(this.aggregationValues);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param  probenparameter  DOCUMENT ME!
     */
    public void setProbenparameter(final List<Parameter> probenparameter) {
        this.probenparameter = probenparameter;
        if ((this.probenparameter != null) && !this.probenparameter.isEmpty()) {
            Collections.sort(this.probenparameter);
        }
    }
}
