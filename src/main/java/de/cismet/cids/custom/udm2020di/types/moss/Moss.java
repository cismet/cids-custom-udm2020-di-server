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

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.Tag;

import de.cismet.cids.dynamics.CidsBean;

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

    //~ Static fields/initializers ---------------------------------------------

    protected static final Date MIN_DATE = new GregorianCalendar(2010, 01, 01).getTime();
    protected static final Date MAX_DATE = new GregorianCalendar(2010, 12, 31).getTime();

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

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new Moss object.
     *
     * @param  cidsBean  DOCUMENT ME!
     */
    public Moss(final CidsBean cidsBean) {
        this.id = cidsBean.getPrimaryKeyValue();
        this.type = cidsBean.getProperty("moss_type.type").toString();
        this.sampleId = cidsBean.getProperty("sample_id").toString();
        this.labNo = cidsBean.getProperty("lab_no").toString();

        this.probenparameter.add(new Parameter(
                "AL_CONV",
                "MET",
                "Al [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "AS_CONV",
                "MET",
                "As [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "CD_CONV",
                "MET",
                "Cd [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "CO_CONV",
                "MET",
                "Co konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "CR_CONV",
                "MET",
                "Cr konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "CU_CONV",
                "MET",
                "Cu konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "FE_CONV",
                "MET",
                "Fe konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "MO_CONV",
                "MET",
                "Mo konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "NI_CONV",
                "MET",
                "Ni konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "PB_CONV",
                "MET",
                "Pb konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "Pb",
                "MET",
                "Pb konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "S_CONV",
                "DNM",
                "S konv. [mg/kg]",
                "Düngemittel",
                false));
        this.probenparameter.add(new Parameter(
                "V_CONV",
                "MET",
                "V konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "SB_CONV",
                "MET",
                "Sb konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "ZN_CONV",
                "MET",
                "Zn konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "HG_CONV",
                "MET",
                "Hg konv. [mg/kg]",
                "Metalle",
                false));
        this.probenparameter.add(new Parameter(
                "N_TOTAL",
                "DNM",
                "N ges. [mg/kg]",
                "Düngemittel",
                false));

        this.aggregationValues.add(new AggregationValue(
                "Al",
                "mg/kg",
                this.sampleId,
                null,
                "Al",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("al_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("al_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "As",
                "mg/kg",
                this.sampleId,
                null,
                "As",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("as_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("as_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "As",
                "mg/kg",
                this.sampleId,
                null,
                "As",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("as_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("as_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Cd",
                "mg/kg",
                this.sampleId,
                null,
                "Cd",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("cd_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("cd_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Co",
                "mg/kg",
                this.sampleId,
                null,
                "Co",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("co_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("co_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Cr",
                "mg/kg",
                this.sampleId,
                null,
                "Cr",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("cr_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("cr_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Cu",
                "mg/kg",
                this.sampleId,
                null,
                "Cu",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("cu_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("cu_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Fe",
                "mg/kg",
                this.sampleId,
                null,
                "Fe",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("fe_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("fe_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Mo",
                "mg/kg",
                this.sampleId,
                null,
                "Mo",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("mo_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("mo_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Ni",
                "mg/kg",
                this.sampleId,
                null,
                "Ni",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("ni_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("ni_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Pb",
                "mg/kg",
                this.sampleId,
                null,
                "Pb",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("pb_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("pb_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Pb",
                "mg/kg",
                this.sampleId,
                null,
                "Pb",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("pb_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("pb_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "S",
                "mg/kg",
                this.sampleId,
                null,
                "S",
                "DNM",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("s_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("s_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "V",
                "mg/kg",
                this.sampleId,
                null,
                "V",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("v_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("v_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Sb",
                "mg/kg",
                this.sampleId,
                null,
                "Sb",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("sb_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("sb_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Zn",
                "mg/kg",
                this.sampleId,
                null,
                "Zn",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("zn_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("zn_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "Hg",
                "mg/kg",
                this.sampleId,
                null,
                "Hg",
                "MET",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("hg_conv")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("hg_conv")).floatValue()));
        this.aggregationValues.add(new AggregationValue(
                "N ges.",
                "mg/kg",
                this.sampleId,
                null,
                "N",
                "DNM",
                MIN_DATE,
                MAX_DATE,
                ((BigDecimal)cidsBean.getProperty("n_total")).floatValue(),
                ((BigDecimal)cidsBean.getProperty("n_total")).floatValue()));
    }

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
