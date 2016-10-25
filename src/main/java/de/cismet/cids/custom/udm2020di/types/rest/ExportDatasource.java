/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import java.util.Collection;

import de.cismet.cids.custom.udm2020di.types.Parameter;

/**
 * REST Export Action Configuration.
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
class ExportDatasource {

    //~ Instance fields --------------------------------------------------------

    @Getter @Setter @NonNull
    @JsonProperty(required = true)
    private String name;

    @Getter @Setter
    @JsonProperty(required = false)
    private String filename;

    @Getter @Setter
    @JsonProperty(required = true)
    private boolean global;

    @Getter @Setter @NonNull
    @JsonProperty(required = true)
    private Collection<Parameter> parameters;
}
