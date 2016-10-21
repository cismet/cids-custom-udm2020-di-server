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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import java.util.Collection;

/**
 * REST Export Options Configuration.
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class ExportOptions {

    //~ Instance fields --------------------------------------------------------

    @Getter @Setter
    @JsonProperty(
        value = "isMergeExternalDatasource",
        required = true
    )
    @JacksonXmlProperty(localName = "isMergeExternalDatasource")
    private boolean mergeExternalDatasource;

    @Getter @Setter @NonNull
    @JsonProperty(required = true)
    private String exportFormat;

    @JsonProperty(
        value = "selectedExportThemes",
        required = true
    )
    @JacksonXmlProperty(localName = "selectedExportThemes")
    @Getter @Setter @NonNull private Collection<ExportTheme> exportThemes;
}
