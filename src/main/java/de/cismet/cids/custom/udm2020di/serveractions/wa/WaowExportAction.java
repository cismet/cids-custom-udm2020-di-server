/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.wa;

import org.apache.log4j.Logger;

import java.io.IOException;

import java.sql.SQLException;

import de.cismet.cids.server.actions.ServerAction;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
//@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class WaowExportAction extends WaExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "waowExportAction";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new WaowExportAction object.
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public WaowExportAction() throws IOException, ClassNotFoundException, SQLException {
        super(WAOW);
        this.log = Logger.getLogger(WaowExportAction.class);
        log.info("new WaowExportAction created");
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }
}
