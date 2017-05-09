/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.wa;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.sql.SQLException;

import java.util.Arrays;
import java.util.Collection;

import de.cismet.cids.custom.udm2020di.types.Parameter;

import de.cismet.cids.server.actions.ServerAction;
import de.cismet.cids.server.actions.ServerActionParameter;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class WagwExportAction extends WaExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "wagwExportAction";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new WagwExportAction object.
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public WagwExportAction() throws IOException, ClassNotFoundException, SQLException {
        super(WAGW);
        this.log = Logger.getLogger(WagwExportAction.class);
        log.info("new WagwExportAction created");
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        try {
            final Collection<String> messstellePks = Arrays.asList(
                    new String[] { "PG10000122" });

            final Collection<Parameter> parameter = Arrays.asList(
                    new Parameter[] {
                        new Parameter("G146", "PPPP µg/l"),
                        new Parameter("G145", "Was.WEIissisch"),
                        new Parameter("G142", "Schadstof. X"),
                        new Parameter("G142", "G142  X x"),
                        new Parameter("F833", "ZINK GES. µg/l")
                    });

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<Collection<String>>(PARAM_MESSSTELLEN, messstellePks),
                    new ServerActionParameter<Collection<Parameter>>(PARAM_PARAMETER, parameter),
                    new ServerActionParameter<String>(PARAM_EXPORTFORMAT, PARAM_EXPORTFORMAT_SHP),
                    new ServerActionParameter<String>(PARAM_NAME, "wagw-shape-export"),
                    new ServerActionParameter<Boolean>(PARAM_INTERNAL, true),
                };

            BasicConfigurator.configure();
            final WagwExportAction exportAction = new WagwExportAction();

            final Object result = exportAction.execute(null, serverActionParameters);
            // final Path file = Files.write(Paths.get("wagw-export.csv"), result.toString().getBytes("UTF-8"));
            // final Path file = Files.write(Paths.get("wagw-export.XLSX"), (byte[])result);
            final Path file = Files.write(Paths.get("wagw-export.zip"), (byte[])result);
            System.out.println("Export File written to "
                        + file.toAbsolutePath().toString());
        } catch (Throwable ex) {
            Logger.getLogger(WaExportAction.class).fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
