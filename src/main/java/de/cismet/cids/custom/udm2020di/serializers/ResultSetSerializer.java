/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.log4j.Logger;

import java.io.IOException;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Based on http://stackoverflow.com/a/8120442 by http://stackoverflow.com/users/818764/plap.
 *
 * @author   PLAP
 * @version  $Revision$, $Date$
 */
public class ResultSetSerializer extends JsonSerializer<ResultSet> {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger log = Logger.getLogger(ResultSetSerializer.class);

    //~ Methods ----------------------------------------------------------------

    @Override
    public Class<ResultSet> handledType() {
        return ResultSet.class;
    }

    @Override
    public void serialize(final ResultSet rs, final JsonGenerator jgen, final SerializerProvider provider)
            throws IOException, JsonProcessingException {
        try {
            final ResultSetMetaData rsmd = rs.getMetaData();
            final int numColumns = rsmd.getColumnCount();
            int numRows = 0;
            final String[] columnNames = new String[numColumns];
            final int[] columnTypes = new int[numColumns];
            if (log.isDebugEnabled()) {
                log.debug("serializing result set with " + numColumns + " columns to JSON");
            }

            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = rsmd.getColumnLabel(i + 1);
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }

            jgen.writeStartArray();

            while (rs.next()) {
                boolean b;
                long l;
                double d;

                jgen.writeStartObject();

                for (int i = 0; i < columnNames.length; i++) {
                    jgen.writeFieldName(columnNames[i].toLowerCase());
                    switch (columnTypes[i]) {
                        case Types.INTEGER: {
                            l = rs.getInt(i + 1);
                            if (rs.wasNull()) {
                                jgen.writeNull();
                            } else {
                                jgen.writeNumber(l);
                            }
                            break;
                        }

                        case Types.BIGINT: {
                            l = rs.getLong(i + 1);
                            if (rs.wasNull()) {
                                jgen.writeNull();
                            } else {
                                jgen.writeNumber(l);
                            }
                            break;
                        }

                        case Types.DECIMAL:
                        case Types.NUMERIC: {
                            jgen.writeNumber(rs.getBigDecimal(i + 1));
                            break;
                        }

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE: {
                            d = rs.getDouble(i + 1);
                            if (rs.wasNull()) {
                                jgen.writeNull();
                            } else {
                                jgen.writeNumber(d);
                            }
                            break;
                        }

                        case Types.NVARCHAR:
                        case Types.VARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.CLOB: {
                            jgen.writeString(rs.getString(i + 1));
                            break;
                        }

                        case Types.BOOLEAN:
                        case Types.BIT: {
                            b = rs.getBoolean(i + 1);
                            if (rs.wasNull()) {
                                jgen.writeNull();
                            } else {
                                jgen.writeBoolean(b);
                            }
                            break;
                        }

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY: {
                            jgen.writeBinary(rs.getBytes(i + 1));
                            break;
                        }

                        case Types.TINYINT:
                        case Types.SMALLINT: {
                            l = rs.getShort(i + 1);
                            if (rs.wasNull()) {
                                jgen.writeNull();
                            } else {
                                jgen.writeNumber(l);
                            }
                            break;
                        }

                        case Types.DATE: {
                            provider.defaultSerializeDateValue(rs.getDate(i + 1), jgen);
                            break;
                        }

                        case Types.TIMESTAMP: {
                            provider.defaultSerializeDateValue(rs.getTime(i + 1), jgen);
                            break;
                        }

                        case Types.BLOB: {
                            final Blob blob = rs.getBlob(i);
                            provider.defaultSerializeValue(blob.getBinaryStream(), jgen);
                            blob.free();
                            break;
                        }

//                        case Types.CLOB: {
//                            final Clob clob = rs.getClob(i);
//                            provider.defaultSerializeValue(clob.getCharacterStream(), jgen);
//                            clob.free();
//                            break;
//                        }

                        case Types.ARRAY: {
                            log.warn("ResultSetSerializer not yet implemented for SQL type ARRAY");
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type ARRAY");
                        }

                        case Types.STRUCT: {
                            log.warn("ResultSetSerializer not yet implemented for SQL type STRUCT");
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type STRUCT");
                        }

                        case Types.DISTINCT: {
                            log.warn("ResultSetSerializer not yet implemented for SQL type DISTINCT");
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type DISTINCT");
                        }

                        case Types.REF: {
                            log.warn("ResultSetSerializer not yet implemented for SQL type REF");
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type REF");
                        }

                        case Types.JAVA_OBJECT:
                        default: {
                            log.warn("Using default serializer for SQL TYPE " + columnTypes[i]);
                            provider.defaultSerializeValue(rs.getObject(i + 1), jgen);
                            break;
                        }
                    }
                }

                jgen.writeEndObject();
                numRows++;
            }

            jgen.writeEndArray();
            if (log.isDebugEnabled()) {
                log.debug("result set with " + numColumns + " columns and "
                            + numRows + " rows successfully serialized to JSON");
            }
        } catch (SQLException e) {
            throw new ResultSetSerializerException(e);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    public static class ResultSetSerializerException extends JsonProcessingException {

        //~ Static fields/initializers -----------------------------------------

        private static final long serialVersionUID = -914957626413580734L;

        //~ Constructors -------------------------------------------------------

        /**
         * Creates a new ResultSetSerializerException object.
         *
         * @param  cause  DOCUMENT ME!
         */
        public ResultSetSerializerException(final Throwable cause) {
            super(cause);
        }
    }
}
