/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.mongodb.xquery.mongodb.collection;

import com.mongodb.*;
import com.mongodb.MapReduceCommand.OutputType;
import org.exist.dom.QName;
import org.exist.mongodb.shared.*;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.util.Locale;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Function to execute map-reduce on Mongo
 *
 * @author Dannes Wessels
 */
public class MapReduce extends BasicFunction {

    public static final String PARAM_MAP = "map";
    public static final String DESCR_MAP = "A JavaScript function that associates or \"maps\" a value with a key and emits the key and value pair.";
    public static final FunctionParameterSequenceType PARAMETER_MAP
            = new FunctionParameterSequenceType(PARAM_MAP, Type.STRING, Cardinality.ONE, DESCR_MAP);

    public static final String PARAM_REDUCE = "reduce";
    public static final String DESCR_REDUCE = "A JavaScript function that \"reduces\" to a single object all the values associated with a particular key.";
    public static final FunctionParameterSequenceType PARAMETER_REDUCE
            = new FunctionParameterSequenceType(PARAM_REDUCE, Type.STRING, Cardinality.ONE, DESCR_REDUCE);

    public static final String PARAM_OUTPUT_TARGET = "output-target";
    public static final String DESCR_OUTPUT_TARGET = "Specifies the location of the result of the map-reduce operation (optional), empty-sequence if want to use temp collection";
    public static final FunctionParameterSequenceType PARAMETER_OUTPUT_TARGET
            = new FunctionParameterSequenceType(PARAM_OUTPUT_TARGET, Type.STRING, Cardinality.ZERO_OR_ONE, DESCR_OUTPUT_TARGET);

    public static final String PARAM_OUTPUT_TYPE = "output-type";
    public static final String DESCR_OUTPUT_TYPE = "Specifies the option for outputting the results of a map-reduce operation. Available values: "
            + "replace,merge,reduce,inline. Empty sequence defaults to inline.";
    public static final FunctionParameterSequenceType PARAMETER_OUTPUT_TYPE
            = new FunctionParameterSequenceType(PARAM_OUTPUT_TYPE, Type.STRING, Cardinality.ZERO_OR_ONE, DESCR_OUTPUT_TYPE);

    public static final String PARAM_MR_QUERY = "query";
    public static final String DESCR_MR_QUERY = "Specifies the selection criteria using query operators for determining the documents input to the map function.";
    public static final FunctionParameterSequenceType PARAMETER_MR_QUERY
            = new FunctionParameterSequenceType(PARAM_MR_QUERY, Type.STRING, Cardinality.ZERO_OR_ONE, DESCR_MR_QUERY);

    private static final String MAP_REDUCE = "map-reduce";
    public final static FunctionSignature[] signatures = {

            new FunctionSignature(
                    new QName(MAP_REDUCE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Allows you to run map-reduce aggregation operations "
                    + "over a collection. Runs the command in REPLACE output mode (saves to named collection)..",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_MAP, PARAMETER_REDUCE, PARAMETER_OUTPUT_TARGET, PARAMETER_OUTPUT_TYPE, PARAMETER_MR_QUERY},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The results of this map reduce operation formatted as JSON")
            ),

    };

    public MapReduce(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            final String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            final MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get parameters
            final String dbname = args[1].itemAt(0).getStringValue();
            final String collection = args[2].itemAt(0).getStringValue();

            final String map = args[3].itemAt(0).getStringValue();
            final String reduce = args[4].itemAt(0).getStringValue();

            // output-target can have value null
            final String outputTarget = args[5].isEmpty()
                    ? null
                    : args[5].itemAt(0).getStringValue();

            final OutputType outputType = args[6].isEmpty()
                    ? OutputType.INLINE
                    : OutputType.valueOf(args[6].itemAt(0).getStringValue().toUpperCase(Locale.US));


            final DBObject query = MapToBSON.convert(args[7]);

            // Get collection in database
            final DB db = client.getDB(dbname);
            final DBCollection dbcol = db.getCollection(collection);

            // Execute query      
            final MapReduceOutput output = dbcol.mapReduce(map, reduce, outputTarget, outputType, query);

            // Parse results
            final Sequence retVal = new ValueSequence();

            for (final DBObject result : output.results()) {
                retVal.addAll(BSONtoMap.convert(result,context));
            }

            return retVal;

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }


}
