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
package org.exist.mongodb.xquery.mongodb.db;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.exist.dom.QName;
import org.exist.mongodb.shared.*;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to access the command and eval methods of the API.
 *
 * @author Dannes Wessels
 */
public class EvalCommand extends BasicFunction {

    public static final String PARAM_JS_QUERY = "code";
    public static final String DESCR_JS_QUERY = "Javascript code";
    public static final FunctionParameterSequenceType PARAMETER_JS_QUERY
            = new FunctionParameterSequenceType(PARAM_JS_QUERY, Type.ITEM, Cardinality.ONE, DESCR_JS_QUERY);

    public static final String PARAM_JS_PARAMS = "args";
    public static final String DESCR_JS_PARAMS = "Parameters for script";
    public static final FunctionParameterSequenceType PARAMETER_JS_PARAMS
            = new FunctionParameterSequenceType(PARAM_JS_PARAMS, Type.ITEM, Cardinality.ZERO_OR_MORE, DESCR_JS_PARAMS);

    private static final String EVAL = "eval";
    private static final String COMMAND = "command";

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(EVAL, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Evaluates JavaScript "
                    + "functions on the database server. This is useful if you need to touch a lot of data lightly, "
                    + "in which case network transfer could be a bottleneck",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_JS_QUERY,},
                    new FunctionReturnSequenceType(Type.ITEM, Cardinality.ZERO_OR_MORE, "The result of the script")
            ),

            new FunctionSignature(
                    new QName(EVAL, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Evaluates JavaScript "
                    + "functions on the database server with the provided parameters. This is useful "
                    + "if you need to touch a lot of data lightly, "
                    + "in which case network transfer could be a bottleneck",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_JS_QUERY, PARAMETER_JS_PARAMS},
                    new FunctionReturnSequenceType(Type.ITEM, Cardinality.ZERO_OR_MORE, "The result of the script")
            ),

            new FunctionSignature(
                    new QName(COMMAND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Executes a database command.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_QUERY},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The result of the command")
            ),
    };

    public EvalCommand(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            final String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            final MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Additional parameters
            final String dbname = args[1].itemAt(0).getStringValue();
            final Sequence query = args[2];

            // Get and convert 4th parameter, when existent
            final Object[] params = (args.length >= 4) ? ConversionTools.convertParameters(args[3]) : new Object[0];

            // Get database
            final DB db = client.getDB(dbname);

            Sequence retVal=new ValueSequence();

            if (isCalledAs(EVAL)) {
                /* eval */

                // Just get string
                final String queryString = query.itemAt(0).getStringValue();

                // Execute query with additional parameter 
                final Object result = db.eval(queryString, params);

                retVal = convertResult(context, result);


            } else {
                /* command */

                // Convert query string
                final BasicDBObject mongoQuery = MapToBSON.convert(query);

                // execute query
                final CommandResult result = db.command(mongoQuery);

                // Convert result to string
                if(result.ok()){
                    result.remove("ok");

                    retVal = BSONtoMap.convert(result,context);
                }

            }

            return retVal;

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }


    }

    private Sequence convertResult(final XQueryContext context, final Object result) throws XPathException {
        final Sequence retVal;
        if (result instanceof BasicDBObject) {
            retVal = BSONtoMap.convert((BasicDBObject)result, context);

        } else if (result instanceof String) {
            retVal = new StringValue((String) result);

        } else if (result instanceof Boolean) {
            retVal = BooleanValue.valueOf(((Boolean) result));

        } else if (result instanceof Float) {
            retVal = new FloatValue(((Float) result));

        } else if (result instanceof Double) {
            retVal = new DoubleValue(((Double) result));

        } else if (result instanceof Short) {
            retVal = new IntegerValue(((Short) result), Type.SHORT);

        } else if (result instanceof Integer) {
            retVal = new IntegerValue(((Integer) result), Type.INT);

        } else if (result instanceof Long) {
            retVal = new IntegerValue(((Long) result), Type.LONG);

        } else {
            // Convert result to string
            retVal = new StringValue(result.toString());
        }
        return retVal;
    }


}
