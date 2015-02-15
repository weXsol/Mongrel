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
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_JS_PARAMS;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_JS_QUERY;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_QUERY;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.FloatValue;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to access the command and eval methods of the API.
 *
 * @author Dannes Wessels
 */
public class EvalCommand extends BasicFunction {

    private static final String EVAL = "eval";
    private static final String COMMAND = "command";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(EVAL, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Evaluates JavaScript "
                + "functions on the database "
                + "server. This is useful if you need to touch a lot of data lightly, "
                + "in which case network transfer could be a bottleneck",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_JS_QUERY,},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The result")
        ),
        
        new FunctionSignature(
        new QName(EVAL, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Evaluates JavaScript "
                + "functions on the database"
                + "server with the provided parameters. This is useful if you need to touch a lot of data lightly, "
                + "in which case network transfer could be a bottleneck",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_JS_QUERY, PARAMETER_JS_PARAMS},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The result")
        ),
        
        new FunctionSignature(
        new QName(COMMAND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Executes a database command.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_QUERY},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The result")
        ),
    };

    public EvalCommand(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            String mongodbClientId = args[0].itemAt(0).getStringValue();                  
            MongodbClientStore.getInstance().validate(mongodbClientId);
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);
            
            // Additional parameters
            String dbname = args[1].itemAt(0).getStringValue();
            String query = args[2].itemAt(0).getStringValue();
           
            // Get and convert 4th parameter, when existent
            Object[] params = (args.length >= 4) ? ConversionTools.convertParameters(args[3]) : new Object[0];

            // Get database
            DB db = client.getDB(dbname);
            
            Sequence retVal;

            if(isCalledAs(EVAL)){
                /* eval */

                // Execute query with additional parameter 
                Object result = db.eval(query, params);
                
                retVal = convertResult(result);

                
            } else {
                /* command */
                
                // Convert query string
                BasicDBObject mongoQuery = (BasicDBObject) JSON.parse(query);
                
                // execute query
                CommandResult result = db.command(mongoQuery);
                
                // Convert result to string
                retVal = new StringValue(result.toString());
            }

            return retVal;

        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0002, ex.getMessage());

        } catch (Throwable ex) {
            /* The library throws a lot of runtime exceptions */
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0003, ex.getMessage());
        }

    }

    private Sequence convertResult(Object result) throws XPathException {
        Sequence retVal;
        if (result instanceof BasicDBObject) {
            retVal = new StringValue(result.toString());
            
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
