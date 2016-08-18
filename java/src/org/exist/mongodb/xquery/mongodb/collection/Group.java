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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.MongoClient;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Group extends BasicFunction {

    private static final String GROUP = "group";
       
    public static final FunctionParameterSequenceType PARAMETER_KEY
            = new FunctionParameterSequenceType("key", Type.ITEM, Cardinality.ONE, "Specifies one or more document fields to group. JSON formatted.");

    public static final FunctionParameterSequenceType PARAMETER_COND
            = new FunctionParameterSequenceType("cond", Type.ITEM, Cardinality.ONE, "Specifies the selection criteria to determine which documents in the collection to process. JSON formatted.");

    public static final FunctionParameterSequenceType PARAMETER_INITIAL
            = new FunctionParameterSequenceType("initial", Type.ITEM, Cardinality.ONE, "Initializes the aggregation result document. JSON formatted.");

    public static final FunctionParameterSequenceType PARAMETER_REDUCE
            = new FunctionParameterSequenceType("reduce", Type.STRING, Cardinality.ONE, "Specifies an $reduce Javascript function, that operates on the documents during the grouping operation.");

    public static final FunctionParameterSequenceType PARAMETER_FINALIZE
            = new FunctionParameterSequenceType("finalize", Type.STRING, Cardinality.ONE, "Specifies a Javascript function that runs each item in the result set before final value will be returned.");
    
    private static final String DESCRIPTION = "Group documents in a collection by the specified key and performs "
            + "simple aggregation functions such as computing counts and sums. This is analogous to a SELECT ... GROUP BY statement in SQL.";
    
    private static final String RESULT = "A document with the grouped records as well as the command meta-data formatted as JSON";

    public final static FunctionSignature signatures[] = {
        
        new FunctionSignature(
        new QName(GROUP, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), DESCRIPTION,
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_KEY, PARAMETER_COND, PARAMETER_INITIAL, PARAMETER_REDUCE},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, RESULT)
        ),
        
        new FunctionSignature(
        new QName(GROUP, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), DESCRIPTION,
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_KEY, PARAMETER_COND, PARAMETER_INITIAL, PARAMETER_REDUCE, PARAMETER_FINALIZE},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, RESULT)
        )
       
        
    };

    public Group(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            String mongodbClientId = args[0].itemAt(0).getStringValue();                  
            MongodbClientStore.getInstance().validate(mongodbClientId);
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);
            
            // Get parameters
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();

            BasicDBObject key = ConversionTools.convertJSonParameter(args[3]);

            BasicDBObject condition = ConversionTools.convertJSonParameter(args[4]);

            BasicDBObject initial = ConversionTools.convertJSonParameter(args[5]);

            String reduce = args[6].itemAt(0).getStringValue();

            // The finalize can be null
            String finalize = (args.length >= 8)
                    ? args[7].itemAt(0).getStringValue()
                    : null;


            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);
            
            // Propare groupcommand
            GroupCommand command = new GroupCommand(dbcol, key, condition, initial, reduce, finalize);
                  
            if(LOG.isDebugEnabled()){
                LOG.debug(command.toDBObject().toString());
            }
            
            // Execute, finalize can have value null
            DBObject result = dbcol.group(command);
            
            // Execute query

            return new StringValue(result.toString());
            
        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        } 

    }

}
