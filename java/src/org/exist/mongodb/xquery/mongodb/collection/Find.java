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
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Find extends BasicFunction {

    private static final String FIND = "find";
    
  
    public final static FunctionSignature signatures[] = {
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for all documents in the collection",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "The all document(s) in collection")
        ),
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for documents in the collection",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "The selected document(s)")
        ),
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for documents in the collection and get specified fields",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_KEYS},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "The selected document(s)")
        ),
    };

    public Find(XQueryContext context, FunctionSignature signature) {
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

            BasicDBObject mongoQuery = (args.length >= 4)
                    ? ConversionTools.convertJSonParameter(args[3])
                    : new BasicDBObject();
 
            BasicDBObject mongoKeys = (args.length >= 5)
                    ? ConversionTools.convertJSonParameter(args[4])
                    : null;
 
            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            Sequence retVal = new ValueSequence();

            // Execute querys
            try (
                    DBCursor cursor = (mongoKeys == null)
                            ? dbcol.find(mongoQuery)
                            : dbcol.find(mongoQuery, mongoKeys);) {
                // Harvest result
                while (cursor.hasNext()) {
                    DBObject getValues = cursor.next();
                    retVal.addAll(ConversionTools.getValues(context, getValues));
                }
            }

            return retVal;
            
        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        } 

    }

}
