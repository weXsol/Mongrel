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
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_KEYS;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_QUERY;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;
import org.exist.xquery.value.ValueSequence;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Find extends BasicFunction {

    private static final String FIND = "find";
    
  
    public final static FunctionSignature signatures[] = {
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for an object in the collection",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The object formatted as JSON")
        ),
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for an object in the collection",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The object formatted as JSON")
        ),
        
        new FunctionSignature(
        new QName(FIND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Query for an object in the collection, return specified fields",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_KEYS},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The object formatted as JSON")
        ),
    };

    public Find(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();
            
            String query = (args.length >= 4) ? args[3].itemAt(0).getStringValue() : null;

            String keys = (args.length >= 5) ? args[4].itemAt(0).getStringValue() : null;

            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);
            
            // PLace holder result cursor
            DBCursor cursor;
            
            if (query == null) {
                // No query
                cursor = dbcol.find();

            } else {
                // Parse query
                BasicDBObject mongoQuery = (BasicDBObject) JSON.parse(query);

                // Parse keys when available
                BasicDBObject mongoKeys = (keys == null) ? null : (BasicDBObject) JSON.parse(keys);

                // Call correct method
                cursor = (mongoKeys == null) ? dbcol.find(mongoQuery) : dbcol.find(mongoQuery, mongoKeys);
            }


            
            // Execute query
            Sequence retVal = new ValueSequence();

            // Harvest results
            try {
                while (cursor.hasNext()) {
                    retVal.add(new StringValue(cursor.next().toString()));
                }
            } finally {
                cursor.close();
            }

            return retVal;
            
        } catch(JSONParseException ex){
            LOG.error(ex.getMessage());
            throw new XPathException(this, MongodbModule.MONG0004, ex.getMessage());

        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0002, ex.getMessage());

        } catch (Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0003, ex.getMessage());
        }

    }

}
