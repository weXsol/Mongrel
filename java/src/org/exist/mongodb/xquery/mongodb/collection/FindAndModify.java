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
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.util.HashMap;
import java.util.Map;
import org.exist.dom.QName;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_FIELDS;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_ORDERBY;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_QUERY;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_SORT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_UPDATE;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to modify documents in mongodb
 *
 * @author Dannes Wessels
 */
public class FindAndModify extends BasicFunction {

    private static final String FIND_AND_MODIFY = "findAndModify";
    
  
    public final static FunctionSignature signatures[] = {
        
        new FunctionSignature(
        new QName(FIND_AND_MODIFY, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Atomically modify and return a single document.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_UPDATE},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The document as it was before the modifications, formatted as JSON")
        ),
        
        new FunctionSignature(
        new QName(FIND_AND_MODIFY, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Atomically modify and return a single document.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY,PARAMETER_UPDATE, PARAMETER_SORT},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The document as it was before the modifications, formatted as JSON")
        ),
        
    };

    public FindAndModify(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();
            
            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);
            
            BasicDBObject query = (args.length >= 4)
                    ? (BasicDBObject) JSON.parse(args[3].itemAt(0).getStringValue())
                    : null;

            BasicDBObject update = (args.length >= 5)
                    ? (BasicDBObject) JSON.parse(args[4].itemAt(0).getStringValue())
                    : null;

            BasicDBObject sort = (args.length >= 6)
                    ? (BasicDBObject) JSON.parse(args[5].itemAt(0).getStringValue())
                    : null;

             Map<String,Boolean> options = (args.length >= 7)
                    ? convertOptions((AbstractMapType) args[2].itemAt(0))
                    : null;
             
            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);
            
            //query update sort
                    
            DBObject result;
            if (sort == null /* && options==null */) {
                result = dbcol.findAndModify(query, update);
                
            } else  /* if (options==null) */ {
                result = dbcol.findAndModify(query, sort, update);
            }
            
//            else {
//               options.putIfAbsent("remove", unordered);
//               options.putIfAbsent("returnNew", unordered);
//               options.putIfAbsent("update", unordered);
//               options.putIfAbsent("upsert", unordered);
//                
//               dbcol.findAndModify(query, sort, update, unordered, result, unordered, unordered);
//            }
            
           
            // Execute query
            Sequence retVal = (result==null) 
                    ? Sequence.EMPTY_SEQUENCE 
                    : new StringValue(result.toString());

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
    
    public Map<String,Boolean> convertOptions(AbstractMapType map) throws XPathException {
        
        Map<String,Boolean> retVal = new HashMap<>();
        
        // Get all keys
        Sequence keys = map.keys();
        
        // Iterate over all keys
        for (final SequenceIterator i = keys.unorderedIterator(); i.hasNext();) {

            // Get next item
            Item key = i.nextItem();
            
            // Only use Strings as key, as required by JMS
            String keyValue = key.getStringValue();
            
            // Get values
            Sequence values = map.get((AtomicValue)key);
            
            retVal.put(keyValue, Boolean.valueOf(values.getStringValue()));
            
        }
        
        return retVal;
    }

}
