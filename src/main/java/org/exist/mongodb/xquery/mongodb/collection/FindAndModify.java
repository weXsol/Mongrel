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
import org.exist.mongodb.shared.*;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to modify documents in mongodb
 *
 * @author Dannes Wessels
 */
public class FindAndModify extends BasicFunction {

    public static final String PARAM_UPDATE = "update";
    public static final String DESCR_UPDATE = "The modifications to apply, JSON formatted";
    public static final FunctionParameterSequenceType PARAMETER_UPDATE
            = new FunctionParameterSequenceType(PARAM_UPDATE, Type.ITEM, Cardinality.ONE, DESCR_UPDATE);
    public static final String PARAM_SORT = "sort";
    public static final String DESCR_SORT = "Determines which document the operation will modify if the query selects multiple documents, JSON formatted";
    public static final FunctionParameterSequenceType PARAMETER_SORT
            = new FunctionParameterSequenceType(PARAM_SORT, Type.ITEM, Cardinality.ONE, DESCR_SORT);
    private static final String FIND_AND_MODIFY = "findAndModify";
    public final static FunctionSignature[] signatures = {

            new FunctionSignature(
                    new QName(FIND_AND_MODIFY, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Atomically modify and return a single document.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_UPDATE},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The document as it was before the modifications, formatted as JSON")
            ),

            new FunctionSignature(
                    new QName(FIND_AND_MODIFY, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Atomically modify and return a single document.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_UPDATE, PARAMETER_SORT},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The document as it was before the modifications, formatted as JSON")
            ),

    };

    public FindAndModify(final XQueryContext context, final FunctionSignature signature) {
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

            final BasicDBObject query = (args.length >= 4)
                    ? MapToBSON.convert(args[3])
                    : null;

            final BasicDBObject update = (args.length >= 5)
                    ? MapToBSON.convert(args[4])
                    : null;

            final BasicDBObject sort = (args.length >= 6)
                    ? MapToBSON.convert(args[5])
                    : null;

//             Map<String,Boolean> options = (args.length >= 7)
//                    ? convertOptions((AbstractMapType) args[6].itemAt(0))
//                    : null;

            // Get database
            final DB db = client.getDB(dbname);
            final DBCollection dbcol = db.getCollection(collection);

            //query update sort

            final DBObject result;
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

            return (result == null)
                    ? Sequence.EMPTY_SEQUENCE
                    : BSONtoMap.convert(result, context);

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }

//    public Map<String, Boolean> convertOptions(AbstractMapType map) throws XPathException {
//
//        Map<String, Boolean> retVal = new HashMap<>();
//
//        // Get all keys
//        Sequence keys = map.keys();
//
//        // Iterate over all keys
//        for (final SequenceIterator i = keys.unorderedIterator(); i.hasNext(); ) {
//
//            // Get next item
//            Item key = i.nextItem();
//
//            // Only use Strings as key, as required by JMS
//            String keyValue = key.getStringValue();
//
//            // Get values
//            Sequence values = map.get((AtomicValue) key);
//
//            retVal.put(keyValue, Boolean.valueOf(values.getStringValue()));
//
//        }
//
//        return retVal;
//    }

}
