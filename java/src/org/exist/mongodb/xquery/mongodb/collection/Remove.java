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
import org.exist.mongodb.shared.MapToBSON;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Function to Remove document from mongodb
 *
 * @author Dannes Wessels
 */
public class Remove extends BasicFunction {

    public static final String PARAM_DELETE_CRITERIUM = "criterium";
    public static final String DESCR_DELETE_CRITERIUM = "The deletion criteria using query operators. Omit "
            + "the query parameter or pass an empty document to delete all documents in the collection. JSON formatted";
    public static final FunctionParameterSequenceType PARAMETER_DELETE_CRITERIUM
            = new FunctionParameterSequenceType(PARAM_DELETE_CRITERIUM, Type.ITEM, Cardinality.ONE, DESCR_DELETE_CRITERIUM);
    private static final String REMOVE = "remove";
    public final static FunctionSignature signatures[] = {

            new FunctionSignature(
                    new QName(REMOVE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Remove documents from a collection.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_DELETE_CRITERIUM},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The remove result")
            ),

    };

    public Remove(XQueryContext context, FunctionSignature signature) {
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

            BasicDBObject query = (args.length >= 4)
                    ? (BasicDBObject) MapToBSON.convert(args[3])
                    : null;

            // Get collection in database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            // Execute query      
            WriteResult result = dbcol.remove(query);

            // Wrap results into map
            final MapType map = new MapType(context);
            map.add(new StringValue("acknowledged"), new ValueSequence(new BooleanValue(result.wasAcknowledged())));

            if (result.wasAcknowledged()) {
                map.add(new StringValue("n"), new ValueSequence(new IntegerValue(result.getN())));
                map.add(new StringValue("updateOfExisting"), new ValueSequence(new BooleanValue(result.isUpdateOfExisting())));
                map.add(new StringValue("upsertedId"), new ValueSequence(new StringValue((String) result.getUpsertedId())));
            }

            return map;

        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }


}
