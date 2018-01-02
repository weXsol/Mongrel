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
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class FindOne extends BasicFunction {

    private static final String FIND_ONE = "findOne";

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName(FIND_ONE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Retrieve a single object from this collection.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The object formatted as JSON")
            ),
            new FunctionSignature(
                    new QName(FIND_ONE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Retrieve a single object from this collection matching the query.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The object formatted as JSON")
            ),
            new FunctionSignature(
                    new QName(FIND_ONE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Retrieve a single object from this collection matching the query.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_FIELDS},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The object formatted as JSON")
            ),
            new FunctionSignature(
                    new QName(FIND_ONE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Returns a single object from this collection matching the query.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY, PARAMETER_FIELDS, PARAMETER_ORDERBY},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The object formatted as JSON")
            ),};

    public FindOne(XQueryContext context, FunctionSignature signature) {
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
                    ? MapToBSON.convert(args[3])
                    : null;

            BasicDBObject fields = (args.length >= 5)
                    ? MapToBSON.convert(args[4])
                    : null;

            BasicDBObject orderBy = (args.length >= 6)
                    ? MapToBSON.convert(args[5])
                    : null;

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            DBObject result;
            if (fields == null && orderBy == null && query == null) {
                result = dbcol.findOne();

            } else if (fields == null && orderBy == null) {
                result = dbcol.findOne(query);

            } else if (orderBy == null) {
                result = dbcol.findOne(query, fields);

            } else {
                result = dbcol.findOne(query, fields, orderBy);
            }


            return (result == null)
                    ? Sequence.EMPTY_SEQUENCE
                    : BSONtoMap.convert(result,context);

        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }

}
