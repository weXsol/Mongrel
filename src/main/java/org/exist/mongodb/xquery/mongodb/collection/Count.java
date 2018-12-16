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
import com.mongodb.MongoClient;
import org.exist.dom.QName;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MapToBSON;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Count the number of documents in the collection
 *
 * @author Dannes Wessels
 */
public class Count extends BasicFunction {

    private static final String COUNT = "count";

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(COUNT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Count the number of documents in the collection.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION},
                    new FunctionReturnSequenceType(Type.LONG, Cardinality.ONE, "Number of documents")
            ),

            new FunctionSignature(
                    new QName(COUNT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Count the number of documents in the collection that match the query.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
                    new FunctionReturnSequenceType(Type.INTEGER, Cardinality.ONE, "Number of documents")
            ),
    };

    public Count(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            final String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            final MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            final String dbname = args[1].itemAt(0).getStringValue();
            final String collection = args[2].itemAt(0).getStringValue();

            // Get query when available
            final BasicDBObject mongoQuery = (args.length == 4)
                    ? MapToBSON.convert(args[3])
                    : null;

            // Get database and collection
            final DB db = client.getDB(dbname);
            final DBCollection dbcol = db.getCollection(collection);

            // Count documents
            final Long nrOfDocuments = (mongoQuery == null) ? dbcol.count() : dbcol.count(mongoQuery);

            return new IntegerValue(nrOfDocuments);

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }


}
