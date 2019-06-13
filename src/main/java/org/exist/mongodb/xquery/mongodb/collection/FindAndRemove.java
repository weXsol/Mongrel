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
 * Function to remove document from mongodb
 *
 * @author Dannes Wessels
 */
public class FindAndRemove extends BasicFunction {

    private static final String FIND_AND_REMOVE = "findAndRemove";


    public final static FunctionSignature[] signatures = {

            new FunctionSignature(
                    new QName(FIND_AND_REMOVE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Atomically modify and return a single document.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The document as it was before it was removed formatted as JSON")
            ),

    };

    public FindAndRemove(final XQueryContext context, final FunctionSignature signature) {
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

            // Get collection in database
            final DB db = client.getDB(dbname);
            final DBCollection dbcol = db.getCollection(collection);

            // Execute query      
            final DBObject result = dbcol.findAndRemove(query);

            // Parse results

            return (result == null)
                    ? Sequence.EMPTY_SEQUENCE
                    : BSONtoMap.convert(result, context);

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }


}
