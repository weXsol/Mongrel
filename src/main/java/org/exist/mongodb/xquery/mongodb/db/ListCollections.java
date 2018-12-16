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

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.exist.dom.QName;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.util.Set;

import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;

/**
 * Function to list all GridFS collections
 *
 * @author Dannes Wessels
 */
public class ListCollections extends BasicFunction {

    private static final String LIST_DOCUMENTS = "list-collections";

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(LIST_DOCUMENTS, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX),
                    "List the names of all collections contained in a databases. " +
                            "The connection is identified by the supplied $mongodbClientId, and the name of the database " +
                            "is supplied via $database.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE,},
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "Sequence of bucket names")
            ),};

    public ListCollections(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            final String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            final MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Additional parameter
            final String dbname = args[1].itemAt(0).getStringValue();

            // Retrieve database          
            final DB db = client.getDB(dbname);

            // Retrieve collection names
            final Set<String> collectionNames = db.getCollectionNames();

            // Storage for results
            final ValueSequence valueSequence = new ValueSequence();

            // Iterate over collection names
            collectionNames.forEach((collName) -> valueSequence.add(new StringValue(collName)));

            return valueSequence;

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }


    }

}
