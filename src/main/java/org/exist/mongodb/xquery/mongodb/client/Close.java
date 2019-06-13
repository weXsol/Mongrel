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
package org.exist.mongodb.xquery.mongodb.client;

import com.mongodb.MongoClient;
import org.exist.dom.QName;
import org.exist.mongodb.shared.Constants;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;

/**
 * Implementation of the gridfs:close() function
 *
 * @author Dannes Wessels
 */
public class Close extends BasicFunction {


    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName("close", MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX),
                    "Close an open database connection. The connection to be closed is identified by the supplied $mongodbClientId. " +
                            "When a database connection is closed, the associated id is discarded and invalidated. " +
                            "As a consequence, each database can be closed once.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT
                    },
                    new FunctionReturnSequenceType(Type.EMPTY, Cardinality.ZERO, "")
            ),
    };

    public Close(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.MONGODB_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.MONGODB_GROUP);
            LOG.error(txt);
            throw new XPathException(this, txt);
        }

        // Get connection URL
        final String mongodbClientId = args[0].itemAt(0).getStringValue();

        // Handle close()
        try(MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);){

            if (client == null) {
                throw new XPathException(this, String.format("Mongoclient %s could not be found.", mongodbClientId));
            }

            // Close connector with all connections
            client.close();

            // Remove from cache
            MongodbClientStore.getInstance().remove(mongodbClientId);

            // Report identifier
            return EmptySequence.EMPTY_SEQUENCE;

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }
}
