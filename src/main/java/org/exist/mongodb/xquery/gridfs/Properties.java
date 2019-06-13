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
package org.exist.mongodb.xquery.gridfs;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.bson.types.ObjectId;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ContentSerializer;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.xquery.*;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Properties extends BasicFunction {

    private static final String PROPS_BY_OBJECTID = "properties-by-objectid";

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(PROPS_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
                    "Retrieve properties and metadata of a document",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_OBJECTID,},
                    new FunctionReturnSequenceType(Type.ELEMENT, Cardinality.ONE, "XML fragment with document properties")
            ),
    };

    public Properties(final XQueryContext context, final FunctionSignature signature) {
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
            final String bucket = args[2].itemAt(0).getStringValue();
            final String documentId = args[3].itemAt(0).getStringValue();

            // Get database
            final DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            final GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            final GridFSDBFile gfsFile = (isCalledAs(PROPS_BY_OBJECTID))
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            if (gfsFile == null) {
                throw new XPathException(this, GridfsModule.GRFS0004, String.format("Document '%s' could not be found.", documentId));
            }

            return ContentSerializer.getReport(gfsFile);

        } catch (final XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (final MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, GridfsModule.GRFS0002, ex.getMessage());

        } catch (final Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, GridfsModule.GRFS0003, ex.getMessage());
        }

    }

}
