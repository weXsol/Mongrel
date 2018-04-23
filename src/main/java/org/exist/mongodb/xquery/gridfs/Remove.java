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
import org.bson.types.ObjectId;
import org.exist.dom.QName;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to remove documents from GridFS
 *
 * @author Dannes Wessels
 */
public class Remove extends BasicFunction {

    private static final String REMOVE_BY_OBJECTID = "remove-by-objectid";
    private static final String REMOVE_BY_FILENAME = "remove-by-filename";

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName(REMOVE_BY_FILENAME, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
                    "Remove document from gridFS",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_FILENAME,
                    },
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "Filename of removed document")
            ),
            new FunctionSignature(
                    new QName(REMOVE_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
                    "Remove document from gridFS",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_OBJECTID,
                    },
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "ObjectID of removed document.")
            ),};

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
            String bucket = args[2].itemAt(0).getStringValue();
            String documentId = args[3].itemAt(0).getStringValue();

            // Get database
            DB db = client.getDB(dbname);

            // Create a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Remove document by id or by filename
            if (isCalledAs(REMOVE_BY_OBJECTID)) {
                gfs.remove(new ObjectId(documentId));
            } else {
                gfs.remove(documentId);
            }

            return new StringValue(documentId);

        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, GridfsModule.GRFS0002, ex.getMessage());

        } catch (Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, GridfsModule.GRFS0003, ex.getMessage());
        }

    }


}
