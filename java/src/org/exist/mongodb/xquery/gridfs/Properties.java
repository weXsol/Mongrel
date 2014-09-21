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
import static org.exist.mongodb.shared.Constants.DESCR_BUCKET;
import static org.exist.mongodb.shared.Constants.DESCR_DATABASE;
import static org.exist.mongodb.shared.Constants.DESCR_MONGODB_CLIENT_ID;
import static org.exist.mongodb.shared.Constants.DESCR_OBJECT_ID;
import static org.exist.mongodb.shared.Constants.PARAM_BUCKET;
import static org.exist.mongodb.shared.Constants.PARAM_DATABASE;
import static org.exist.mongodb.shared.Constants.PARAM_MONGODB_CLIENT_ID;
import static org.exist.mongodb.shared.Constants.PARAM_OBJECT_ID;
import org.exist.mongodb.shared.ContentSerializer;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Properties extends BasicFunction {

    private static final String PROPS_BY_OBJECTID = "properties-by-objectid";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(PROPS_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Retrieve properties and metadata of document",
        new SequenceType[]{
            new FunctionParameterSequenceType(PARAM_MONGODB_CLIENT_ID, Type.STRING, Cardinality.ONE, DESCR_MONGODB_CLIENT_ID),
            new FunctionParameterSequenceType(PARAM_DATABASE, Type.STRING, Cardinality.ONE, DESCR_DATABASE),
            new FunctionParameterSequenceType(PARAM_BUCKET, Type.STRING, Cardinality.ONE, DESCR_BUCKET),
            new FunctionParameterSequenceType(PARAM_OBJECT_ID, Type.STRING, Cardinality.ONE, DESCR_OBJECT_ID),},
        new FunctionReturnSequenceType(Type.ELEMENT, Cardinality.ONE, "XML fragment with document properties")
        ),};

    public Properties(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Stream parameters
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String bucket = args[2].itemAt(0).getStringValue();
            String documentId = args[3].itemAt(0).getStringValue();

            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            GridFSDBFile gfsFile = (isCalledAs(PROPS_BY_OBJECTID))
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            if (gfsFile == null) {
                throw new XPathException(this, GridfsModule.GRFS0001, String.format("Document '%s' could not be found.", documentId));
            }
         
            return ContentSerializer.getReport(gfsFile);

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
