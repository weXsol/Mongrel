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
import org.exist.mongodb.shared.Constants;
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
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "MongoDB client id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("filename", Type.STRING, Cardinality.ONE, "Name of document"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "n/a")
        ),
        new FunctionSignature(
        new QName(REMOVE_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Remove document from gridFS",
        new SequenceType[]{
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "MongoDB client id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("objectid", Type.STRING, Cardinality.ONE, "Name of document"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "n/a")
        ),};

    public Remove(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.MONGODB_GROUP)) {
            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.MONGODB_GROUP);
            LOG.error(txt);
            throw new XPathException(this, txt);
        }

        try {
            // Stream parameters
            String driverId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String bucket = args[2].itemAt(0).getStringValue();
            String documentId = args[3].itemAt(0).getStringValue();

            // Stream appropriate Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(driverId);

            // Stream database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Remove one document by id or by filename
            if (isCalledAs(REMOVE_BY_OBJECTID)) {
                gfs.remove(new ObjectId(documentId));
            } else {
                gfs.remove(documentId);
            }


        } catch (XPathException ex) {
            LOG.error(ex);
            throw ex;
            
        } catch (MongoException ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);       
            
        } catch (Throwable ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);
        }


        return Sequence.EMPTY_SEQUENCE;

    }


}
