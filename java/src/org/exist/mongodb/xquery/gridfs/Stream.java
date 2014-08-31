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
import java.io.OutputStream;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.exist.dom.QName;
import org.exist.http.servlets.ResponseWrapper;
import org.exist.mongodb.shared.Constants;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.Variable;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.response.ResponseModule;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.JavaObjectValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Stream extends BasicFunction {

    private static final String FIND_BY_OBJECTID = "stream-findone-by-objectid";
    private static final String FIND_BY_FILENAME = "stream-findone-by-filename";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(FIND_BY_FILENAME, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Store document into Gridfs",
        new SequenceType[]{
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "MongoDB client id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("filename", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("as-attachment", Type.BOOLEAN, Cardinality.ONE, "Add content-disposition header"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "Servlet output stream")
        ),
        new FunctionSignature(
        new QName(FIND_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Store document into Gridfs",
        new SequenceType[]{
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "Mongo driver id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("objectid", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("as-attachment", Type.BOOLEAN, Cardinality.ONE, "Add content-disposition header"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "Servlet output stream")
        ),};

    public Stream(XQueryContext context, FunctionSignature signature) {
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
            Boolean setDisposition = args[4].itemAt(0).toJavaObject(Boolean.class);

            // Stream appropriate Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(driverId);

            // Stream database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            GridFSDBFile gfsFile = (isCalledAs(FIND_BY_OBJECTID))
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            if (gfsFile == null) {
                // TODO make catchable with try0catch
                throw new XPathException(this, GridfsModule.GRFS0001, String.format("Document '%s' could not be found.", documentId));
            }

            // Stream response stream
            ResponseWrapper rw = getResponseWrapper(context);

            // Set HTTP Headers
            
            
            long length = gfsFile.getLength();
            rw.addHeader("Content-Length", "" + length);

            // Set filename when required
            String filename = determineFilename(documentId, gfsFile);
            if (setDisposition && StringUtils.isNotBlank(filename)) {
                rw.addHeader("Content-Disposition", "attachment;filename=" + filename);
            }
            
            String contentType = getMimeType(gfsFile.getContentType(), filename);
            if(contentType!=null){
                rw.setContentType(contentType);
            }

            // Stream data
            try (OutputStream os = rw.getOutputStream()) {
                gfsFile.writeTo(os);
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

    /**
     * Get filename from the provided filename, or as stored in the database when blank e.g 
     * because document is referenced by documentID
     */
    private String determineFilename(String documentId, GridFSDBFile gfsFile) {
        String documentName = null;

        // Use filename when it is passed to method
        if (isCalledAs(FIND_BY_FILENAME) && StringUtils.isNotBlank(documentId)) {
            documentName = documentId;
        }

        // If documentname is not set, retrieve from database
        if (StringUtils.isBlank(documentName)) {
            documentName = gfsFile.getFilename();
        }

        return documentName;
    }

    /**
     * Get mime-type: from stored value or from file name. Value NULL has not existent or blank.
     */
    private String getMimeType(String storedType, String filename) throws XPathException {

        String mimeType = storedType;

        // When no data is found  get from filename
        if (StringUtils.isBlank(mimeType) && StringUtils.isNotBlank(filename)) {
            MimeType mime = MimeTable.getInstance().getContentTypeFor(filename);
            mimeType = mime.getName();
        }

        // Nothing could be found
        if (StringUtils.isBlank(mimeType)) {
            LOG.debug(String.format("Content type for %s could not be retrieved from database or document name.", filename));
            mimeType=null; // force NULL
        }

        return mimeType;
    }

    /**
     * Stream HTTP response wrapper which provides access to the servlet outputstream.
     *
     * @throws XPathException Thrown when something bad happens.
     */
    private ResponseWrapper getResponseWrapper(XQueryContext context) throws XPathException {
        ResponseModule myModule = (ResponseModule) context.getModule(ResponseModule.NAMESPACE_URI);
        // response object is read from global variable $response
        Variable respVar = myModule.resolveVariable(ResponseModule.RESPONSE_VAR);
        if (respVar == null) {
            throw new XPathException(this, "No response object found in the current XQuery context.");
        }
        if (respVar.getValue().getItemType() != Type.JAVA_OBJECT) {
            throw new XPathException(this, "Variable $response is not bound to an Java object.");
        }
        JavaObjectValue respValue = (JavaObjectValue) respVar.getValue().itemAt(0);
        if (!"org.exist.http.servlets.HttpResponseWrapper".equals(respValue.getObject().getClass().getName())) {
            throw new XPathException(this, signatures[1].toString()
                    + " can only be used within the EXistServlet or XQueryServlet");
        }
        ResponseWrapper response = (ResponseWrapper) respValue.getObject();

        return response;
    }
}
