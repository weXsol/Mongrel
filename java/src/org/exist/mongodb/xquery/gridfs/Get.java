/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2013 The eXist Project
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
 * Implementation gridfs:get() functions
 *
 * @author Dannes Wessels
 */
public class Get extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName("stream-findone-by-filename", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Store document into Gridfs",
        new SequenceType[]{
            new FunctionParameterSequenceType("id", Type.STRING, Cardinality.ONE, "Mongo driver id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("collection", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("filename", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("as-attachment", Type.BOOLEAN, Cardinality.ONE, "Add content-disposition header"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "Servlet output stream")
        ),
        new FunctionSignature(
        new QName("stream-findone-by-objectid", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Store document into Gridfs",
        new SequenceType[]{
            new FunctionParameterSequenceType("id", Type.STRING, Cardinality.ONE, "Mongo driver id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("collection", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("objectid", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("as-attachment", Type.BOOLEAN, Cardinality.ONE, "Add content-disposition header"),},
        new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, "Servlet output stream")
        ),};
    
    public Get(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the JMS group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.MONGODB_GROUP)) {
            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.MONGODB_GROUP);
            XPathException ex = new XPathException(this, txt);
            LOG.error(txt, ex);
            throw ex;
        }
        
        try {
            // Get parameters
            String driverId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String bucket = args[2].itemAt(0).getStringValue();
            String documentId = args[3].itemAt(0).getStringValue();
            Boolean setDisposition = args[4].itemAt(0).toJavaObject(Boolean.class);

            // Get appropriate Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(driverId);

            // Get database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            GridFSDBFile gfsFile = (isCalledAs("stream-findone-by-objectid")) 
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            // Set meta data
            String contentType = getMimeType(gfsFile.getContentType(), documentId);
            long length = gfsFile.getLength();

            // Get response stream
            ResponseWrapper rw = getResponseWrapper(context);

            // Set Headers
            rw.setContentType(contentType);
            rw.addHeader("Content-Length", "" + length);
            
            String filename = determineFilename(documentId, gfsFile);
            
            if (setDisposition && StringUtils.isNotBlank(filename)) {
                rw.addHeader("Content-Disposition", "attachment;filename=" + filename);
            }

            // Stream data
            try (OutputStream os = rw.getOutputStream()) {
                gfsFile.writeTo(os);
            }
            
        } catch (XPathException ex) {
            LOG.error(ex);
            throw ex;
            
        } catch (Throwable ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);
        }
        
        return Sequence.EMPTY_SEQUENCE;
        
    }

    private String determineFilename(String documentId, GridFSDBFile gfsFile) {
        String documentName = null;
        
        // Use filename when it is passed to method
        if(isCalledAs("stream-findone-by-filename") && StringUtils.isNotBlank(documentId)){
            documentName=documentId;
        }
        
        // If documentname is not set, retrieve from database
        if(StringUtils.isBlank(documentName)){
            documentName = gfsFile.getFilename();
        }
        
        return documentName;
    }
    
    private String getMimeType(String stored, String filename) throws XPathException {
        
        String mimeType = stored;

        // When no data is found  get from filename
        if (StringUtils.isBlank(mimeType)) {
            MimeType mime = MimeTable.getInstance().getContentTypeFor(filename);
            mimeType = mime.getName();
        }

        // Nothing could be found
        if (StringUtils.isBlank(mimeType)) {
            throw new XPathException(this, "Content type could not be retrieved from database or document name.");
        }
        
        return mimeType;
    }

    /**
     * Get HTTP response wrapper which provides access to the servlet outputstream.
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
