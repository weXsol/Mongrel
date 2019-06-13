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
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.exist.dom.QName;
import org.exist.http.servlets.RequestWrapper;
import org.exist.http.servlets.ResponseWrapper;
import org.exist.mongodb.shared.Constants;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xquery.*;
import org.exist.xquery.functions.request.RequestModule;
import org.exist.xquery.functions.response.ResponseModule;
import org.exist.xquery.value.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import static org.exist.mongodb.shared.Constants.*;
import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Stream extends BasicFunction {

    private static final String FIND_BY_OBJECTID = "stream-by-objectid";
    private static final String FIND_BY_FILENAME = "stream-by-filename";

    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(FIND_BY_FILENAME, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
                    "Retrieve document by filename as stream",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_FILENAME, PARAMETER_AS_ATTACHMENT
                    },
                    new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, Constants.DESCR_OUTPUT_STREAM)
            ),
            new FunctionSignature(
                    new QName(FIND_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
                    "Retrieve document by objectid as stream",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_OBJECTID, PARAMETER_AS_ATTACHMENT
                    },
                    new FunctionReturnSequenceType(Type.EMPTY, Cardinality.EMPTY, Constants.DESCR_OUTPUT_STREAM)
            ),
    };

    public Stream(final XQueryContext context, final FunctionSignature signature) {
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
            final Boolean setDisposition = args[4].itemAt(0).toJavaObject(Boolean.class);

            // Get database
            final DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            final GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            final GridFSDBFile gfsFile = (isCalledAs(FIND_BY_OBJECTID))
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            stream(gfsFile, documentId, setDisposition);

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

        return Sequence.EMPTY_SEQUENCE;

    }

    /**
     * Stream document to HTTP agent
     */
    void stream(final GridFSDBFile gfsFile, final String documentId, final Boolean setDisposition) throws IOException, XPathException {
        if (gfsFile == null) {
            throw new XPathException(this, GridfsModule.GRFS0004, String.format("Document '%s' could not be found.", documentId));
        }

        final DBObject metadata = gfsFile.getMetaData();

        // Determine actual size
        final String compression = (metadata == null) ? null : (String) metadata.get(EXIST_COMPRESSION);
        final Long originalSize = (metadata == null) ? null : (Long) metadata.get(EXIST_ORIGINAL_SIZE);

        long length = gfsFile.getLength();
        if (originalSize != null) {
            length = originalSize;
        }

        // Stream response stream
        final ResponseWrapper rw = getResponseWrapper(context);

        // Set HTTP Headers
        rw.addHeader(Constants.CONTENT_LENGTH, String.format("%s", length));

        // Set filename when required
        final String filename = determineFilename(documentId, gfsFile);
        if (setDisposition && StringUtils.isNotBlank(filename)) {
            rw.addHeader(Constants.CONTENT_DISPOSITION, String.format("attachment;filename=%s", filename));
        }

        final String contentType = getMimeType(gfsFile.getContentType(), filename);
        if (contentType != null) {
            rw.setContentType(contentType);
        }

        final boolean isGzipSupported = isGzipEncodingSupported(context);

        // Stream data
        if ((StringUtils.isBlank(compression))) {
            // Write data as-is, no marker available that data is stored compressed
            try (final OutputStream os = rw.getOutputStream()) {
                gfsFile.writeTo(os);
                os.flush();
            }

        } else {

            if (isGzipSupported && StringUtils.contains(compression, GZIP)) {
                // Write compressend data as-is, since data is stored as gzipped data and
                // the agent suports it.
                rw.addHeader(Constants.CONTENT_ENCODING, GZIP);
                try (final OutputStream os = rw.getOutputStream()) {
                    gfsFile.writeTo(os);
                    os.flush();
                }

            } else {
                // Write data uncompressed
                try (final OutputStream os = rw.getOutputStream()) {
                    final InputStream is = gfsFile.getInputStream();
                    try (final GZIPInputStream gzis = new GZIPInputStream(is)) {
                        IOUtils.copyLarge(gzis, os);
                        os.flush();
                    }
                }
            }
        }
    }

    /**
     * Get filename from the provided filename, or as stored in the database
     * when blank e.g because document is referenced by documentID
     */
    private String determineFilename(final String documentId, final GridFSDBFile gfsFile) {
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
     * Get mime-type: from stored value or from file name. Value NULL has not
     * existent or blank.
     */
    private String getMimeType(final String storedType, final String filename) {

        String mimeType = storedType;

        // When no data is found  get from filename
        if (StringUtils.isBlank(mimeType) && StringUtils.isNotBlank(filename)) {
            final MimeType mime = MimeTable.getInstance().getContentTypeFor(filename);
            mimeType = mime.getName();
        }

        // Nothing could be found
        if (StringUtils.isBlank(mimeType)) {
            LOG.debug(String.format("Content type for %s could not be retrieved from database or document name.", filename));
            mimeType = null; // force NULL
        }

        return mimeType;
    }

    /**
     * Get the Response wrapper which provides access to the servlet outputstream.
     *
     * @throws XPathException Thrown when something bad happens.
     */
    private ResponseWrapper getResponseWrapper(final XQueryContext context) throws XPathException {
        final ResponseModule myModule = (ResponseModule) context.getModule(ResponseModule.NAMESPACE_URI);
        // response object is read from global variable $response
        final Variable respVar = myModule.resolveVariable(ResponseModule.RESPONSE_VAR);
        if (respVar == null) {
            throw new XPathException(this, "No response object found in the current XQuery context.");
        }
        if (respVar.getValue().getItemType() != Type.JAVA_OBJECT) {
            throw new XPathException(this, "Variable $response is not bound to an Java object.");
        }
        final JavaObjectValue respValue = (JavaObjectValue) respVar.getValue().itemAt(0);
        if (!"org.exist.http.servlets.HttpResponseWrapper".equals(respValue.getObject().getClass().getName())) {
            throw new XPathException(this, signatures[1].toString()
                    + " can only be used within the EXistServlet or XQueryServlet");
        }

        return (ResponseWrapper) respValue.getObject();
    }

    /**
     * Get the Request wrapper which provides access to the servlet
     * outputstream.
     *
     * @throws XPathException Thrown when something bad happens.
     */
    private RequestWrapper getRequestWrapper(final XQueryContext context) throws XPathException {
        final RequestModule myModule = (RequestModule) context.getModule(RequestModule.NAMESPACE_URI);
        // request object is read from global variable $request
        final Variable respVar = myModule.resolveVariable(RequestModule.REQUEST_VAR);
        if (respVar == null) {
            throw new XPathException(this, "No request object found in the current XQuery context.");
        }
        if (respVar.getValue().getItemType() != Type.JAVA_OBJECT) {
            throw new XPathException(this, "Variable $request is not bound to an Java object.");
        }
        final JavaObjectValue respValue = (JavaObjectValue) respVar.getValue().itemAt(0);
        if (!"org.exist.http.servlets.HttpRequestWrapper".equals(respValue.getObject().getClass().getName())) {
            throw new XPathException(this, signatures[1].toString()
                    + " can only be used within the EXistServlet or XQueryServlet");
        }

        return (RequestWrapper) respValue.getObject();
    }

    /**
     * Verify if HTTP agent supports GZIP content encoding.
     */
    private boolean isGzipEncodingSupported(final XQueryContext context) {
        try {
            final RequestWrapper request = getRequestWrapper(context);

            final String content = request.getHeader(ACCEPT_ENCODING);

            if (StringUtils.contains(content, GZIP)) {
                return true;
            }

        } catch (final XPathException ex) {
            LOG.error(ex.getMessage(), ex);
        }
        return false;
    }


}
