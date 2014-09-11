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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ContentSerializer;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * Implementation gridfs:connect() functions
 *
 * @author Dannes Wessels
 */
public class Store extends BasicFunction {

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName("store", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "Store document into Gridfs",
        new SequenceType[]{
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "MongoDB client id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("filename", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("contentType", Type.STRING, Cardinality.ZERO_OR_ONE, "Document Content type, use () for mime-type based on file extension"),
            new FunctionParameterSequenceType("content", Type.ITEM, Cardinality.ONE, "Document content as node() or  base64-binary")
        },
        new FunctionReturnSequenceType(Type.ELEMENT, Cardinality.ONE, "an ID")
        ),};

    public Store(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Get parameters
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String bucket = args[2].itemAt(0).getStringValue();
            String documentName = args[3].itemAt(0).getStringValue();
            String contentType = getMimeType(args[4],documentName);
            
            // Actual content: File object, doc() element, base64...
            Item content = args[5].itemAt(0);
            int dataType = content.getType();
            
            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get appropriate Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);
            
            // Create file
            GridFSInputFile gfsFile = gfs.createFile();
            
            // Set meta data
            gfsFile.setFilename(documentName);
            gfsFile.setContentType(contentType);
                       
            // Write data
            try ( OutputStream stream = gfsFile.getOutputStream() ) {
                MessageDigest md = MessageDigest.getInstance("MD5");
                GZIPOutputStream gos = new GZIPOutputStream(stream);
                DigestOutputStream dos = new DigestOutputStream(gos, md);
                CountingOutputStream cos = new CountingOutputStream(dos);
                
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                ContentSerializer.serialize(content, context, cos);
                cos.flush();
                cos.close();
                stopWatch.stop();
                
                long nrBytes=cos.getByteCount();
                String checksum = Hex.encodeHexString( dos.getMessageDigest().digest() );
                long duration = stopWatch.getTime();
                
                LOG.info("#bytes="+nrBytes);
                LOG.info("md5sum="+checksum);
                LOG.info("time="+duration);
                
                BasicDBObject info = new BasicDBObject();
                info.put("compression", "gzip");
                info.put("original_size", nrBytes);
                info.put("original_md5", checksum);
                info.put("exist_datatype", dataType);
                info.put("exist_datatype_text", Type.getTypeName(dataType));
                  
                gfsFile.setMetaData(info);
            }

            // Report identifier
            return ContentSerializer.getReport(gfsFile);
            
        
        } catch (XPathException ex) {
            LOG.error(ex);
            throw ex;
            
        } catch (MongoException ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);       
            
        } catch (Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex);
        }

    }

    



    private String getMimeType(Sequence inputValue, String filename) throws XPathException {
        
        String mimeType = null;

        // Use input when provided
        if (inputValue.hasOne()) {
            mimeType = inputValue.itemAt(0).getStringValue();
        } 
        
        // When no data is found  get from filename
        if(StringUtils.isBlank(mimeType) && StringUtils.isNotBlank(filename)){
            MimeType mime = MimeTable.getInstance().getContentTypeFor(filename);
            mimeType =  mime.getName();
        }
        
        // Nothing could be found
        if(StringUtils.isBlank(mimeType)){
            throw new XPathException(this,"Content type could not be retrieved from parameter or document name.");
        }

        return mimeType;
    }
}
