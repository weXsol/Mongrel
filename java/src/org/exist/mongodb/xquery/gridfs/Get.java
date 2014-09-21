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
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.exist.Namespaces;
import org.exist.dom.QName;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.SAXAdapter;
import org.exist.mongodb.shared.Constants;
import static org.exist.mongodb.shared.Constants.EXIST_COMPRESSION;
import static org.exist.mongodb.shared.Constants.EXIST_DATATYPE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_BUCKET;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_FILENAME;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_OBJECTID;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.validation.ValidationReport;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Base64BinaryDocument;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Get extends BasicFunction {

    private static final String FIND_BY_OBJECTID = "get-by-objectid";
    private static final String FIND_BY_FILENAME = "get-by-filename";
    
    private static final FunctionParameterSequenceType PARAMETER_FORCE_BINARY = 
            new FunctionParameterSequenceType("forceBinary", Type.BOOLEAN, Cardinality.ONE, "Set true() to force binary datatype for XML data.");

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName(FIND_BY_FILENAME, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
            "Retrieve document",
            new SequenceType[]{
                PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_FILENAME, PARAMETER_FORCE_BINARY
            },
            new FunctionReturnSequenceType(Type.ITEM, Cardinality.ZERO_OR_ONE, "Requested document")
        ),     
        new FunctionSignature(
            new QName(FIND_BY_OBJECTID, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
            "Retrieve document",
            new SequenceType[]{
                PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_BUCKET, PARAMETER_OBJECTID, PARAMETER_FORCE_BINARY,},
            new FunctionReturnSequenceType(Type.ITEM, Cardinality.ZERO_OR_ONE, "Requested document")
        ),
    };

    public Get(XQueryContext context, FunctionSignature signature) {
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
            boolean forceBinary = args[4].itemAt(0).toJavaObject(Boolean.class);

            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);

            // Creates a GridFS instance for the specified bucket
            GridFS gfs = new GridFS(db, bucket);

            // Find one document by id or by filename
            GridFSDBFile gfsFile = (isCalledAs(FIND_BY_OBJECTID))
                    ? gfs.findOne(new ObjectId(documentId))
                    : gfs.findOne(documentId);

            if (gfsFile == null) {
                // TODO make catchable with try-catch
                throw new XPathException(this, GridfsModule.GRFS0001, String.format("Document '%s' could not be found.", documentId));
            }

            Sequence retVal = get(gfsFile, forceBinary);

            return retVal;

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

    /**
     *  Get document from GridFS
     */
    Sequence get(GridFSDBFile gfsFile, boolean forceBinary) throws IOException, XPathException {
        
        // Obtain meta-data
        DBObject metadata = gfsFile.getMetaData();
        
        // Decompress when needed
        String compression = (metadata == null) ? null : (String) metadata.get(EXIST_COMPRESSION);
        boolean isGzipped = StringUtils.equals(compression, Constants.GZIP);
        InputStream is = isGzipped ? new GZIPInputStream(gfsFile.getInputStream()) : gfsFile.getInputStream();
        
        // Find what kind of data is stored
        int datatype = (metadata == null) ? Type.UNTYPED : (int) metadata.get(EXIST_DATATYPE);
        boolean hasXMLContentType = StringUtils.contains(gfsFile.getContentType(), "xml");
        boolean isXMLtype = (Type.DOCUMENT == datatype || Type.ELEMENT == datatype || hasXMLContentType);
        
        // Convert input stream to eXist-db object
        Sequence retVal;
        if (forceBinary || !isXMLtype) {
            retVal = Base64BinaryDocument.getInstance(context, is);

        } else {
            retVal = processXML(context, is);
        }
        return retVal;
    }

    /**
     * Parse an byte-array containing (compressed) XML data into an eXist-db
     * document.
     *
     * @param data Byte array containing the XML data.
     * @return Sequence containing the XML as DocumentImpl
     *
     * @throws XPathException Something bad happened.
     */
    private Sequence processXML(XQueryContext xqueryContext, InputStream is) throws XPathException {

        Sequence content = null;
        try {
            final ValidationReport validationReport = new ValidationReport();
            final SAXAdapter adapter = new SAXAdapter(xqueryContext);
            final SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            final InputSource src = new InputSource(is);
            final SAXParser parser = factory.newSAXParser();
            XMLReader xr = parser.getXMLReader();

            xr.setErrorHandler(validationReport);
            xr.setContentHandler(adapter);
            xr.setProperty(Namespaces.SAX_LEXICAL_HANDLER, adapter);

            xr.parse(src);

            // Cleanup
            IOUtils.closeQuietly(is);

            if (validationReport.isValid()) {
                content = (DocumentImpl) adapter.getDocument();
            } else {
                String txt = String.format("Received document is not valid: %s", validationReport.toString());
                LOG.debug(txt);
                throw new XPathException(txt);
            }

        } catch (SAXException | ParserConfigurationException | IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(ex.getMessage());

        }

        return content;
    }
}
