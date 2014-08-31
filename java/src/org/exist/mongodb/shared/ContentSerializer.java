package org.exist.mongodb.shared;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSFile;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import javax.xml.transform.OutputKeys;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.BinaryValue;
import org.exist.xquery.value.DateTimeValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.JavaObjectValue;
import org.exist.xquery.value.NodeValue;
import org.exist.xquery.value.Type;

/**
 *
 * @author wessels
 */
public class ContentSerializer {

    private final static Logger LOG = Logger.getLogger(ContentSerializer.class);

    /**
     * Stream content of item to output stream
     *
     * @param item The Item
     * @param context The XQUery context
     * @param os The output stream
     * @throws XPathException Thrown when an something unexpected happened
     * @throws IOException An error occurred during serialization
     */
    public static void serialize(Item item, XQueryContext context, OutputStream os) throws XPathException, IOException {

        switch (item.getType()) {
            case Type.JAVA_OBJECT:
                streamJavaObject(item, os);
                break;
            case Type.ANY_URI:
                streamAnyURI(item, os);
                break;
            case Type.ELEMENT:
            case Type.DOCUMENT:
                streamElement(context, item, os);
                break;
            case Type.BASE64_BINARY:
            case Type.HEX_BINARY:
                streamBase64Binary(item, os);
                break;
            case Type.TEXT:
            case Type.STRING:
                streamText(item, os);
                break;
            default:
                LOG.error("Wrong item type " + Type.getTypeName(item.getType()));
                throw new XPathException("wrong item type " + Type.getTypeName(item.getType()));
        }

    }

    private static void streamText(Item item, OutputStream os) throws IOException, XPathException {
        LOG.debug("Streaming text");
        IOUtils.write(item.getStringValue(), os, "UTF-8");
    }

    private static void streamBase64Binary(Item item, OutputStream os) throws IOException {
        LOG.debug("Streaming base64 binary");
        final BinaryValue binary = (BinaryValue) item;
        binary.streamBinaryTo(os);
    }

    private static void streamElement(XQueryContext context, Item item, OutputStream os) throws IOException {
        LOG.debug("Streaming element or document node");
        
        final Serializer serializer = context.getBroker().newSerializer();
        
        final NodeValue node = (NodeValue) item;
        
        // Setup serialization options
        // TODO: get global serialization options
        final Properties outputProperties = new Properties();
        outputProperties.setProperty(OutputKeys.INDENT, "yes");
        outputProperties.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        
        SerializerPool serializerPool = SerializerPool.getInstance();
        
        LOG.debug("Serializing started.");
        final SAXSerializer sax = (SAXSerializer) serializerPool.borrowObject(SAXSerializer.class);
        try {
            final String encoding = "UTF-8";
            try (Writer writer = new OutputStreamWriter(os, encoding)) {
                sax.setOutput(writer, outputProperties);
                
                serializer.reset();
                serializer.setProperties(outputProperties);
                serializer.setSAXHandlers(sax, sax);
                
                sax.startDocument();
                serializer.toSAX(node);
                
                sax.endDocument();
            }
            
        } catch (final Exception e) {
            final String txt = "A problem occurred while serializing the node set: " + e.getMessage();
            LOG.debug(txt, e);
            throw new IOException(txt, e);
            
        } finally {
            LOG.debug("Serializing done.");
            serializerPool.returnObject(sax);
        }
    }

    private static void streamAnyURI(Item item, OutputStream os) throws IOException, XPathException {
        LOG.debug("Streaming xs:anyURI");
        
        // anyURI provided
        String url = item.getStringValue();
        
        // Fix URL
        if (url.startsWith("/")) {
            url = "xmldb:exist://" + url;
        }
        
        final InputStream is = new BufferedInputStream(new URL(url).openStream());
        IOUtils.copyLarge(is, os);
        IOUtils.closeQuietly(is);
    }

    private static void streamJavaObject(Item item, OutputStream os) throws XPathException, FileNotFoundException, IOException {
        LOG.debug("Streaming Java object");
        
        final Object obj = ((JavaObjectValue) item).getObject();
        if (!(obj instanceof File)) {
            throw new XPathException("Passed java object should be a File");
        }
        
        final File inputFile = (File) obj;
        final InputStream is = new BufferedInputStream(new FileInputStream(inputFile));
        IOUtils.copyLarge(is, os);
        IOUtils.closeQuietly(is);
    }

    /**
     * Get simple report for the Gridfs file
     *
     * @param gfsFile The GridFS fle
     * @return in-memory structure describing the file
     */
    static NodeImpl getReport(GridFSFile gfsFile) {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        int nodeNr = addGridFSFileEntry(builder, gfsFile);

        // return result
        return builder.getDocument().getNode(nodeNr);
    }

    /**
     * Get list of documents in gridFS
     *
     * @param gfs GridFS object
     * @return in-memory structure describing all documents
     */
    public static NodeImpl getDocuments(GridFS gfs) {
        MemTreeBuilder builder = new MemTreeBuilder();
        
        // Start document
        builder.startDocument();
        int nodeNr = builder.startElement("", "GridFSFiles", "GridFSFiles", null);

        // Iterate over all files, write report
        DBCursor cursor = gfs.getFileList();
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            if (next instanceof GridFSFile) {
                addGridFSFileEntry(builder, (GridFSFile) next);
            }
        }

        // Finish document
        builder.endElement();
        builder.endDocument();

        // Return result
        return builder.getDocument().getNode(nodeNr);
    }

    /**
     * Add element describing GridFSFile to in-memory structure
     */
    static int addGridFSFileEntry(final MemTreeBuilder builder, final GridFSFile gfsFile) {
        // start root element
        int nodeNr = builder.startElement("", "GridFSFile", "GridFSFile", null);

        // Some identities
        addElementValue(builder, "id", "" + gfsFile.getId());
        addElementValue(builder, "filename", gfsFile.getFilename());

        List<String> aliases = gfsFile.getAliases();
        if (aliases != null) {
            for (String alias : gfsFile.getAliases()) {
                addElementValue(builder, "alias", alias);
            }
        }

        // mimetype
        addElementValue(builder, "contentType", gfsFile.getContentType());

        // sizes
        addElementValue(builder, "length", "" + gfsFile.getLength());
        addElementValue(builder, "chunkSize", "" + gfsFile.getChunkSize());
        addElementValue(builder, "numberOfChunks", "" + gfsFile.numChunks());

        // more meta data
        try {   
            addElementValue(builder, "uploadDate", "" + (new DateTimeValue(gfsFile.getUploadDate()).getStringValue()));
        } catch (XPathException ex) {
            LOG.error("Error adding upload date. " + ex.getMessage());
        }
        addElementValue(builder, "md5", gfsFile.getMD5());

        // finish root element
        builder.endElement();

        return nodeNr;
    }

    /**
     * Add simple element with value to XML structure.
     */
    static void addElementValue(final MemTreeBuilder builder, final String elementName, final String value) {

        if (StringUtils.isNotBlank(value) && StringUtils.isNotBlank(elementName)) {
            builder.startElement("", elementName, elementName, null);
            builder.characters(value);
            builder.endElement();
        } else {
            LOG.debug(String.format("Skipping element '%s' with value '%s'.", elementName, value));
        }

    }

}
