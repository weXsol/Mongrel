package org.exist.mongodb.shared;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.dom.memtree.NodeImpl;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.*;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URL;
import java.util.List;

/**
 * @author wessels
 */
public class ContentSerializer {

    protected final static Logger LOG = LogManager.getLogger(ContentSerializer.class);

    /**
     * Stream content of item to output stream
     *
     * @param item    The Item
     * @param context The XQuery context
     * @param os      The output stream
     * @throws XPathException Thrown when an something unexpected happened
     * @throws IOException    An error occurred during serialization
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


        try (final DBBroker broker = context.getBroker()) {
            final Serializer serializer = broker.newSerializer();
            serializer.reset();

            final NodeValue node = (NodeValue) item;


            LOG.debug("Serializing started.");

            try {
                final String encoding = "UTF-8";
                try (Writer writer = new OutputStreamWriter(os, encoding)) {
                    serializer.serialize(node, writer);
                }

            } catch (final IOException | SAXException e) {
                final String txt = "A problem occurred while serializing the node set: " + e.getMessage();
                LOG.debug(txt, e);
                throw new IOException(txt, e);

            } finally {
                LOG.debug("Serializing done.");
            }
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

        try (InputStream is = new BufferedInputStream(new URL(url).openStream())) {
            IOUtils.copyLarge(is, os);
        }
    }

    private static void streamJavaObject(Item item, OutputStream os) throws XPathException, IOException {
        LOG.debug("Streaming Java object");

        final Object obj = ((JavaObjectValue) item).getObject();
        if (!(obj instanceof File)) {
            throw new XPathException("Passed java object should be a File object");
        }

        final File inputFile = (File) obj;
        try (InputStream is = new BufferedInputStream(new FileInputStream(inputFile))) {
            IOUtils.copyLarge(is, os);
        }
    }

    /**
     * Get simple report for the Gridfs file
     *
     * @param gfsFile The GridFS fle
     * @return in-memory structure describing the file
     */
    public static NodeImpl getReport(GridFSFile gfsFile) {

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
            gfsFile.getAliases().forEach((alias) -> addElementValue(builder, "alias", alias));
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

        DBObject metaData = gfsFile.getMetaData();
        if (metaData != null && !metaData.keySet().isEmpty()) {
            builder.startElement("", "metaData", "metaData", null);

            metaData.keySet().forEach((key) -> {
                String value = metaData.get(key).toString();
                addElementValue(builder, key, value);
            });

            builder.endElement();
        }

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
