package org.exist.mongodb.shared;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSFile;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import javax.xml.transform.OutputKeys;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.exist.memtree.DocumentImpl;
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

    public static void serialize(Item item, XQueryContext context, OutputStream os) throws XPathException, MalformedURLException, IOException {

        if (item.getType() == Type.JAVA_OBJECT) {
            LOG.debug("Streaming Java object");

            final Object obj = ((JavaObjectValue) item).getObject();
            if (!(obj instanceof File)) {
                throw new XPathException("Passed java object should be a File");
            }

            final File inputFile = (File) obj;
            final InputStream is = new BufferedInputStream(new FileInputStream(inputFile));
            IOUtils.copyLarge(is, os);
            IOUtils.closeQuietly(is);

        } else if (item.getType() == Type.ANY_URI) {
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

        } else if (item.getType() == Type.ELEMENT || item.getType() == Type.DOCUMENT) {
            LOG.debug("Streaming element or document node");

            final Serializer serializer = context.getBroker().newSerializer();

            final NodeValue node = (NodeValue) item;

            //parse serialization options
            final Properties outputProperties = new Properties();
            outputProperties.setProperty(OutputKeys.INDENT, "yes");
            outputProperties.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

            LOG.debug("Serializing started.");
            final SAXSerializer sax = (SAXSerializer) SerializerPool.getInstance().borrowObject(SAXSerializer.class);
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
                SerializerPool.getInstance().returnObject(sax);
            }

        } else if (item.getType() == Type.BASE64_BINARY || item.getType() == Type.HEX_BINARY) {
            LOG.debug("Streaming base64 binary");
            final BinaryValue binary = (BinaryValue) item;
            binary.streamBinaryTo(os);

        } else if (item.getType() == Type.TEXT || item.getType() == Type.STRING) {
            LOG.debug("Streaming text");
            IOUtils.write(item.getStringValue(), os, "UTF-8");

        } else {
            LOG.error("Wrong item type " + Type.getTypeName(item.getType()));
            throw new XPathException("wrong item type " + Type.getTypeName(item.getType()));
        }

    }

    public static NodeImpl getReport(GridFSFile gfsFile) throws XPathException {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        int nodeNr = addGridFSFileEntry(builder, gfsFile);

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);
    }

    public static NodeImpl getDocuments(GridFS gfs) throws XPathException {
        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();
        
        int nodeNr = builder.startElement("", "GridFSFiles", "GridFSFiles", null);

        DBCursor cursor = gfs.getFileList();
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            if (next instanceof GridFSFile) {
                addGridFSFileEntry(builder, (GridFSFile) next);
            }
        }
        
        builder.endElement();
        
        builder.endDocument();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);
    }

    public static int addGridFSFileEntry(MemTreeBuilder builder, GridFSFile gfsFile) throws XPathException {
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
        addElementValue(builder, "uploadDate", "" + (new DateTimeValue(gfsFile.getUploadDate()).getStringValue()));
        addElementValue(builder, "md5", gfsFile.getMD5());

        // finish root element
        builder.endElement();

        return nodeNr;
    }

    private static void addElementValue(MemTreeBuilder builder, String elementName, String value) {

        if (StringUtils.isNotBlank(value) && StringUtils.isNotBlank(elementName)) {
            builder.startElement("", elementName, elementName, null);
            builder.characters(value);
            builder.endElement();
        }

    }

}
