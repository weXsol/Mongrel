package org.exist.mongodb.shared;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.exist.dom.NodeProxy;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.validation.internal.node.NodeInputStream;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Base64BinaryDocument;
import org.exist.xquery.value.BinaryValue;
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

        final StreamSource streamSource = new StreamSource();
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
                final String txt = "A problem occurred while serializing the node set";
                LOG.debug(txt + ".", e);
                throw new IOException(txt + ": " + e.getMessage(), e);

            } finally {
                LOG.debug("Serializing done.");
                SerializerPool.getInstance().returnObject(sax);
            }

        } else if (item.getType() == Type.BASE64_BINARY || item.getType() == Type.HEX_BINARY) {
            LOG.debug("Streaming base64 binary");
            final BinaryValue binary = (BinaryValue) item;            
            binary.streamTo(os);
            
         } else if (item.getType() == Type.TEXT || item.getType() == Type.STRING) {
            LOG.debug("Streaming text");
            IOUtils.write(item.getStringValue(), os, "UTF-8");
            
        } else {
            LOG.error("Wrong item type " + Type.getTypeName(item.getType()));
            throw new XPathException( "wrong item type " + Type.getTypeName(item.getType()));
        }

    }

}
