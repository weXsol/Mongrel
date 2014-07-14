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
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.gridfs.GridFSInputFile;
import java.util.List;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.xml.datatype.Duration;
import org.apache.commons.lang3.StringUtils;
import org.exist.dom.QName;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
import org.exist.messaging.shared.Report;
import org.exist.mongodb.shared.Constants;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.DateTimeValue;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
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
            new FunctionParameterSequenceType("id", Type.STRING, Cardinality.ONE, "Mongo driver id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),
            new FunctionParameterSequenceType("collection", Type.STRING, Cardinality.ONE, "Collection"),
            new FunctionParameterSequenceType("filename", Type.STRING, Cardinality.ONE, "Name of document"),
            new FunctionParameterSequenceType("contentType", Type.STRING, Cardinality.ONE, "COntent type"),
            new FunctionParameterSequenceType("content", Type.STRING, Cardinality.ONE, "Content TODO")
        },
        new FunctionReturnSequenceType(Type.ELEMENT, Cardinality.ONE, "an ID")
        ),};

    public Store(XQueryContext context, FunctionSignature signature) {
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

        // Get parameters
        String id = args[0].itemAt(0).getStringValue();
        String dbname = args[1].itemAt(0).getStringValue();
        String collection = args[2].itemAt(0).getStringValue();
        String documentName = args[3].itemAt(0).getStringValue();
        String contentType = args[4].itemAt(0).getStringValue();
        String content = args[5].itemAt(0).getStringValue();

        // Get appropriate Mongodb client
        MongoClient client = MongodbClientStore.getInstance().get(id);

        // Get database
        DB db = client.getDB(dbname);

        // Creates a GridFS instance for the specified bucket
        GridFS gfs = new GridFS(db, collection);

        // Create file entry
        GridFSInputFile gfsFile = gfs.createFile(content.getBytes());

        // Set meta data
        gfsFile.setFilename(documentName);
        gfsFile.setContentType(contentType);

        // Save and persist
        gfsFile.save();

        System.out.println(gfsFile.toString());

        // Report identifier
        return getReport(gfsFile);

    }

    NodeImpl getReport(GridFSFile gfsFile) throws XPathException {

        MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        // start root element
        int nodeNr = builder.startElement("", "GridFSFile", "GridFSFile", null);

        // Some identities
        add(builder, "id", "" + gfsFile.getId());
        add(builder, "filename", gfsFile.getFilename());
        
        List<String> aliases = gfsFile.getAliases();
        if (aliases != null) {
            for (String alias : gfsFile.getAliases()) {
                add(builder, "alias", alias);
            }
        }

        // mimetype
        add(builder, "contentType",gfsFile.getContentType());
        
        // sizes
        add(builder, "length", "" + gfsFile.getLength());
        add(builder, "chunkSize", "" + gfsFile.getChunkSize());
        add(builder, "numberOfChunks", "" + gfsFile.numChunks());
        
        // more meta data
        add(builder, "uploadDate", "" + (new DateTimeValue(gfsFile.getUploadDate()).getStringValue()));
        add(builder, "md5", gfsFile.getMD5());
        
        // finish root element
        builder.endElement();

        // return result
        return ((DocumentImpl) builder.getDocument()).getNode(nodeNr);
    }

    private void add(MemTreeBuilder builder, String elementName, String value) {

        if (StringUtils.isNotBlank(value) && StringUtils.isNotBlank(elementName)) {
            builder.startElement("", elementName, elementName, null);
            builder.characters(value);
            builder.endElement();
        }

    }
}
