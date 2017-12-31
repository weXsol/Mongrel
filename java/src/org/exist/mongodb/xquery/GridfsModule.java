package org.exist.mongodb.xquery;

import org.exist.dom.QName;
import org.exist.mongodb.xquery.gridfs.*;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

import java.util.List;
import java.util.Map;

/**
 * Module for functions to work with a MongoDB GridFS server.
 *
 * @author Dannes Wessels
 */
public class GridfsModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://expath.org/ns/mongo/gridfs";
    public final static String PREFIX = "gridfs";
    public final static String INCLUSION_DATE = "2014-08-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.3";

    public final static FunctionDef[] functions = {
            new FunctionDef(ListBuckets.signatures[0], ListBuckets.class),
            new FunctionDef(ListDocuments.signatures[0], ListDocuments.class),
            new FunctionDef(Properties.signatures[0], Properties.class),
            new FunctionDef(Get.signatures[0], Get.class),
            new FunctionDef(Get.signatures[1], Get.class),
            new FunctionDef(Remove.signatures[0], Remove.class),
            new FunctionDef(Remove.signatures[1], Remove.class),
            new FunctionDef(Store.signatures[0], Store.class),
            new FunctionDef(Stream.signatures[0], Stream.class),
            new FunctionDef(Stream.signatures[1], Stream.class)

    };

    public final static ErrorCode GRFS0002 = MongodbModule.MONG0002; // new GridfsErrorCode("GRFS0002", "Mongodb exception");
    public final static ErrorCode GRFS0003 = MongodbModule.MONG0003; //new GridfsErrorCode("GRFS0003", "Generic exception");
    public final static ErrorCode GRFS0004 = new GridfsErrorCode("GRFS0004", "Document not found");

    public final static QName EXCEPTION_QNAME
            = new QName("exception", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public GridfsModule(Map<String, List<?>> parameters) {
        super(functions, parameters);
    }

    @Override
    public String getNamespaceURI() {
        return NAMESPACE_URI;
    }

    @Override
    public String getDefaultPrefix() {
        return PREFIX;
    }

    @Override
    public String getDescription() {
        return "GridFS module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }

    protected final static class GridfsErrorCode extends ErrorCode {

        public GridfsErrorCode(String code, String description) {
            super(new QName(code, NAMESPACE_URI, PREFIX), description);
        }

    }
}
