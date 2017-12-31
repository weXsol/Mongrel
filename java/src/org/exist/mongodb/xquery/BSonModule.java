package org.exist.mongodb.xquery;

import org.exist.dom.QName;
import org.exist.mongodb.xquery.bson.Parse;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

import java.util.List;
import java.util.Map;


public class BSonModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/mongrel/bson";
    public final static String PREFIX = "bson";
    public final static String INCLUSION_DATE = "2015-02-08";
    public final static String RELEASED_IN_VERSION = "eXist-2.3";

    public final static FunctionDef[] functions = {
            new FunctionDef(Parse.signatures[0], Parse.class),
            new FunctionDef(Parse.signatures[1], Parse.class)
    };

    public final static ErrorCode MONG0001 = new MongodbErrorCode("MONG0001", "Forbidden");
    public final static ErrorCode MONG0002 = new MongodbErrorCode("MONG0002", "Mongodb exception");
    public final static ErrorCode MONG0003 = new MongodbErrorCode("MONG0003", "Generic exception");
    public final static ErrorCode MONG0004 = new MongodbErrorCode("MONG0004", "JSON Syntax exception");
    public final static ErrorCode MONG0005 = new MongodbErrorCode("MONG0005", "Command exception");

    public final static QName EXCEPTION_QNAME
            = new QName("exception", BSonModule.NAMESPACE_URI, BSonModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", BSonModule.NAMESPACE_URI, BSonModule.PREFIX);

    public BSonModule(Map<String, List<?>> parameters) {
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
        return "BSon parser module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }

    protected final static class MongodbErrorCode extends ErrorCode {

        public MongodbErrorCode(String code, String description) {
            super(new QName(code, NAMESPACE_URI, PREFIX), description);
        }

    }
}
