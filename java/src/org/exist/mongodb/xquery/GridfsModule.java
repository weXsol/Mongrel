
package org.exist.mongodb.xquery;

import java.util.List;
import java.util.Map;
import org.exist.dom.QName;
import org.exist.mongodb.xquery.gridfs.Close;
import org.exist.mongodb.xquery.gridfs.Connect;
import org.exist.mongodb.xquery.gridfs.Store;
import org.exist.mongodb.xquery.gridfs.Stream;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

/**
 *
 * @author wessels
 */


public class GridfsModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/gridfs";
    public final static String PREFIX = "gridfs";
    public final static String INCLUSION_DATE = "2013-11-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";

    public final static FunctionDef[] functions = {
        new FunctionDef(Connect.signatures[0], Connect.class),
        new FunctionDef(Close.signatures[0], Close.class),
        new FunctionDef(org.exist.mongodb.xquery.gridfs.List.signatures[0], org.exist.mongodb.xquery.gridfs.List.class),
        new FunctionDef(Store.signatures[0], Store.class),
        new FunctionDef(Stream.signatures[0], Stream.class),
        new FunctionDef(Stream.signatures[1], Stream.class)

    };
    
    public final static ErrorCode GRFS0001 = new GridfsErrorCode("GRFS0001", "Document not found");


    public final static QName EXCEPTION_QNAME
            = new QName("exception", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public GridfsModule(Map<String, List<? extends Object>> parameters) throws XPathException {
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
