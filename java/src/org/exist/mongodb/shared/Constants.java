
package org.exist.mongodb.shared;

/**
 *
 * @author wessels
 */


public class Constants {

    public static final String MONGODB_GROUP = "mongodb";
    
    public static final String GZIP = "gzip";
    
    public static final String EXIST_ORIGINAL_MD5 = "exist_original_md5";
    public static final String EXIST_DATATYPE_TEXT = "exist_datatype_text";
    public static final String EXIST_DATATYPE = "exist_datatype";   
    public static final String EXIST_COMPRESSION = "exist_compression";
    public static final String EXIST_ORIGINAL_SIZE = "exist_original_size";
    
    public static final String CONTENT_DISPOSITION = "Content-Disposition";
    public static final String CONTENT_LENGTH = "Content-Length";
    
    public static final String PARAM_MONGODB_CLIENT_ID = "mongodbClientId";
    public static final String DESCR_MONGODB_CLIENT_ID = "MongoDB client id";
    
    public static final String PARAM_DATABASE = "database";
    public static final String DESCR_DATABASE = "Name of database";
    
    public static final String PARAM_BUCKET = "bucket";
    public static final String DESCR_BUCKET = "Name of bucket";
        
    public static final String PARAM_OBJECT_ID = "objectid";
    public static final String DESCR_OBJECT_ID = "ObjectID of document";

    public static final String PARAM_FILENAME = "filename";
    public static final String DESCR_FILENAME = "Filename of document";
    
    public static final String PARAM_CONTENT = "content";
    public static final String DESCR_CONTENT = "Document content as node() or  base64-binary";
     
    public static final String PARAM_CONTENT_TYPE = "contentType";
    public static final String DESCR_CONTENT_TYPE = "Document Content type, use () for mime-type based on file extension";
    
    public static final String PARAM_AS_ATTACHMENT = "as-attachment";
    public static final String DESCR_AS_ATTACHMENT = "Add content-disposition header";
    
    public static final String DESCR_OUTPUT_STREAM = "Servlet output stream";
}
