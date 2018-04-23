package org.exist.mongodb.test.bson;

import org.apache.commons.lang3.StringUtils;
import xquery.TestRunner;

public class ParserTests extends TestRunner {

    @Override
    protected String getDirectory() {
        
        ClassLoader loader = this.getClass().getClassLoader();
        String className = this.getClass().getCanonicalName().replaceAll("\\.", "\\/");
        
        String fullPath = loader.getResource(className + ".class").getFile();
        String directory = StringUtils.substringBeforeLast(fullPath, "/");
          
        return directory;
    }
}
