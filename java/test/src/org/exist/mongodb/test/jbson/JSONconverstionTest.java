package org.exist.mongodb.test.jbson;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests to understand the J(B)SON parser classes.
 * 
 * @author Dannes Wessels
 */
public class JSONconverstionTest {


    @Test
    public void simpleObject() {
        BasicDBObject obj = (BasicDBObject) JSON.parse("{'firstName':'John', 'lastName':'Doe'}");

        // Verify keys & values via key set
        assertArrayEquals(new String[]{"firstName", "lastName"}, obj.keySet().toArray());
        assertEquals("John", obj.getString("firstName"));
        assertEquals("Doe", obj.getString("lastName"));
        
        // Verify via iterator
        Iterator<Map.Entry<String, Object>> iterator = obj.entrySet().iterator();
        
        // First entry
        Map.Entry<String, Object> next = iterator.next();   
        assertEquals("firstName", next.getKey());
        assertEquals("John", next.getValue());
        
        // Seconf entry
        next = iterator.next();   
        assertEquals("lastName", next.getKey());
        assertEquals("Doe", next.getValue());
    }

    @Test
    public void date() {
        BasicDBObject bdbo = (BasicDBObject) JSON.parse("{'date': { '$date' : '2014-02-12T15:28:31Z'} }");
        
        // compare with epoch value
        assertEquals(1392218911000L, bdbo.getDate("date").getTime());
    }

    @Test
    public void simpleMap() {
        BasicDBObject obj = (BasicDBObject) JSON.parse("{ 'aa' : { 'bb' : 'cc' , 'dd' : 'ee'} }");

        // Verify first key
        assertEquals("aa", obj.keySet().toArray()[0]);

        // Get value of 'aa'
        BasicDBObject aa = (BasicDBObject) obj.get("aa");
        
        // Verify keys (better to use iterator) - bonus
        assertArrayEquals(new String[]{"bb", "dd"}, aa.keySet().toArray());       
        
        // Prepare iteration over content 'aa'   
        Iterator<Map.Entry<String, Object>> iterator = aa.entrySet().iterator();
        
        // First entry
        Map.Entry<String, Object> next = iterator.next();   
        assertEquals("bb", next.getKey());
        assertEquals("cc", next.getValue());
        
        // Second entry
        next = iterator.next();   
        assertEquals("dd", next.getKey());
        assertEquals("ee", next.getValue());
    }
}
