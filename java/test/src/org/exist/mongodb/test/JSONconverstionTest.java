package org.exist.mongodb.test;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author wessels
 */
public class JSONconverstionTest {

    public JSONconverstionTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void simpleObject() {

        BasicDBObject bdbo = (BasicDBObject) JSON.parse("{'firstName':'John', 'lastName':'Doe'}");

        assertArrayEquals(new String[]{"firstName", "lastName"}, bdbo.keySet().toArray());
        assertEquals( "John" , bdbo.getString("firstName"));
        assertEquals( "Doe" , bdbo.getString("lastName"));
        
        
    }
    
    @Test
    public void date() {

        BasicDBObject bdbo = (BasicDBObject) JSON.parse("{'date': { '$date' : '2014-02-12T15:28:31Z'} }");

        assertEquals(1392218911000L, bdbo.getDate("date").getTime());
    }
}
