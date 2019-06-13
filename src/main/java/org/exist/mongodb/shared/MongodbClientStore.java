package org.exist.mongodb.shared;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.xquery.XPathException;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.exist.mongodb.xquery.MongodbModule.MONGO_ID;

/**
 * @author wessels
 */
public class MongodbClientStore {

    protected final static Logger LOG = LogManager.getLogger(MongodbClientStore.class);

    private static MongodbClientStore instance = null;
    private final Map<String, MongoClientWrapper> clients = new HashMap<>();

    public static synchronized MongodbClientStore getInstance() {
        if (instance == null) {
            instance = new MongodbClientStore();
        }
        return instance;
    }

    public void add(final String id, final MongoClient client, final String username) {
        final MongoClientWrapper wrapper = new MongoClientWrapper(id, client, username);
        clients.put(id, wrapper);
    }

    public void remove(final String mongodbClientId) {
        clients.remove(mongodbClientId);
    }

    public Set<String> list() {
        return clients.keySet();
    }

    public MongoClient get(final String mongodbClientId) {

        final MongoClientWrapper clientwrapper = clients.get(mongodbClientId);
        if (clientwrapper != null) {
            return clientwrapper.getMongoClient();
        }
        return null;
    }

    public boolean isValid(final String mongodbClientId) {
        return get(mongodbClientId) != null;
    }

    public String create(final String url, final String username) {

        // Construct client
        final MongoClientURI uri = new MongoClientURI(url);
        final MongoClient client = new MongoClient(uri);

        LOG.debug(String.format("client: %s", client));

        // Create unique identifier
        final String mongodbClientId = UUID.randomUUID().toString();

        // Register
        add(mongodbClientId, client, username);

        return mongodbClientId;
    }

    public MongoClient validate(final String mongodbClientId) throws XPathException {
        if (mongodbClientId == null || !isValid(mongodbClientId)) {
            try {
                // introduce a delay
                Thread.sleep(1000L);

            } catch (final InterruptedException ex) {
                LOG.error(ex);
            }
            throw new XPathException(MONGO_ID, null);
        }

        final MongoClientWrapper clientwrapper = clients.get(mongodbClientId);
        if (clientwrapper != null) {
            return clientwrapper.getMongoClient();
        }
        return null;

    }

    class MongoClientWrapper {
        // TODO: use information to make connections tracable

        private String mongodbClientId;
        private MongoClient client;
        private String username;
        private XMLGregorianCalendar calendar;

        public MongoClientWrapper(final String mongodbClientId, final MongoClient client, final String username) {
            this.mongodbClientId = mongodbClientId;
            this.client = client;
            this.username = username;

            try {
                calendar = DatatypeFactory.newInstance().newXMLGregorianCalendar();
            } catch (final DatatypeConfigurationException ex) {
                //
            }
        }

        public String getMongodbClientId() {
            return mongodbClientId;
        }

        public void setMongodbClientId(final String mongodbClientId) {
            this.mongodbClientId = mongodbClientId;
        }

        public MongoClient getMongoClient() {
            return client;
        }

        public void setMongoClient(final MongoClient client) {
            this.client = client;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(final String username) {
            this.username = username;
        }

        public XMLGregorianCalendar getCalendar() {
            return calendar;
        }

        public void setCalendar(final XMLGregorianCalendar calendar) {
            this.calendar = calendar;
        }

    }
}
