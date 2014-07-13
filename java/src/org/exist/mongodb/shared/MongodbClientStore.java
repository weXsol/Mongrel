package org.exist.mongodb.shared;

import com.mongodb.MongoClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author wessels
 */
public class MongodbClientStore {

    private static MongodbClientStore instance = null;

    public static synchronized MongodbClientStore getInstance() {
        if (instance == null) {
            instance = new MongodbClientStore();
        }
        return instance;
    }

    private final Map<String, MongoClient> clients = new HashMap();

    public void add(String id, MongoClient client) {
        clients.put(id, client);
    }

    public void remove(String id) {
        clients.remove(id);
    }

    public Set<String> list() {
        return clients.keySet();
    }

    public MongoClient get(String id) {
        return clients.get(id);
    }
}
