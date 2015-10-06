package arangoDB;

import com.arangodb.entity.IndexType;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;

import com.arangodb.ArangoHost;
import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.util.MapBuilder;

import java.util.*;

/**
 * Created by GreatYYX on 10/5/15.
 */
public class ArangoDbClient extends DB implements ArangoDbClientConstants {

    private static ArangoDriver arango;
    private static Properties props;
    private String database;

    @Override
    public boolean init() throws DBException {

        // get parameters
        props = getProperties();
        database = props.getProperty(ARANGODB_DB_PROPERTY);
        if (database.equals(null) || database.equals("")) {
            System.out.println("Error Property: " + ARANGODB_DB_PROPERTY);
            return false;
        }
        String url = props.getProperty(ARANGODB_URL_PROPERTY);
        if (url.equals(null) || !url.contains(":")) {
            System.out.println("Error Property: " + ARANGODB_URL_PROPERTY);
            return false;
        }
        String host = url.split(":")[0];
        int port = Integer.parseInt(url.split(":")[1]);

        // db configuration
        ArangoConfigure configure = new ArangoConfigure();
        configure.setArangoHost(new ArangoHost(host, port));
        configure.init();

        // connect db
        arango = new ArangoDriver(configure, database);

        return true;
    }

    @Override
    public void createSchema(Properties props) {

        // delete exists
        try {
            arango.deleteCollection("users");
            arango.deleteCollection("resources");
//            arango.deleteCollection("manipulation");
        } catch (Exception e) {
            // ignore exceptions
        }

        // create collections
        try {
            CollectionEntity collection;
            collection = arango.createCollection("users");
            collection = arango.createCollection("resources");
//            arango.createCollection("manipulation");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup(boolean warmup) throws DBException {
        if (!warmup) {
            //
        }
    }

    @Override
    public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
        try {
            CollectionEntity collection = arango.getCollection(entitySet);

            BaseDocument docObj = new BaseDocument();
            docObj.setDocumentKey(entityPK);
            for (String k: values.keySet()) {
                if(!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic"))) {
                    docObj.addAttribute(k, values.get(k).toString());
                }
            }

            // add two attributes for collection "users"
            if (entitySet.equalsIgnoreCase("users")) {
                docObj.addAttribute("ConfFriends", new ArrayList<Integer>());
                docObj.addAttribute("PendFriends", new ArrayList<Integer>());
            }

            // add "manipulations" for "resources"
            if (entitySet.equalsIgnoreCase("resources")) {
                docObj.addAttribute("manipulations", new ArrayList<CollectionEntity>());
            }

            // insert doc
            arango.createDocument(entitySet, docObj);

        } catch (Exception e) {
            System.out.println(e.toString());
            return -1;
        }

        return 0;
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
        return 0;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
        return 0;
    }

    @Override
    public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {
        return 0;
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {
        return 0;
    }

    @Override
    public int rejectFriend(int inviterID, int inviteeID) {
        return 0;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {
        return 0;
    }

    @Override
    public int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String, ByteIterator> values) {
        return 0;
    }

    @Override
    public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
        return 0;
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2) {
        return 0;
    }

    @Override
    public HashMap<String, String> getInitialStats() {
        return null;
    }

    @Override
    public int CreateFriendship(int friendid1, int friendid2) {
        return 0;
    }

    @Override
    public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
        return 0;
    }

    @Override
    public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
        return 0;
    }
}
