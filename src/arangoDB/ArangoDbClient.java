package arangoDB;

import com.arangodb.*;
import com.arangodb.entity.IndexType;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.util.MapBuilder;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by GreatYYX on 10/5/15.
 */
public class ArangoDbClient extends DB implements ArangoDbClientConstants {

    private static ArangoDriver arango;
    private static Properties props;
    private String database;
    private static AtomicInteger NumThreads = null;
    private static Semaphore crtcl = new Semaphore(1, true);

    private static int incrementNumThreads() {
        int v;
        do {
            v = NumThreads.get();
        } while (!NumThreads.compareAndSet(v, v + 1));
        return v + 1;
    }

    private static int decrementNumThreads() {
        int v;
        do {
            v = NumThreads.get();
        } while (!NumThreads.compareAndSet(v, v - 1));
        return v - 1;
    }

    @Override
    public boolean init() throws DBException {

        // get parameters
        props = getProperties();
        database = props.getProperty(ARANGODB_DB_PROPERTY);
        if (database == null || database.equals("")) {
            System.out.println("Error Property: " + ARANGODB_DB_PROPERTY);
            return false;
        }
        String url = props.getProperty(ARANGODB_URL_PROPERTY);
        if (url == null || !url.contains(":")) {
            System.out.println("Error Property: " + ARANGODB_URL_PROPERTY);
            return false;
        }
        String host = url.split(":")[0];
        int port = Integer.parseInt(url.split(":")[1]);

        try {
            crtcl.acquire();

            if (NumThreads == null) {
                NumThreads = new AtomicInteger();
                NumThreads.set(0);

                // db configuration
                ArangoConfigure configure = new ArangoConfigure();
                configure.setArangoHost(new ArangoHost(host, port));
                configure.init();

                // connect db
                arango = new ArangoDriver(configure, database);
            }
            incrementNumThreads();
        } catch(Exception e) {
            System.out.println("Failed to acquire semaphore.");
            e.printStackTrace();
        } finally {
            crtcl.release();
        }

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
            try {
                crtcl.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            decrementNumThreads();

            // add instance to vector of connections
            if (NumThreads.get() > 0) {
                crtcl.release();
                return;
            } else {
                // close all connections in vector
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(arango != null) arango = null;
                crtcl.release();
            }
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

            // add specific attributes for collection "users"
            if (entitySet.equalsIgnoreCase("users")) {
                docObj.addAttribute("uid", Integer.parseInt(entityPK));
                docObj.addAttribute("ConfFriends", new ArrayList<Integer>());
                docObj.addAttribute("PendFriends", new ArrayList<Integer>());
            }

            // add specific attributes for collection "resources"
            if (entitySet.equalsIgnoreCase("resources")) {
                docObj.addAttribute("rid", Integer.parseInt(entityPK));
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
    public int CreateFriendship(int friendid1, int friendid2) {
        return acceptFriend(friendid1, friendid2);
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {

        if(inviterID < 0 || inviteeID < 0)
            return -1;

        try {
            String strInviterID = Integer.toString(inviterID);
            String strInviteeID = Integer.toString(inviteeID);
            BaseDocument docObjUpdate = null;
            ArrayList<Integer> pendFriends = null;
            ArrayList<Integer> confFriends = null;

            // remove invitor id from invitees pending list
            DocumentEntity<BaseDocument> docInvitee = arango.getDocument("users", strInviteeID, BaseDocument.class);
            docObjUpdate = docInvitee.getEntity();
            pendFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("PendFriends");
            if (pendFriends.contains(inviterID)) {
                pendFriends.remove(inviterID);
                docObjUpdate.updateAttribute("PendFriends", pendFriends);
                arango.updateDocument(docInvitee.getDocumentHandle(), docObjUpdate);
            }

            // add invitor id to invitee confirmed list
            docObjUpdate = docInvitee.getEntity();
            confFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("ConfFriends");
            confFriends.add(inviterID);
            docObjUpdate.updateAttribute("ConfFriends", confFriends);
            arango.updateDocument(docInvitee.getDocumentHandle(), docObjUpdate);

            // add invitee id to inviter confirmed list
            DocumentEntity<BaseDocument> docInviter = arango.getDocument("users", strInviterID, BaseDocument.class);
            docObjUpdate = docInviter.getEntity();
            confFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("ConfFriends");
            confFriends.add(inviteeID);
            docObjUpdate.updateAttribute("ConfFriends", confFriends);
            arango.updateDocument(docInviter.getDocumentHandle(), docObjUpdate);

        } catch (Exception e) {
            System.out.println(e.toString());
            return -1;
        }

        return 0;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {

        if(inviterID < 0 || inviteeID < 0)
            return -1;

        try {
            String strInviteeID = Integer.toString(inviteeID);
            BaseDocument docObjUpdate = null;
            ArrayList<Integer> pendFriends = null;

            // add pending for invitee
            DocumentEntity<BaseDocument> docInvitee = arango.getDocument("users", strInviteeID, BaseDocument.class);
            docObjUpdate = docInvitee.getEntity();
            pendFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("PendFriends");
            pendFriends.add(inviterID);
            docObjUpdate.updateAttribute("PendFriends", pendFriends);
            arango.updateDocument(docInvitee.getDocumentHandle(), docObjUpdate);

        } catch (Exception e) {
            System.out.println(e.toString());
            return -1;
        }
        return 0;
    }

    @Override
    public HashMap<String, String> getInitialStats() {

        HashMap<String, String> stats = new HashMap<String, String>();
        CollectionEntity collection = null;
        String query = null;
        DocumentCursor<BaseDocument> docCursor = null;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        ArrayList<Integer> arrList = null;

        try {
            // user count
            query = "FOR u IN users RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions().setCount(true), BaseDocument.class);
            int resUserCnt = docCursor.getCount();
            stats.put("usercount", Integer.toString(resUserCnt));

            // user offset
            String userOffset = "0";
            query = "FOR u IN users SORT u.uid LIMIT 1 RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions(), BaseDocument.class);
            resList = docCursor.asList();
            if (resList.size() == 1) {
                userOffset = resList.get(0).getEntity().getDocumentKey();
            }

            // average friends per user & average pending friend per user
            query = "FOR u IN users FILTER u._key==\"1\" RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions(), BaseDocument.class);
            resList = docCursor.asList();
            docObj = resList.get(0).getEntity();
            arrList = (ArrayList<Integer>)docObj.getAttribute("ConfFriends");
            int resAvgConf = arrList.size();
            stats.put("avgfriendsperuser", Integer.toString(resAvgConf));
            arrList = (ArrayList<Integer>)docObj.getAttribute("PendFriends");
            int resAvgPend = arrList.size();
            stats.put("avgpendingperuser", Integer.toString(resAvgPend));

            // resources per user
            query = "FOR r IN resources FILTER r.creatorid==\"" + userOffset + "\" RETURN r";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions().setCount(true), BaseDocument.class);
            int resPerUser = docCursor.getCount();
            stats.put("resourcesperuser", Integer.toString(resPerUser));

        } catch (ArangoException e) {
            e.printStackTrace();
        }


        return stats;
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
    public int rejectFriend(int inviterID, int inviteeID) {
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
    public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
        return 0;
    }

    @Override
    public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
        return 0;
    }
}
