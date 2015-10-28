package arangoDB;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

import com.arangodb.*;
import com.arangodb.entity.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by GreatYYX on 10/5/15.
 */
public class ArangoDbClient extends DB implements ArangoDbClientConstants {

    public static final int SUCCESS = 0;
    public static final int ERROR = -1;

    private static ArangoDriver arango;
    private static ArangoConfigure arangoCfg;
    private static Properties props;
    private String database;
    private String storeRoot;
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
                arangoCfg = new ArangoConfigure();
                arangoCfg.setArangoHost(new ArangoHost(host, port));
                arangoCfg.init();

                // connect db
                arango = new ArangoDriver(arangoCfg, database);
            }
            incrementNumThreads();
        } catch(Exception e) {
            System.out.println("Failed to acquire semaphore.");
            e.printStackTrace();
        } finally {
            crtcl.release();
        }

        storeRoot = props.getProperty(ARANGODB_FS_PATH);

        return true;
    }

    @Override
    public void createSchema(Properties props) {

        try {
            // delete existent collections
            try {
                arango.deleteCollection("users");
            } catch (Exception e) {}
            try {
                arango.deleteCollection("resources");
            } catch (Exception e) {}

            // create collections
            arango.createCollection("users");
            arango.createCollection("resources");
            arango.createIndex("resources", IndexType.HASH, false, "walluserid");

            // initialize filesystem
            String fsPath = props.getProperty(ARANGODB_FS_PATH);
            if (fsPath != null) {
                new ArangoDbFsStore(fsPath, ARANGODB_FS_IMAGE_FOLDER).initFolder();
                new ArangoDbFsStore(fsPath, ARANGODB_FS_THUMB_FOLDER).initFolder();
            }

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

            // close instance to vector of connections
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
                arangoCfg.shutdown();
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
            docObj.setDocumentKey(entityPK); // create _key
            int intEntityPK = Integer.parseInt(entityPK); // _key in integer

            // add attributes
            for (String k: values.keySet()) {
                if(!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic"))) {
                    docObj.addAttribute(k, values.get(k).toString());
                }
            }

            // add specific attributes for collection "users"
            if (entitySet.equalsIgnoreCase("users")) {
                docObj.addAttribute("uid", intEntityPK);
                docObj.addAttribute("ConfFriends", new ArrayList<Integer>());
                docObj.addAttribute("PendFriends", new ArrayList<Integer>());

                // image
                if(insertImage) {

                    try {
                        // profile
                        byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
                        ArangoDbFsStore fs = new ArangoDbFsStore(storeRoot, ARANGODB_FS_IMAGE_FOLDER);
                        fs.writeFile(entityPK, profileImage);
                        docObj.addAttribute("imageid", intEntityPK);

                        // thumbnail
                        byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
                        ArangoDbFsStore fsThumb = new ArangoDbFsStore(storeRoot, ARANGODB_FS_THUMB_FOLDER);
                        fsThumb.writeFile(entityPK, thumbImage);
                        docObj.addAttribute("thumbid", intEntityPK);
                    } catch (IOException e) {
                        System.out.println(e.toString());
                        return ERROR;
                    }
                }
            }

            // add specific attributes for collection "resources"
            if (entitySet.equalsIgnoreCase("resources")) {
                docObj.addAttribute("rid", intEntityPK);
                docObj.addAttribute("manipulations", new ArrayList<CollectionEntity>());
            }

            // insert doc
            arango.createDocument(entitySet, docObj);

        } catch (Exception e) {
            System.out.println(e.toString());
            return ERROR;
        }

        return SUCCESS;
    }

    @Override
    public int CreateFriendship(int friendid1, int friendid2) {
        return acceptFriend(friendid1, friendid2);
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {

        if(inviterID < 0 || inviteeID < 0)
            return ERROR;

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
            return ERROR;
        }

        return SUCCESS;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {

        if(inviterID < 0 || inviteeID < 0)
            return ERROR;

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
            return ERROR;
        }
        return SUCCESS;
    }

    @Override
    public HashMap<String, String> getInitialStats() {

        HashMap<String, String> stats = new HashMap<String, String>();
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
            query = "FOR u IN users FILTER u._key==\"" + userOffset + "\" RETURN u";
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
    public int rejectFriend(int inviterID, int inviteeID) {

        if(inviterID < 0 || inviteeID < 0)
            return ERROR;

        try {
            String strInviteeID = Integer.toString(inviteeID);
            BaseDocument docObjUpdate = null;
            ArrayList<Integer> pendFriends = null;

            //remove from pending of invitee
            DocumentEntity<BaseDocument> docInvitee = arango.getDocument("users", strInviteeID, BaseDocument.class);
            docObjUpdate = docInvitee.getEntity();
            pendFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("PendFriends");
            if (pendFriends.contains(inviteeID)) {
                pendFriends.remove(inviterID);
            }
            docObjUpdate.updateAttribute("PendFriends", pendFriends);
            arango.updateDocument(docInvitee.getDocumentHandle(), docObjUpdate);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }

        return SUCCESS;
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2) {

        if(friendid1 < 0 || friendid2 < 0)
            return ERROR;

        try {
            String strId1 = Integer.toString(friendid1);
            String strId2 = Integer.toString(friendid2);
            BaseDocument docObjUpdate = null;
            ArrayList<Integer> confFriends = null;

            // delete friend2 from user1
            DocumentEntity<BaseDocument> docUser1 = arango.getDocument("users", strId1, BaseDocument.class);
            docObjUpdate = docUser1.getEntity();
            confFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("ConfFriends");
            if (confFriends.contains(friendid1)) {
                confFriends.remove(friendid1);
            }
            docObjUpdate.updateAttribute("ConfFriends", confFriends);
            arango.updateDocument(docUser1.getDocumentHandle(), docObjUpdate);

            // delete friend1 from user2
            DocumentEntity<BaseDocument> docUser2 = arango.getDocument("users", strId2, BaseDocument.class);
            docObjUpdate = docUser2.getEntity();
            confFriends = (ArrayList<Integer>)docObjUpdate.getAttribute("ConfFriends");
            if (confFriends.contains(friendid2)) {
                confFriends.remove(friendid2);
            }
            docObjUpdate.updateAttribute("ConfFriends", confFriends);
            arango.updateDocument(docUser2.getDocumentHandle(), docObjUpdate);

        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }

        return SUCCESS;
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {

        if (requesterID < 0 || profileOwnerID < 0)
            return ERROR;

        String query;
        DocumentCursor docCursor;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        ArrayList<Integer> arrList = null;
        String strProfileOwnerID = Integer.toString(profileOwnerID);

        try {
            // count of confirmed friends & pending friends (only when requesting by himself)
            query = "FOR u IN users FILTER u._key==\"" + strProfileOwnerID + "\" RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions(), BaseDocument.class);
            resList = docCursor.asList();
            docObj = resList.get(0).getEntity();
            arrList = (ArrayList<Integer>)docObj.getAttribute("ConfFriends");
            int confCount = arrList.size();
            result.put("friendcount", new ObjectByteIterator(Integer.toString(confCount).getBytes()));
            if (requesterID == profileOwnerID) {
                arrList = (ArrayList<Integer>)docObj.getAttribute("PendFriends");
                int pendCount = arrList.size();
                result.put("pendingcount", new ObjectByteIterator(Integer.toString(pendCount).getBytes()));
            }

            // read profile details:
            int uid = new Double((Double)docObj.getAttribute("uid")).intValue();
            result.put("uid",       new ObjectByteIterator(Integer.toString(uid).getBytes()));
            result.put("username",  new ObjectByteIterator(((String)docObj.getAttribute("username")).getBytes()));
            result.put("fname",     new ObjectByteIterator(((String)docObj.getAttribute("fname")).getBytes()));
            result.put("lname",     new ObjectByteIterator(((String)docObj.getAttribute("lname")).getBytes()));
            result.put("gender",    new ObjectByteIterator(((String)docObj.getAttribute("gender")).getBytes()));
            result.put("dob",       new ObjectByteIterator(((String)docObj.getAttribute("dob")).getBytes()));
            result.put("jdate",     new ObjectByteIterator(((String)docObj.getAttribute("jdate")).getBytes()));
            result.put("ldate",     new ObjectByteIterator(((String)docObj.getAttribute("ldate")).getBytes()));
            result.put("address",   new ObjectByteIterator(((String)docObj.getAttribute("address")).getBytes()));
            result.put("email",     new ObjectByteIterator(((String)docObj.getAttribute("email")).getBytes()));
            result.put("tel",       new ObjectByteIterator(((String)docObj.getAttribute("tel")).getBytes()));

            // count of resources
            query = "FOR r IN resources FILTER r.walluserid==\"" + strProfileOwnerID + "\" RETURN r";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions().setCount(true), BaseDocument.class);
            int resCount = docCursor.getCount();
            result.put("resourcecount", new ObjectByteIterator(Integer.toString(resCount).getBytes()));

            // handle images
            if(insertImage){
                ArangoDbFsStore fs = new ArangoDbFsStore(storeRoot, ARANGODB_FS_IMAGE_FOLDER);
                byte[] profileImage = fs.readFile(strProfileOwnerID);
                if (testMode) {
                    // dump to file
//                    try {
//                        FileOutputStream fos = new FileOutputStream(strProfileOwnerID + "-proimage.bmp");
//                        fos.write(profileImage);
//                        fos.close();
//                    } catch (Exception ex) {
//                    }
                }
                result.put("pic", new ObjectByteIterator(profileImage));
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }

        return SUCCESS;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

        return SUCCESS;

    }

    @Override
    public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {
        
        return SUCCESS;
    }

    @Override
    public int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String, ByteIterator>> result) {
        if(profileOwnerID < 0 || requesterID < 0 || k < 0)
            return ERROR;

        String query;
        DocumentCursor docCursor;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        String strProfileOwnerID = Integer.toString(profileOwnerID);
        try {
            query = "FOR r IN resources FILTER r.walluserid==\"" + strProfileOwnerID + "\" SORT r.rid DESC LIMIT " + Integer.toString(k)
                    + " RETURN r";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions().setCount(true), BaseDocument.class);
            resList = docCursor.asList();
            Integer i = 0;
            while (i < resList.size()){
                HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
                docObj = resList.get(i).getEntity();
                vals.put("rid",  new ObjectByteIterator(((String)docObj.getAttribute("_key")).getBytes()));
                vals.put("walluserid",     new ObjectByteIterator(((String)docObj.getAttribute("walluserid")).getBytes()));
                vals.put("creatorid",     new ObjectByteIterator(((String)docObj.getAttribute("creatorid")).getBytes()));
                vals.put("doc",    new ObjectByteIterator(((String)docObj.getAttribute("doc")).getBytes()));
                vals.put("type",       new ObjectByteIterator(((String)docObj.getAttribute("type")).getBytes()));
                vals.put("body",     new ObjectByteIterator(((String)docObj.getAttribute("body")).getBytes()));
                i ++;
                result.add(vals);
            }

        } catch (ArangoException e) {
            e.printStackTrace();
            return ERROR;
        }
        return SUCCESS;
    }

    @Override
    public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
        if(creatorID < 0)
            return ERROR;

        String query;
        DocumentCursor docCursor;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        String strCreatorID = Integer.toString(creatorID);
        try {
            query = "FOR r IN resources FILTER r.creatorID==\"" + strCreatorID + "\" RETURN r";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions().setCount(true), BaseDocument.class);
            resList = docCursor.asList();
            Integer i = 0;
            while (i < resList.size()){
                HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
                docObj = resList.get(i).getEntity();
                vals.put("rid",  new ObjectByteIterator(((String)docObj.getAttribute("_key")).getBytes()));
                vals.put("walluserid",     new ObjectByteIterator(((String)docObj.getAttribute("walluserid")).getBytes()));
                vals.put("creatorid",     new ObjectByteIterator(((String)docObj.getAttribute("creatorid")).getBytes()));
                vals.put("doc",    new ObjectByteIterator(((String)docObj.getAttribute("doc")).getBytes()));
                vals.put("type",       new ObjectByteIterator(((String)docObj.getAttribute("type")).getBytes()));
                vals.put("body",     new ObjectByteIterator(((String)docObj.getAttribute("body")).getBytes()));
                i ++;
                result.add(vals);
            }

        } catch (ArangoException e) {
            e.printStackTrace();
            return ERROR;
        }
        return SUCCESS;
    }

    @Override
    public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String, ByteIterator>> result) {
        return SUCCESS;
    }

    @Override
    public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String, ByteIterator> values) {
        return SUCCESS;
    }

    @Override
    public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
        return SUCCESS;
    }

    @Override
    public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
        if (memberID < 0)
            return ERROR;
        String query;
        DocumentCursor docCursor;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        ArrayList<Integer> arrList = null;
        String strMemberID = Integer.toString(memberID);
        try {
            query = "FOR u IN users FILTER u._key==\"" + strMemberID + "\" RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions(), BaseDocument.class);
            resList = docCursor.asList();
            if (resList.size() > 0){
                docObj = resList.get(0).getEntity();
                arrList = (ArrayList<Integer>) docObj.getAttribute("PendFriends");
                int count = 0;
                while (count < arrList.size()){
                    pendingIds.add(arrList.get(count));
                    count ++;

                }
            }
        } catch (ArangoException e) {
            e.printStackTrace();
            return ERROR;
        }
        return SUCCESS;
    }

    @Override
    public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
        if (memberID < 0)
            return ERROR;
        String query;
        DocumentCursor docCursor;
        BaseDocument docObj = null;
        List<DocumentEntity<BaseDocument>> resList = null;
        ArrayList<Integer> arrList = null;
        String strMemberID = Integer.toString(memberID);
        try {
            query = "FOR u IN users FILTER u._key==\"" + strMemberID + "\" RETURN u";
            docCursor = arango.executeDocumentQuery(
                    query, null, arango.getDefaultAqlQueryOptions(), BaseDocument.class);
            resList = docCursor.asList();
            if (resList.size() > 0) {
                docObj = resList.get(0).getEntity();
                arrList = (ArrayList<Integer>) docObj.getAttribute("ConfFriends");
                int count = 0;
                while (count < arrList.size()) {
                    confirmedIds.add(arrList.get(count));
                    count++;
                }
            }
        } catch (ArangoException e) {
            e.printStackTrace();
            return ERROR;
        }
        return SUCCESS;
    }
}
