package arangoDB;

/**
 * Created by GreatYYX on 10/5/15.
 */
public interface ArangoDbClientConstants {

    public static final String ARANGODB_URL_PROPERTY = "arangodb.url";
    public static final String ARANGODB_DB_PROPERTY = "arangodb.database";

    public static final String ARANGODB_FS_PATH = "arangodb.fspath";
    public static final String ARANGODB_FS_IMAGE_FOLDER = "images";
    public static final String ARANGODB_FS_THUMB_FOLDER = "thumbs";
    public static final String ARANGODB_FS_DUMP_FOLDER = "dump";

    public static enum ArangoDbValueType {
        STRING, NUMBER, BOOLEAN
    }
}
