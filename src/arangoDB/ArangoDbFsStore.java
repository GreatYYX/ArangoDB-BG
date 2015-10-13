package arangoDB;

import org.apache.commons.io.FileUtils;

import java.io.*;

/**
 * Created by GreatYYX on 10/12/15.
 */
public class ArangoDbFsStore {

    private String path;

    public ArangoDbFsStore(String rootPath, String folderPath) throws IOException {
        if (rootPath == null || folderPath == null) {
            throw new IOException("Error: Filesystem store path.");
        }
        path = rootPath + File.separator + folderPath;
    }

    public void createFile(String name, byte[] data) throws IOException {
        String fileName = path + File.separator + name;
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
        out.write(data);
        out.close();
    }

    public void initFolder() throws IOException {
        File f = new File(path);
        FileUtils.deleteDirectory(f);
        FileUtils.forceMkdir(f);
    }

}
