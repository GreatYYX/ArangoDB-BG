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

    public void writeFile(String name, byte[] data) throws IOException {
        String fileName = path + File.separator + name;
//        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
//        out.write(data);
//        out.close();
        File f = new File(fileName);
        FileUtils.writeByteArrayToFile(f, data);
    }

    public byte[] readFile(String name) throws IOException {
        String fileName = path + File.separator + name;
        File f = new File(fileName);
        byte[] data = FileUtils.readFileToByteArray(f);
        return data;
    }

    public void initFolder() throws IOException {
        File f = new File(path);
        FileUtils.deleteDirectory(f);
        FileUtils.forceMkdir(f);
    }

}
