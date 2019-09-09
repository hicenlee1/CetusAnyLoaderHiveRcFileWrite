package com.meizu.bigdata.cetus.anyloader.java;

import com.meizu.bigdata.cetus.anyloader.java.model.DataPath;
import org.apache.commons.lang.StringUtils;

public class DxFileUtils {
    /**
     * /tmp/path/abc.txt.gz   return /tmp/path/
     * /tmp/path/abc.txt      return /tmp/path/
     * /tmp/path/abc          return /tmp/path/
     * /tmp/path/bac/         return /tmp/path/abc/
     * abc.txt                return  ""
     * ""                     return ""
     * @param filepath
     * @return
     */
    public static String extractFilePath(String filepath) {
        if (filepath == null) {
            return null;
        }
        if (filepath.contains("\\")) {
            filepath = filepath.replaceAll("\\\\", "/");
        }
        String fp = filepath.substring(0, filepath.lastIndexOf("/") + 1);
        return fp;
    }

    /**
     * /tmp/path/abc.txt.gz   return abc.txt.gz
     * /tmp/path/abc.txt      return abc.txt
     * /tmp/path/abc          return abc
     * /tmp/path/bac/         return ""
     * abc.txt                return  abc.txt
     * ""                     return ""
     * @param filepath
     * @return
     */
    public static String extractFileName(String filepath) {
        if (filepath == null) {
            return null;
        }
        if (filepath.contains("\\")) {
            filepath = filepath.replaceAll("\\\\", "/");
        }
        String fileFullName = filepath.substring(filepath.lastIndexOf("/") + 1);
        return fileFullName;
    }

    public static String extractFileNameWithoutExtension(String filepath) {
        if (filepath == null) {
            return null;
        }
        if (filepath.contains("\\")) {
            filepath.replaceAll("\\\\", "/");
        }
        String fileFullName = filepath.substring(filepath.lastIndexOf("/") + 1);
        String fileName = "";
        if (fileFullName.indexOf(".") != -1) {
            fileName = fileFullName.substring(0, fileFullName.indexOf("."));
        } else {
            fileName = fileFullName;
        }
        return fileName;
    }

    public static String extractFileExtension(String filepath) {
        if (filepath == null) {
            return null;
        }
        if (filepath.contains("\\")) {
            filepath.replaceAll("\\\\", "/");
        }
        String fileFullName = filepath.substring(filepath.lastIndexOf("/") + 1);
        String fileExtension = "";
        if (fileFullName.indexOf(".") != -1) {
            fileExtension = fileFullName.substring(fileFullName.indexOf("."));
        }
        return fileExtension;
    }

    public static String parseFileSequence(DataPath path) {
        String filePath = null;
        String tmpFilePath = extractFilePath(path.getPath());

        String tmpFileName = extractFileNameWithoutExtension(path.getPath());
        String tmpFileExtemsion = extractFileExtension(path.getPath());
        StringBuffer tmpfilePathBuf = new StringBuffer();
        tmpfilePathBuf.append(tmpFilePath);
        tmpfilePathBuf.append(StringUtils.isBlank(tmpFileName) ? "data" : tmpFileName);
        if (path.getSequence() != 1)
            tmpfilePathBuf.append(path.getSequence() - 1);
        tmpfilePathBuf
                .append(StringUtils.isBlank(tmpFileExtemsion) ? ".out" : tmpFileExtemsion);
        filePath = tmpfilePathBuf.toString();
        return filePath;
    }

    public static void main(String[] args) {
        System.out.println(parseFileSequence(new DataPath("/abc/456/", 0)));
    }

}
