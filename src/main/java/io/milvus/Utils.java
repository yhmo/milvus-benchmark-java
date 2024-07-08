package io.milvus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static String generatorLocalPath(String subDir) {
        Path currentWorkingDirectory = Paths.get("").toAbsolutePath();
        Path currentScriptPath = currentWorkingDirectory.resolve(subDir);
        File dirPath = new File(currentScriptPath.toString());
        dirPath.mkdirs();
        return currentScriptPath.toString();
    }

    public static String downloadRemoteFile(String url, String localDir) {
        try {
            File file = new File(url);
            String fileName = file.getName();
            String baseDir = Utils.generatorLocalPath(localDir);
            String localFilePath = baseDir + "/" + fileName;
            file = new File(localFilePath);
            if (file.exists()) {
                System.out.println(String.format("'%s' already exists, no need to download from '%s'",
                        localFilePath, url));
                return localFilePath;
            }

            if (!url.startsWith("http://")) {
                url = "http://" + url;
            }

            System.out.println(String.format("Downloading from '%s'...", url));
            URL urlObj = new URL(url);
            HttpURLConnection httpConn = (HttpURLConnection) urlObj.openConnection();
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = httpConn.getInputStream();
                FileOutputStream outputStream = new FileOutputStream(localFilePath);
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                outputStream.close();
                inputStream.close();
            } else {
                System.out.println(String.format("Failed to download '%s', response code: %d",
                        url, responseCode));
            }
            httpConn.disconnect();
            return localFilePath;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(String.format("Failed to download '%s', error: %s",
                    url, e.getMessage()));
            return "";
        }
    }

    public static Map<String, Object> readConfigurations() {
        try {
            String baseDir = Utils.generatorLocalPath("config");
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> data = mapper.readValue(new File(baseDir + "/benchmark.yaml"), Map.class);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to parse configuration YAML file");
            return new HashMap<>();
        }
    }
}
