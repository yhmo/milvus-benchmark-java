package io.milvus;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {
    public static String generatorLocalPath(String subDir) {
        Path currentWorkingDirectory = Paths.get("").toAbsolutePath();
        Path currentScriptPath = currentWorkingDirectory.resolve(subDir);
        return currentScriptPath.toString();
    }
}
