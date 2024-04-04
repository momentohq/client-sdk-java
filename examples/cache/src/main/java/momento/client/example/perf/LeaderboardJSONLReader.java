package momento.client.example.perf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class LeaderboardJSONLReader {

    public static List<String> parseFile(String filePath) throws IOException  {
        return Files.readAllLines(new File(filePath).toPath());
    }
}
