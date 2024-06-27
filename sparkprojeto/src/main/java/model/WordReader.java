package model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WordReader {
    private BufferedReader reader;

    public WordReader(String fileName) throws IOException {
        reader = new BufferedReader(new FileReader(fileName));
    }

    public boolean hasNextLine() throws IOException {
        return reader.ready();
    }

    public String getNextLine() throws IOException {
        return reader.readLine();
    }

    public void close() throws IOException {
        reader.close();
    }
}
