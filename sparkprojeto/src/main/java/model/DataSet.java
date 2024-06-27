package model;

import java.io.IOException;
import java.util.ArrayList;
import model.WordReader;

public class DataSet {
	
	private ArrayList<String> textWords;
	
	public DataSet() {
		this.textWords = new ArrayList<String>();
	}

	public void read(String datasetPath) throws IOException {
		WordReader wordReader = new WordReader(datasetPath);
        while (wordReader.hasNextLine()) {
            String line = wordReader.getNextLine();
            String words[] = line.split("\\s+");
            for (String word : words) {
            	this.textWords.add(word);
            }
        }	
	}
	
	
	public ArrayList<String> getTextWords() {
		return this.textWords;
	}
}
