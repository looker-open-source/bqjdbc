package net.starschema.clouddb.jdbc.list;

import java.util.ArrayList;
import java.util.List;

public class SynonymContainer {
    private UniQueIdContainer pointedResource;
    private List<String> Synonyms;
    
    public SynonymContainer(UniQueIdContainer pointedResource) {
        this.pointedResource = pointedResource;
        Synonyms = new ArrayList<String>();
    }
    
    public void addSynonym(String synonym) {
       Synonyms.add(synonym);
    }
    
    public UniQueIdContainer getPointedResource() {
        return this.pointedResource;
    }
    
    public List<String> getSynonyms() {
        return Synonyms;
    }
}
