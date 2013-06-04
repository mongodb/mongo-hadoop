package com.mongodb.hadoop;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class BSONPathFilter implements PathFilter{

    private static final Log log = LogFactory.getLog( BSONPathFilter.class );

    public BSONPathFilter(){
        log.info("path filter constructed.");
    }

    public boolean accept(Path path){
        String pathName = path.getName().toLowerCase();
        boolean acceptable = pathName.endsWith(".bson") && !pathName.startsWith("."); 
        log.info(path.toString() + " returning " + acceptable);
        return acceptable;
    }
}

