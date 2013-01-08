package com.mongodb.hadoop.util;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.input.BSONFileRecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;

public class BSONSplitter extends Configured implements Tool {

    private static final Log log = LogFactory.getLog( BSONSplitter.class );

    private Map<Path, List<FileSplit>> splitsMap;
    private Path inputPath;
    private BasicBSONEncoder bsonEnc = new BasicBSONEncoder();


    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName(); 
            return !name.startsWith("_") && !name.startsWith("."); 
        }
    }; 


    public Path[] getInputPaths(){
        String dirs = getConf().get("mapred.input.dir", "");
        String [] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }


    public BSONSplitter(){
        this.splitsMap = new HashMap<Path, List<FileSplit>>();
    }

    public void setInputPath(Path p){
        this.inputPath = p;
    }

    public void readSplitsForFile(FileStatus file) throws IOException{
        long minSize = Math.max(1L, getConf().getLong("mapred.min.split.size", 1L));
        long maxSize = getConf().getLong("mapred.max.split.size", Long.MAX_VALUE);
        Path path = file.getPath();
        List<FileSplit> splits = new ArrayList<FileSplit>();
        log.info("generating splits for " + path);
        FileSystem fs = path.getFileSystem(getConf());
        long length = file.getLen();
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
        if (length != 0) { 
            long blockSize = file.getBlockSize();
            long splitSize = Math.max(minSize, Math.min(maxSize, blockSize));
            long bytesRemaining = length;
            ByteBuffer bb;
            byte[] headerBuf = new byte[4];
            int numDocs = 0;
            FSDataInputStream fsDataStream = fs.open(path);
            long curSplitLen = 0;
            long curSplitStart = 0;
            long curSplitEnd = 0;
            log.info("Split size: " + splitSize + " bytes.");
            while(fsDataStream.getPos() + 1 < length){
                fsDataStream.read(fsDataStream.getPos(), headerBuf, 0, 4);
                //TODO check that 4 bytes were actually read successfully, otherwise error
                //TODO just parse the integer so we don't init a bytebuf every time
                bb = ByteBuffer.wrap(headerBuf);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                int bsonDocSize = bb.getInt();
                if(curSplitLen + bsonDocSize >= splitSize){
                    FileSplit split = new FileSplit(path, curSplitStart,
                            curSplitLen,
                            blkLocations[blkLocations.length-1].getHosts());
                    splits.add(split);
                    curSplitLen = 0;
                    curSplitStart = fsDataStream.getPos() + bsonDocSize;
                    log.info("Creating new split (" + splits.size() + ") " + split.toString());
                }
                curSplitLen += bsonDocSize;

                fsDataStream.seek(fsDataStream.getPos() + bsonDocSize);
                numDocs++;
                if(numDocs % 10000 == 0){
                    log.info("read " + numDocs + " docs, " + fsDataStream.getPos() + " bytes read.");
                }
            }
            if(curSplitLen > 0){
                FileSplit split = new FileSplit(path,
                        curSplitStart,
                        curSplitLen,
                        blkLocations[blkLocations.length-1].getHosts());
                splits.add(split);
                log.info("Final split (" + splits.size() + ") " + split.toString());
            }
            this.splitsMap.put(path, splits);
        } 
    }

    public void writeSplits() throws IOException{
        pathsloop:
        for(Map.Entry<Path, List<FileSplit>> entry : this.splitsMap.entrySet()) {
            Path key = entry.getKey();
            List<FileSplit> value = entry.getValue();
            Path outputPath = new Path(key.getParent(),  "." + key.getName() + ".splits");
            FileSystem pathFileSystem = outputPath.getFileSystem(getConf());
            FSDataOutputStream fsDataOut = null;
            try{
                fsDataOut = pathFileSystem.create(outputPath, false);
                for(FileSplit inputSplit : value){
                    BSONObject splitObj = BasicDBObjectBuilder.start()
                                            .add( "start" , inputSplit.getStart())
                                            .add( "length" , inputSplit.getLength()).get();
                    byte[] encodedObj = this.bsonEnc.encode(splitObj);
                    try{
                        fsDataOut.write(encodedObj, 0, encodedObj.length);
                    }catch(IOException ioe){
                        log.error("Failed writing data to splits file:" + ioe.getMessage());
                        continue pathsloop;
                    }

                }
            }catch(IOException e){
                log.error("Could not create splits file: " + e.getMessage());
                continue;
            }finally{
                if(fsDataOut!=null){
                    fsDataOut.close();
                }
            }
        }
    }
    
    public void readSplits() throws IOException{
        this.splitsMap.clear();
        //for(Path p : getInputPaths()){
        List<FileStatus> files = getFilesInPath(this.inputPath);
        for(FileStatus file : files){
            Path path = file.getPath();
            log.info("generating splits for " + path);
            readSplitsForFile(file);
        }
    }

    public Map<Path, List<FileSplit>> getSplitsMap(){
        return this.splitsMap;
    }


    public List<FileStatus> getFilesInPath(Path p) throws IOException{
        ArrayList<FileStatus> result = new ArrayList<FileStatus>();
        FileSystem fs = p.getFileSystem(getConf()); 
        FileStatus[] matches = fs.globStatus(p, hiddenFileFilter);
        if (matches == null) {
            throw new IOException("Input path does not exist: " + p);
        } else if (matches.length == 0) {
            throw new IOException("Input Pattern " + p + " matches 0 files");
        } else {
            for (FileStatus globStat: matches) {
                if (globStat.isDir()) {
                    for(FileStatus stat: fs.listStatus(globStat.getPath(), hiddenFileFilter)) {
                        result.add(stat);
                    }          
                } else {
                    result.add(globStat);
                }
            }
        }
        return result;
    }

    public int run( String[] args ) throws Exception{
        readSplits();
        writeSplits();
        return 0;
    }

    public static void main(String args[]) throws Exception{
        System.exit( ToolRunner.run( new BSONSplitter(), args ) );
    }

}
