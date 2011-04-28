
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Created: Apr 12, 2011  4:09:53 PM
 *
 * @author Joseph Shraibman
 * @version $Revision: 1.1 $  $Date:  $ $Author: jks $
 */
public class ApacheLogFileReader {
     
    private void connect() throws MongoException, UnknownHostException{
         com.mongodb.MongoURI uri = new MongoURI(_uriString);
                    com.mongodb.Mongo mongo = uri.connect();
                        com.mongodb.DB db = mongo.getDB(uri.getDatabase());
                        _coll = db.getCollection(uri.getCollection());
    }
    private interface logParser{
        Matcher match(String logline);
        List<String> getKeys();
    }
    private logParser commonApacheParser = new logParser() {
        private final Pattern pattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ((\\d+)|-)");// \"([^\"]+)\" \"([^\"]+)\"";

        public Matcher match(String logline) {
            return pattern.matcher(logline);
        }
        private final List<String> keyList = Arrays.asList("ip", null, "user", "timestamp", "request","statuscode","bytes");
        public List<String> getKeys() {
            return keyList;
        }
    };
    private final static logParser combinedApacheParser = new logParser() {
        private final Pattern pattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ((\\d+)|-) \"([^\"]+)\" \"([^\"]+)\"");
        public Matcher match(String logline) {
            return pattern.matcher(logline);
        }
        private final List<String> keyList = Arrays.asList("ip", null, "user", "timestamp", "request","statuscode","bytes","referer","useragent");
        public List<String> getKeys() {
            return keyList;
        }
    };
    private final static logParser vcombinedApacheParser = new logParser() {
        private final Pattern pattern = Pattern.compile("^(\\S+) ([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ((\\d+)|-) \"([^\"]+)\" \"([^\"]+)\"");
        public Matcher match(String logline) {
            return pattern.matcher(logline);
        }
        private final List<String> keyList = Arrays.asList("serverhn","ip", null, "user", "timestamp", "request","statuscode","bytes","referer","useragent");
        public List<String> getKeys() {
            return keyList;
        }
    };
    private List<logParser> _logParsersToTry = Arrays.asList(vcombinedApacheParser, combinedApacheParser, commonApacheParser);
    private logParser _logParser = null;
    
    // 192.168.1.106 - - [24/Jun/2007:00:27:12 -0400] "GET /temp.html HTTP/1.1" 304 - "-" "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.4) Gecko/20070515 Firefox/2.0.0.4"
    private int _badLinesCount = 0;
    private void insert(String logline){
        if (_logParser == null){
            //try patterns against our log file until we find one that fits
            for(logParser lp : _logParsersToTry){
                Matcher m = lp.match(logline);
                if (m.find())
                    _logParser = lp;
            }
            if (_logParser == null)
                throw new RuntimeException("I do not recognize this log pattern");
        }

        Matcher m = _logParser.match(logline);
        if ( ! m.find()){
            _badLinesCount++;
            return;
        }
        

     BasicDBObject doc = new BasicDBObject();
     int key_idx = 0;
     for(String key : _logParser.getKeys()){
         key_idx++; //first key has index of 1
         if (key != null && ! ("-".equals(key) || "\"-\"".equals(key) ) )
             doc.append(key, m.group(key_idx));
     }
            _coll.insert(doc);
    }
    public static final void main(String[] args) throws Exception {
        ApacheLogFileReader fr = new ApacheLogFileReader();
        fr._uriString = args[0];
        String filename = args[1];
        BufferedReader br = new BufferedReader(new FileReader(filename));
        fr.connect();
        int linecount = 0;
        while(true){
            String line = br.readLine();
            if (line == null)
                break;
            fr.insert(line);
            linecount++;
        }
        System.out.println("Read "+linecount+" lines from logfile, could not parse "+fr._badLinesCount+" of them");
    }
    private String _uriString;
    private com.mongodb.DBCollection _coll ;
}
