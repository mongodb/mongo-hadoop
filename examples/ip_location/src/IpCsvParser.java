
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/** <p>Contains utility methods to parse the ip database file and other utility methods</p>
 * <p>TODO: eliminate code duplication with CsvReader</p>
 * 
 * Created: May 9, 2011  2:02:38 PM
 *
 * @author Joseph Shraibman
 * @version $Revision: 1.1 $  $Date:  $ $Author: jks $
 */
public class IpCsvParser {
    
    public enum Key{
        IP_FROM, IP_TO, REGISTRY, ASSIGNED, CTRY, CNTRY, COUNTRY
    }
    
    public final static Pattern patt = Pattern.compile("\"(\\d+)\",\"(\\d+)\",\"(\\w+)\",\"(\\d+)\",\"(\\w+)\",\"(\\w+)\",\"(.+)\"");
    public final static Map<Key,String> parseLine(String in){
        if (in == null)
            return null;
        Matcher m = patt.matcher(in);
        if (m.find()){
            EnumMap <Key,String> ans = new EnumMap<Key, String>(Key.class);
            for(int i = 0;i < Key.values().length ; i++){
                ans.put(Key.values()[i], m.group(i+1));
            }
            return ans;
        }
        return null;
    }
    private static int howManyBits(long from, long to){
        if (from == 0)//special case
            return  Long.numberOfTrailingZeros(to);
        return 32 - Long. numberOfTrailingZeros(from & to);
    }
     
    private final static int mask_0 = Integer.parseInt("11111111",2);
    private final static int mask_1 = mask_0 << 8;
    private final static int mask_2 = mask_1 << 8;
    private final static int mask_3 = mask_2 << 8;
    private static InetAddress toInetAddress(long in) throws UnknownHostException{
        byte[] ba = new byte[]{
            (byte) ((in & mask_3) >> (8*3)),
            (byte) ((in & mask_2) >> (8*2)),
            (byte) ((in & mask_1) >> (8)),
            (byte) (in & mask_0)};
        return InetAddress.getByAddress(ba);
    }
    public final static String getIpRange(String fromStr, String to) throws UnknownHostException{
        long from = Long.parseLong(fromStr);
        int numBits = howManyBits(from, Long.parseLong(to));
        return toInetAddress(from).getHostAddress()+"/"+numBits;
    }
    //-------------
    public final static long toLong(InetAddress ia){
        byte[] ba = ia.getAddress();
        long accum = 0;
        for(int i = 0; i < ba.length ; i++){
            accum = accum << 8;
            accum |= ba[i];
        }
        return accum;
    }
    public static String getIpRange(String input_ip, int bits){
        try {
            long ipAsNum = toLong(InetAddress.getByName(input_ip));
            int shiftNum = 32 - bits;
            ipAsNum = ((ipAsNum >> shiftNum) << shiftNum);
            InetAddress ia = toInetAddress(ipAsNum);
            return  ia.toString().substring(1) + "/"+bits;
        } catch (UnknownHostException impossible) {
            return null;
        }
    }

}
