import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created: Apr 7, 2011  2:43:07 PM
 *
 * @author Joseph Shraibman
 */
public class CsvReader {

    private static int howManyBits( long from, long to ){
        return 32 - Long.numberOfTrailingZeros( from & to );
    }

    private final static int mask_0 = Integer.parseInt( "11111111", 2 );
    private final static int mask_1 = mask_0 << 8;
    private final static int mask_2 = mask_1 << 8;
    private final static int mask_3 = mask_2 << 8;

    private static InetAddress toInetAddress( long in ) throws UnknownHostException{
        byte[] ba = new byte[] {
                (byte) ( ( in & mask_3 ) >> ( 8 * 3 ) ) ,
                (byte) ( ( in & mask_2 ) >> ( 8 * 2 ) ) ,
                (byte) ( ( in & mask_1 ) >> ( 8 ) ) ,
                (byte) ( in & mask_0 )
        };
        //System.out.println("in: "+in+" ba: "+java.util.Arrays.toString(ba));
        return InetAddress.getByAddress( ba );
    }

    public static final void main( String[] args ) throws Exception{
        BufferedReader br = new BufferedReader( new java.io.FileReader( "IpToCountry.csv" ) );
        Pattern patt = Pattern.compile( "\"(\\d+)\",\"(\\d+)\",\"(\\w+)\",\"(\\d+)\",\"(\\w+)\",\"(\\w+)\",\"(.+)\"" );
        int most_bits = 0;
        while ( true ){
            String line = br.readLine();
            if ( line == null )
                break;
            line = line.trim();
            if ( line.length() == 0 || line.charAt( 0 ) == '#' )
                continue;
            //"16777216","16777471","apnic","1272931200","AU","AUS","Australia"
            Matcher m = patt.matcher( line );
            if ( !m.find() ){
                System.err.println( "line does not match: " + line );
                continue;
            }
            long from = Long.valueOf( m.group( 1 ) );
            long to = Long.valueOf( m.group( 2 ) );
            String reg = m.group( 3 );
            String assigned = m.group( 4 );
            String country = m.group( 7 );
            int numBits = howManyBits( from, to );
            System.out.println(
                    toInetAddress( from ) + " - " + toInetAddress( to ) + "\t " + numBits + "\t " + country );
            most_bits = Math.max( most_bits, numBits );
        }
        System.out.println( "most bits: " + most_bits );
    }


}
