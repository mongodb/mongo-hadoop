package com.mongodb.spark.pickle;

import com.mongodb.DBRef;
import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Implementation of {@link net.razorvine.pickle.IObjectPickler} that pickles
 * BSON types only, so that they can be correctly unpickled into PyMongo
 * objects.
 *
 * For documentation on Python's pickle protocol, see the following:
 * - https://docs.python.org/2/library/pickle.html
 * - http://svn.python.org/projects/python/trunk/Lib/pickletools.py
 */
public class BSONPickler implements IObjectPickler {

    private void putBinstring(final byte[] bytes, final OutputStream out)
      throws IOException {
        int binLen = bytes.length;
        if (binLen <= 0xff) {
            out.write(Opcodes.SHORT_BINSTRING);
            out.write(binLen);
        } else {
            out.write(Opcodes.BINSTRING);
            out.write(binLen & 0xff);
            out.write(binLen >>> 8 & 0xff);
            out.write(binLen >>> 16 & 0xff);
            out.write(binLen >>> 24 & 0xff);
        }
        out.write(bytes);
    }

    private void putBinstring(final String string, final OutputStream out)
      throws IOException {
        putBinstring(string.getBytes(), out);
    }

    /**
     * Translate flags from java.util.regex.Pattern into their respetive values
     * in Python's "re" library.
     *
     * @param javaFlags flags from a java.util.regex.Pattern
     * @return equivalent flags in Python
     */
    private int translateRegexFlags(final int javaFlags) {
        int pyFlags = 0;
        if ((javaFlags & Pattern.CASE_INSENSITIVE) > 0) {
            pyFlags |= 2;
        }
        if ((javaFlags & Pattern.COMMENTS) > 0) {
            pyFlags |= 64;
        }
        if ((javaFlags & Pattern.DOTALL) > 0) {
            pyFlags |= 16;
        }
        if ((javaFlags & Pattern.MULTILINE) > 0) {
            pyFlags |= 8;
        }
        // 0x100 == Pattern.UNICODE_CHARACTER_CLASS in Java >= 7.
        // Python doesn't have separate flags for these. Even if only one of
        // these is set, enabling the UNICODE flag is still probably the closest
        // approximation in Python.
        if (((javaFlags & Pattern.UNICODE_CASE) | (javaFlags & 0x100)) > 0) {
            pyFlags |= 32;
        }
        return pyFlags;
    }

    private void pickleRegex(final Pattern pattern, final OutputStream out,
                             final Pickler pickler)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.regex\nRegex\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        out.write(Opcodes.MARK);
        putBinstring("pattern", out);
        pickler.save(pattern.pattern());
        putBinstring("flags", out);
        pickler.save(translateRegexFlags(pattern.flags()));
        out.write(Opcodes.SETITEMS);
        out.write(Opcodes.BUILD);
    }

    private void pickleBSONTimestamp(final BSONTimestamp timestamp,
                                     final OutputStream out,
                                     final Pickler pickler)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.timestamp\nTimestamp\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        out.write(Opcodes.MARK);
        putBinstring("_Timestamp__time", out);
        pickler.save(timestamp.getTime());
        putBinstring("_Timestamp__inc", out);
        pickler.save(timestamp.getInc());
        out.write(Opcodes.SETITEMS);
        out.write(Opcodes.BUILD);
    }

    private void pickleCode(final Code code, final OutputStream out,
                            final Pickler pickler)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.code\nCode\n".getBytes());
        pickler.save(code.getCode());
        out.write(Opcodes.TUPLE1);
        out.write(Opcodes.NEWOBJ);

        // PyMongo's bson.code.Code always has a scope, even it if is empty.
        out.write(Opcodes.EMPTY_DICT);
        putBinstring("_Code__scope", out);
        if (code instanceof CodeWithScope) {
            pickler.save(((CodeWithScope) code).getScope());
        } else if (code instanceof CodeWScope) {
            pickler.save(((CodeWScope) code).getScope().toMap());
        } else {
            out.write(Opcodes.EMPTY_DICT);
        }
        out.write(Opcodes.SETITEM);

        out.write(Opcodes.BUILD);
    }

    private void writeMinKey(final OutputStream out)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.min_key\nMinKey\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        out.write(Opcodes.BUILD);
    }

    private void writeMaxKey(final OutputStream out)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.max_key\nMaxKey\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        out.write(Opcodes.BUILD);
    }

    private void pickleDBRef(final DBRef dbref, final OutputStream out,
                             final Pickler pickler)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.dbref\nDBRef\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        out.write(Opcodes.MARK);
        putBinstring("_DBRef__kwargs", out);
        out.write(Opcodes.EMPTY_DICT);
        putBinstring("_DBRef__collection", out);
        pickler.save(dbref.getCollectionName());
        putBinstring("_DBRef__database", out);
        // org.bson.types.DBRef stores neither database name nor extra "kwargs".
        out.write(Opcodes.NONE);
        putBinstring("_DBRef__id", out);
        // Not saving this in memo, because a DBRef that uses itself as its own
        // id can't be saved to MongoDB anyway.
        pickler.save(dbref.getId());
        out.write(Opcodes.SETITEMS);
        out.write(Opcodes.BUILD);
    }

    private void pickleBinary(final Binary binary, final OutputStream out,
                              final Pickler pickler)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.binary\nBinary\n".getBytes());
        putBinstring(binary.getData(), out);
        pickler.save(binary.getType());
        out.write(Opcodes.TUPLE2);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.EMPTY_DICT);
        putBinstring("_Binary__subtype", out);
        pickler.save(binary.getType());
        out.write(Opcodes.SETITEM);
        out.write(Opcodes.BUILD);
    }

    private void pickleObjectId(final ObjectId oid, final OutputStream out)
      throws IOException {
        out.write(Opcodes.GLOBAL);
        out.write("bson.objectid\nObjectId\n".getBytes());
        out.write(Opcodes.EMPTY_TUPLE);
        out.write(Opcodes.NEWOBJ);
        out.write(Opcodes.SHORT_BINSTRING);
        out.write(12);
        out.write(oid.toByteArray());
        out.write(Opcodes.BUILD);
    }

    private void pickleDate(
      final Date date, final OutputStream out, final Pickler currentPickler)
      throws IOException {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(date);
        // Assume raw Dates from MongoDB are in UTC.
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));

        out.write(Opcodes.GLOBAL);
        out.write("datetime\ndatetime\n".getBytes());
        out.write(Opcodes.MARK);
        currentPickler.save(calendar.get(Calendar.YEAR));
        currentPickler.save(calendar.get(Calendar.MONTH) + 1);
        currentPickler.save(calendar.get(Calendar.DAY_OF_MONTH));
        currentPickler.save(calendar.get(Calendar.HOUR_OF_DAY));
        currentPickler.save(calendar.get(Calendar.MINUTE));
        currentPickler.save(calendar.get(Calendar.SECOND));
        currentPickler.save(calendar.get(Calendar.MILLISECOND) * 1000);
        // Do not save TimeZone information, so that Python datetimes are
        // timezone-naive (default Pickler will always save timezone
        // information).
        out.write(Opcodes.TUPLE);
        out.write(Opcodes.REDUCE);
    }

    /**
     * Write the Python "pickle" representation of a BSON type.
     *
     * @param obj the object to be pickled
     * @param out the OutputStream to which to write
     * @param currentPickler the current Pickler instance
     * @throws PickleException if an issue is encountered while serializing a
     * BSON object
     * @throws IOException if an issue is encountered writing to the
     * OutputStream
     */
    @Override
    public void pickle(
      final Object obj, final OutputStream out, final Pickler currentPickler)
      throws IOException {

        Object o = obj;
        if (obj instanceof BSONValueBox) {
            o = ((BSONValueBox) obj).get();
        }

        if (o instanceof ObjectId) {
            pickleObjectId((ObjectId) o, out);
        } else if (o instanceof Binary) {
            pickleBinary((Binary) o, out, currentPickler);
        } else if (o instanceof DBRef) {
            pickleDBRef((DBRef) o, out, currentPickler);
        } else if (o instanceof MaxKey) {
            writeMaxKey(out);
        } else if (o instanceof MinKey) {
            writeMinKey(out);
        } else if (o instanceof Code) {
            pickleCode((Code) o, out, currentPickler);
        } else if (o instanceof BSONTimestamp) {
            pickleBSONTimestamp((BSONTimestamp) o, out, currentPickler);
        } else if (o instanceof Pattern) {
            // Since the Hadoop connector is in Java, regular expressions will
            // be of this class, rather than scala.util.matching.Regex.
            pickleRegex((Pattern) o, out, currentPickler);
        } else if (o instanceof Date) {
            pickleDate((Date) o, out, currentPickler);
        } else {
            throw new PickleException("Can't pickle this: " + o);
        }
    }
}
