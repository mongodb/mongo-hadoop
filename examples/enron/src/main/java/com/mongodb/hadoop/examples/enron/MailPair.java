package com.mongodb.hadoop.examples.enron;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MailPair implements WritableComparable {

    //CHECKSTYLE:OFF
    String from;
    String to;
    //CHECKSTYLE:ON

    public MailPair() {
    }

    public MailPair(final String from, final String to) {
        this.from = from;
        this.to = to;
    }

    public void readFields(final DataInput in) throws IOException {
        this.from = in.readUTF();
        this.to = in.readUTF();
    }

    public void write(final DataOutput out) throws IOException {
        out.writeUTF(this.from);
        out.writeUTF(this.to);
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof MailPair) {
            MailPair mp = (MailPair) o;
            return from.equals(mp.from) && to.equals(mp.to);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(final Object o) {
        if (!(o instanceof MailPair)) {
            return -1;
        }
        MailPair mp = (MailPair) o;
        int first = from.compareTo(mp.from);
        if (first != 0) {
            return first;
        }
        int second = to.compareTo(mp.to);
        if (second != 0) {
            return second;
        }
        return 0;
    }

}
