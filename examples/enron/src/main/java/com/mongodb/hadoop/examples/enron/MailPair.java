package com.mongodb.hadoop.examples.enron;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class MailPair implements WritableComparable{

    String from;
    String to;

    public MailPair(){
    }

    public MailPair(String from, String to){
        this.from = from;
        this.to = to;
    }

    public void readFields(DataInput in) throws IOException{
        this.from = in.readUTF();
        this.to = in.readUTF();
    }

    public void write(DataOutput out) throws IOException{
        out.writeUTF(this.from);
        out.writeUTF(this.to);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MailPair) {
           MailPair mp = (MailPair) o;
           return from == mp.from && to == mp.to;
        }
        return false;
    }

    @Override
    public int compareTo(Object o) {
        if(!(o instanceof MailPair)){
            return -1;
        }
        MailPair mp = (MailPair)o;
        int first = from.compareTo(mp.from);
        if(first != 0) return first;
        int second = to.compareTo(mp.to); 
        if(second != 0) return second;
        return 0;
    }

}
