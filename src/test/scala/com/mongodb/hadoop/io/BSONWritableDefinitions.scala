package com.mongodb.hadoop.io

//import com.mongodb.hadoop.io.BSONWritable
import org.junit.Assert._
import scala.actors.Actor._
import cuke4duke._
import collection.JavaConversions._


class BSONWritableDefinitions extends ScalaDsl with EN {

  var writeable1: BSONWritable = null
  var writeable1Dser: BSONWritable = null
  var result = -1

  Given("I have an empty BSONWritable") {
    writeable1 = new BSONWritable
  }

  When("""^I compare it to itself$""") {
    result =  writeable1.compareTo(writeable1)
  }

  When("^I deserialize it$") {
    val pr = new java.io.PipedInputStream();
    val pw = new java.io.PipedOutputStream(pr);
    val oos = new java.io.ObjectOutputStream(pw);
    actor{
      writeable1.write(oos)
      oos close
    }
    writeable1Dser = new BSONWritable
    writeable1Dser.readFields( new java.io.ObjectInputStream(pr) )
  }
  When("^I DO NOT deserialize it$") { //THIS DOES NOT WORK
   val pr = new java.io.PipedInputStream();
   val pw = new java.io.PipedOutputStream(pr);
   val oos = new java.io.ObjectOutputStream(pw);
   actor{
     oos.writeObject(writeable1); //does this really replicate what the mongo java driver does?
     oos close
   }
   writeable1Dser = new java.io.ObjectInputStream(pr).readObject.asInstanceOf[BSONWritable]
}
Then("^the new one should equal the old one$"){
    assertEquals(writeable1, writeable1Dser);
    assertEquals(writeable1Dser, writeable1);
    assertEquals(0, writeable1.compareTo(writeable1Dser))
    assertEquals(0, writeable1Dser.compareTo(writeable1))
}



Then("""the result should be (\d+)""") { i:Int =>
    assertEquals(i, result)
  }

private val arrayRegEx = """\[(.*)\]""".r

  /** May have to change the \S to . to match future values */
  When("^I add key 1 of type <keytype> and value 2 of type Int$")

  When("""^I add key "(\w+)" and value (\S+) of type (\w+)$"""){
    (key:String, valueStr:String, valtype:String) =>
      val value = valtype match{
        case "String" => valueStr
        case "Number" => Integer.valueOf( valueStr )
        case "Int" => Integer.valueOf( valueStr )
        case "Array" => {
          val v = new org.bson.types.BasicBSONList()
          scala.util.parsing.json.JSON.parseFull(valueStr) match{
            case None => null
            case Some(pl) =>
              if ( ! pl.isInstanceOf[List[_]])
                throw new IllegalArgumentException("value is not an array, it is a "+pl.asInstanceOf[AnyRef].getClass() );
            ///vvv fsc has a problem with this, must use scalac
            v addAll pl.asInstanceOf[List[Object]]
            v
          }
        }
        case _ =>
        throw new IllegalArgumentException("Unknown type "+valtype)
      }
    writeable1.put(key, value)
 }
 


}
