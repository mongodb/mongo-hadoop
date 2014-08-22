package helpers;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/*
 * TOBAG : converts a tuple to a bag of one-item tuples
 */
public class TOBAG extends EvalFunc<DataBag> {
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();

    public DataBag exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        try {
            DataBag output = mBagFactory.newDefaultBag();
            Tuple nested = (Tuple) input.get(0);
            for (Object o : nested.getAll()) {
                output.add(mTupleFactory.newTuple(o));
            }

            return output;
        } catch (Exception e) {
            return null;
        }
    }
}
