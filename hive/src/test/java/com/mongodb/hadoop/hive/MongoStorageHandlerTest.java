package com.mongodb.hadoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MongoStorageHandlerTest extends HiveTest {

    private static MongoStorageHandler msh = new MongoStorageHandler();

    @Test
    public void testDecomposePredicate() throws SerDeException {
        BSONSerDe serde = new BSONSerDe();
        Configuration conf = new Configuration();
        Properties tableProperties = new Properties();
        // Set table columns.
        tableProperties.setProperty(
          serdeConstants.LIST_COLUMNS, "id,i,j");
        tableProperties.setProperty(
          serdeConstants.LIST_COLUMN_TYPES, "string,int,int");
        serde.initialize(conf, tableProperties);

        // Build a query.
        // WHERE i > 20
        GenericUDFOPGreaterThan gt = new GenericUDFOPGreaterThan();
        ExprNodeDesc[] children = {
          new ExprNodeColumnDesc(new SimpleMockColumnInfo("i")),
          new ExprNodeConstantDesc(20)
        };
        ExprNodeGenericFuncDesc expr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo,
          gt,
          Arrays.asList(children));
        HiveStoragePredicateHandler.DecomposedPredicate decomposed =
          msh.decomposePredicate(null, serde, expr);
        assertNull(decomposed.residualPredicate);
        assertEquals(
          expr.getExprString(),
          decomposed.pushedPredicate.getExprString());
    }

    @Test
    public void testDecomposePredicateUnrecognizedOps() throws SerDeException {
        BSONSerDe serde = new BSONSerDe();
        Configuration conf = new Configuration();
        Properties tableProperties = new Properties();
        // Set table columns.
        tableProperties.setProperty(
          serdeConstants.LIST_COLUMNS, "id,i,j");
        tableProperties.setProperty(
          serdeConstants.LIST_COLUMN_TYPES, "string,int,int");
        serde.initialize(conf, tableProperties);

        // Build a query.
        // WHERE i > 20 AND j IS BETWEEN 1 AND 10
        GenericUDFOPGreaterThan gt = new GenericUDFOPGreaterThan();
        ExprNodeDesc[] children = {
          new ExprNodeColumnDesc(new SimpleMockColumnInfo("i")),
          new ExprNodeConstantDesc(20)
        };
        ExprNodeGenericFuncDesc iGt20 = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo,
          gt,
          Arrays.asList(children));
        GenericUDFBetween between = new GenericUDFBetween();
        ExprNodeDesc[] betweenChildren = {
          new ExprNodeConstantDesc(false),
          new ExprNodeColumnDesc(new SimpleMockColumnInfo("j")),
          new ExprNodeConstantDesc(1),
          new ExprNodeConstantDesc(10)
        };
        ExprNodeGenericFuncDesc jBetween1And10 = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo,
          between,
          Arrays.asList(betweenChildren));
        ExprNodeDesc[] exprChildren = {
          iGt20,
          jBetween1And10
        };
        ExprNodeGenericFuncDesc expr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo,
          new GenericUDFOPAnd(),
          Arrays.asList(exprChildren));

        HiveStoragePredicateHandler.DecomposedPredicate decomposed =
          msh.decomposePredicate(null, serde, expr);
        assertEquals(
          jBetween1And10.getExprString(),
          decomposed.residualPredicate.getExprString());
        assertEquals(
          iGt20.getExprString(),
          decomposed.pushedPredicate.getExprString());
    }
}
