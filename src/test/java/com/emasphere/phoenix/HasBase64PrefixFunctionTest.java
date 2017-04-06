package com.emasphere.phoenix;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;

import static java.util.Arrays.*;
import static org.apache.hadoop.hbase.util.Bytes.*;
import static org.junit.Assert.*;

/**
 * @author Sebastien Gerard
 */
public class HasBase64PrefixFunctionTest {

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    @Test
    public void evaluatePrefixShortherThanActualValueMatch() throws IOException {
        final byte[] actualValue = toBytes("emasphere");
        final String prefixString = "ema";
        final byte[] prefixValue = ENCODER.encode(prefixString.getBytes());

        final Expression binary = LiteralExpression.newConstant(actualValue);
        final LiteralExpression prefix = LiteralExpression.newConstant(prefixValue);

        final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        final HasBase64PrefixFunction function = createFunction(binary, prefix);

        final boolean evaluate = function.evaluate(new SingleKeyValueTuple(), ptr);

        assertEquals("evaluate", true, evaluate);
        assertArrayEquals("result", PDataType.TRUE_BYTES, ptr.get());
    }

    @Test
    public void evaluatePrefixShorterThanActualValueNotMatch() throws IOException {
        final byte[] actualValue = toBytes("google");
        final String prefixString = "ema";
        final byte[] prefixValue = ENCODER.encode(prefixString.getBytes());

        final Expression binary = LiteralExpression.newConstant(actualValue);
        final LiteralExpression prefix = LiteralExpression.newConstant(prefixValue);

        final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        final HasBase64PrefixFunction function = createFunction(binary, prefix);

        final boolean evaluate = function.evaluate(new SingleKeyValueTuple(), ptr);

        assertEquals("evaluate", true, evaluate);
        assertArrayEquals("result", PDataType.FALSE_BYTES, ptr.get());
    }

    @Test
    public void evaluatePrefixLongerThanActualValue() throws IOException {
        final byte[] actualValue = toBytes("ema");
        final String prefixString = "emasphere";
        final byte[] prefixValue = ENCODER.encode(prefixString.getBytes());

        final Expression binary = LiteralExpression.newConstant(actualValue);
        final LiteralExpression prefix = LiteralExpression.newConstant(prefixValue);

        final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        final HasBase64PrefixFunction function = createFunction(binary, prefix);

        final boolean evaluate = function.evaluate(new SingleKeyValueTuple(), ptr);

        assertEquals("evaluate", true, evaluate);
        assertArrayEquals("result", PDataType.FALSE_BYTES, ptr.get());
    }

    private HasBase64PrefixFunction createFunction(Expression binary, LiteralExpression prefix) {
        return new HasBase64PrefixFunction(asList(binary, prefix));
    }

}