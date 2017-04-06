package com.emasphere.phoenix;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.PrefixFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * {@link PrefixFunction Function} checking that a varbinary value has the specified Base64 prefix.
 * <p>
 * The varbinary value is extracted from 0 up to the prefix length and then encoded in Base64. Then
 * this value is compared to the specified prefix encoded in Base64.
 *
 * @author Sebastien Gerard
 */
@FunctionParseNode.BuiltInFunction(
        name = HasBase64PrefixFunction.NAME,
        args = {
                @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}),
                @FunctionParseNode.Argument(allowedTypes = {PVarchar.class})
        }
)
public class HasBase64PrefixFunction extends PrefixFunction {

    /**
     * Name of this function.
     */
    public static final String NAME = "HAS_BASE64_PREFIX";

    /**
     * {@link Base64.Decoder Decoder} of base 64 format.
     */
    private static final Base64.Decoder DECODER = Base64.getDecoder();

    public HasBase64PrefixFunction() {
    }

    public HasBase64PrefixFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getPrefixExpression().evaluate(tuple, ptr)) {
            return false;
        }

        if (!getVarBinaryExpression().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return true;
        }

        final byte[] decodedPrefix = DECODER.decode(getPrefixExpression().getBytes());

        final byte[] varBinaryPrefix = Arrays.copyOf(
                ptr.get(),
                (decodedPrefix.length < ptr.getLength()) ? decodedPrefix.length : ptr.get().length
        );

        ptr.set(
                Arrays.equals(varBinaryPrefix, decodedPrefix) ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES
        );

        return true;
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

    @Override
    public Integer getMaxLength() {
        return 1;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    protected boolean extractNode() {
        return true;
    }

    /**
     * Returns the {@link LiteralExpression expression} to evaluate.
     */
    private Expression getVarBinaryExpression() {
        return children.get(0);
    }

    /**
     * Returns the {@link LiteralExpression the expected} prefix encoded in the Base64 format.
     */
    private LiteralExpression getPrefixExpression() {
        return (LiteralExpression) children.get(1);
    }
}
