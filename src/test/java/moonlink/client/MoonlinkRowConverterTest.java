package moonlink.client;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

public class MoonlinkRowConverterTest {

    @Test
    public void convert_map_simple_text_and_ints() {
        Schema schema = new Schema(List.of(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("id64", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        Map<String, Object> input = new HashMap<>();
        input.put("name", "alice");
        input.put("age", 30);
        input.put("id64", 1234567890123L);

        moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(input, schema).build();

        assertEquals(3, row.getValuesCount());
        assertTrue(row.getValues(0).hasBytes());
        assertEquals("alice", row.getValues(0).getBytes().toStringUtf8());
        assertTrue(row.getValues(1).hasInt32());
        assertEquals(30, row.getValues(1).getInt32());
        assertTrue(row.getValues(2).hasInt64());
        assertEquals(1234567890123L, row.getValues(2).getInt64());
    }

    @Test
    public void convert_map_nested_struct() {
        Field nested = new Field(
            "nested",
            FieldType.nullable(new ArrowType.Struct()),
            List.of(
                new Field("nick", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );
        Schema schema = new Schema(List.of(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            nested
        ));

        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nick", "al");
        nestedMap.put("score", 99);

        Map<String, Object> input = new HashMap<>();
        input.put("name", "alice");
        input.put("nested", nestedMap);

        moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(input, schema).build();

        assertEquals(2, row.getValuesCount());
        assertTrue(row.getValues(0).hasBytes());
        assertEquals("alice", row.getValues(0).getBytes().toStringUtf8());
        assertTrue(row.getValues(1).hasStruct());
        moonlink.Row.Struct st = row.getValues(1).getStruct();
        assertEquals(2, st.getFieldsCount());
        assertTrue(st.getFields(0).hasBytes());
        assertEquals("al", st.getFields(0).getBytes().toStringUtf8());
        assertTrue(st.getFields(1).hasInt32());
        assertEquals(99, st.getFields(1).getInt32());
    }

    @Test
    public void convert_map_string_to_int_parsing() {
        Schema schema = new Schema(List.of(
            new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("id64", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        Map<String, Object> input = new HashMap<>();
        input.put("age", "42");
        input.put("id64", "9223372036854775806");

        moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(input, schema).build();

        assertEquals(2, row.getValuesCount());
        assertTrue(row.getValues(0).hasInt32());
        assertEquals(42, row.getValues(0).getInt32());
        assertTrue(row.getValues(1).hasInt64());
        assertEquals(9223372036854775806L, row.getValues(1).getInt64());
    }

    @Test
    public void convert_map_unsupported_type_bool_throws() {
        Schema schema = new Schema(List.of(
            new Field("bin", FieldType.nullable(new ArrowType.Binary()), null)
        ));
        Map<String, Object> input = new HashMap<>();
        input.put("bin", new byte[] {1, 2, 3});

        boolean threw = false;
        try {
            MoonlinkRowConverter.convert(input, schema).build();
        } catch (UnsupportedOperationException ex) {
            threw = true;
            assertTrue(ex.getMessage().contains("Unsupported Arrow type"));
        }
        assertTrue(threw);
    }

    @Test
    public void convert_map_bool_supported_true_false_and_string() {
        Schema schema = new Schema(List.of(
            new Field("b1", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("b2", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("b3", FieldType.nullable(new ArrowType.Bool()), null)
        ));
        Map<String, Object> input = new HashMap<>();
        input.put("b1", Boolean.TRUE);
        input.put("b2", Boolean.FALSE);
        input.put("b3", "true");

        moonlink.Row.MoonlinkRow row = MoonlinkRowConverter.convert(input, schema).build();

        assertEquals(3, row.getValuesCount());
        assertTrue(row.getValues(0).hasBool());
        assertTrue(row.getValues(0).getBool());
        assertTrue(row.getValues(1).hasBool());
        assertFalse(row.getValues(1).getBool());
        assertTrue(row.getValues(2).hasBool());
        assertTrue(row.getValues(2).getBool());
    }
}


