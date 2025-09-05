package moonlink.client;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import com.google.protobuf.ByteString;

public class MoonlinkRowConverter {
    public static moonlink.Row.MoonlinkRow.Builder convert(Object value, Schema schema) {
        if (value instanceof Map) {
            return convertMapRoot((Map<?, ?>) value, schema);
        } else if (value instanceof Struct) {
            return convertStructRoot((Struct) value, schema);
        }
        throw new UnsupportedOperationException("Unsupported root value type: " + (value == null ? "null" : value.getClass().getName()));
    }

    public static moonlink.Row.MoonlinkRow.Builder convertMapRoot(Map<?, ?> value, Schema schema) {
        moonlink.Row.MoonlinkRow.Builder rowBuilder = moonlink.Row.MoonlinkRow.newBuilder();
        for (Field field : schema.getFields()) {
            Object fieldValue = value == null ? null : value.get(field.getName());
            moonlink.Row.RowValue rv = convertFieldValue(fieldValue, field);
            rowBuilder.addValues(rv);
        }
        return rowBuilder;
    }

    public static moonlink.Row.MoonlinkRow.Builder convertStructRoot(Struct value, Schema schema) {
        moonlink.Row.MoonlinkRow.Builder rowBuilder = moonlink.Row.MoonlinkRow.newBuilder();
        for (Field field : schema.getFields()) {
            Object fieldValue = null;
            if (value != null && value.schema() != null && value.schema().field(field.getName()) != null) {
                try {
                    fieldValue = value.get(field.getName());
                } catch (DataException ignored) {
                    fieldValue = null;
                }
            }
            moonlink.Row.RowValue rv = convertFieldValue(fieldValue, field);
            rowBuilder.addValues(rv);
        }
        return rowBuilder;
    }

    private static moonlink.Row.RowValue convertFieldValue(Object value, Field field) {
        if (value == null) {
            return moonlink.Row.RowValue.newBuilder().setNull(moonlink.Row.Null.newBuilder().build()).build();
        }

        ArrowType type = field.getType();
        if (type instanceof ArrowType.Struct) {
            return convertStructValue(value, field);
        }
        else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return convertUtf8Value(value);
        }
        else if (type instanceof ArrowType.Int) {
            return convertIntValue(value, (ArrowType.Int) type);
        }
        else if (type instanceof ArrowType.Bool) {
            return convertBoolValue(value);
        }

        // enumerate unsupported types explicitly
        else if (type instanceof ArrowType.FloatingPoint) throw unsupported(field, type);
        else if (type instanceof ArrowType.Decimal) throw unsupported(field, type);
        else if (type instanceof ArrowType.Date) throw unsupported(field, type);
        else if (type instanceof ArrowType.Time) throw unsupported(field, type);
        else if (type instanceof ArrowType.Timestamp) throw unsupported(field, type);
        else if (type instanceof ArrowType.Interval) throw unsupported(field, type);
        else if (type instanceof ArrowType.Duration) throw unsupported(field, type);
        else if (type instanceof ArrowType.Binary) throw unsupported(field, type);
        else if (type instanceof ArrowType.LargeBinary) throw unsupported(field, type);
        else if (type instanceof ArrowType.FixedSizeBinary) throw unsupported(field, type);
        else if (type instanceof ArrowType.List) throw unsupported(field, type);
        else if (type instanceof ArrowType.LargeList) throw unsupported(field, type);
        else if (type instanceof ArrowType.FixedSizeList) throw unsupported(field, type);
        else if (type instanceof ArrowType.Map) throw unsupported(field, type);
        else if (type instanceof ArrowType.Union) throw unsupported(field, type);
        else if (type instanceof ArrowType.Null) throw unsupported(field, type);
        else {
            throw unsupported(field, type);
        }

    }

    private static moonlink.Row.RowValue convertStructValue(Object value, Field structField) {
        moonlink.Row.Struct.Builder structBuilder = moonlink.Row.Struct.newBuilder();
        List<Field> children = structField.getChildren();
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            for (Field child : children) {
                Object childValue = map.get(child.getName());
                structBuilder.addFields(convertFieldValue(childValue, child));
            }
        } else if (value instanceof Struct) {
            Struct struct = (Struct) value;
            for (Field child : children) {
                Object childValue = null;
                if (struct.schema() != null && struct.schema().field(child.getName()) != null) {
                    try {
                        childValue = struct.get(child.getName());
                    } catch (DataException ignored) {
                        childValue = null;
                    }
                }
                structBuilder.addFields(convertFieldValue(childValue, child));
            }
        } else {
            throw new UnsupportedOperationException("Cannot convert to struct from: " + value.getClass().getName());
        }
        return moonlink.Row.RowValue.newBuilder().setStruct(structBuilder.build()).build();
    }

    private static moonlink.Row.RowValue convertUtf8Value(Object value) {
        if (value instanceof CharSequence) {
            ByteString bytes = ByteString.copyFromUtf8(value.toString());
            return moonlink.Row.RowValue.newBuilder().setBytes(bytes).build();
        }
        throw new UnsupportedOperationException("Cannot convert value to text (Utf8): " + value.getClass().getName());
    }

    private static moonlink.Row.RowValue convertIntValue(Object value, ArrowType.Int intType) {
        if (!intType.getIsSigned()) {
            throw new UnsupportedOperationException("Unsupported unsigned integer for field");
        }
        int bitWidth = intType.getBitWidth();
        if (bitWidth <= 32) {
            int v = toInt(value);
            return moonlink.Row.RowValue.newBuilder().setInt32(v).build();
        } else if (bitWidth <= 64) {
            long v = toLong(value);
            return moonlink.Row.RowValue.newBuilder().setInt64(v).build();
        } else {
            throw new UnsupportedOperationException("Unsupported integer width: " + bitWidth);
        }
    }

    private static moonlink.Row.RowValue convertBoolValue(Object value) {
        if (value instanceof Boolean) {
            return moonlink.Row.RowValue.newBuilder().setBool(((Boolean) value).booleanValue()).build();
        }
        if (value instanceof CharSequence) {
            String s = value.toString().trim().toLowerCase();
            if ("true".equals(s) || "false".equals(s)) {
                return moonlink.Row.RowValue.newBuilder().setBool(Boolean.parseBoolean(s)).build();
            }
            throw new UnsupportedOperationException("Cannot parse boolean from string: '" + value + "'");
        }
        throw new UnsupportedOperationException("Cannot convert to boolean: " + value.getClass().getName());
    }

    private static int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof CharSequence) {
            return Integer.parseInt(value.toString());
        }
        throw new UnsupportedOperationException("Cannot convert to int32: " + value.getClass().getName());
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof CharSequence) {
            return Long.parseLong(value.toString());
        }
        throw new UnsupportedOperationException("Cannot convert to int64: " + value.getClass().getName());
    }

    private static UnsupportedOperationException unsupported(Field field, ArrowType type) {
        String fieldName = field == null ? "<unknown>" : field.getName();
        String typeName = type == null ? "<unknown>" : type.toString();
        return new UnsupportedOperationException("Unsupported Arrow type for field '" + fieldName + "': " + typeName);
    }

    private static byte[] buildMockMoonlinkRowBytes(List<Dto.FieldSchema> schemaFields) {
        moonlink.Row.MoonlinkRow.Builder rowBuilder = moonlink.Row.MoonlinkRow.newBuilder();
        for (Dto.FieldSchema f : schemaFields) {
            String dt = f.dataType == null ? "" : f.dataType.toLowerCase();
            moonlink.Row.RowValue.Builder rv = moonlink.Row.RowValue.newBuilder();
            if (dt.equals("int32")) {
                rv.setInt32(1);
            } else if (dt.equals("int64")) {
                rv.setInt64(1L);
            } else if (dt.equals("float32")) {
                rv.setFloat32(1.0f);
            } else if (dt.equals("float64")) {
                rv.setFloat64(1.0d);
            } else if (dt.equals("boolean") || dt.equals("bool")) {
                rv.setBool(true);
            } else if (dt.equals("string") || dt.equals("text")) {
                rv.setBytes(com.google.protobuf.ByteString.copyFromUtf8("mock"));
            } else if (dt.equals("date32")) {
                rv.setInt32(1); // days since epoch
            } else if (dt.startsWith("decimal(")) {
                // 16 zero bytes for decimal128 two's complement
                rv.setDecimal128Be(com.google.protobuf.ByteString.copyFrom(new byte[16]));
            } else {
                rv.setNull(moonlink.Row.Null.newBuilder().build());
            }
            rowBuilder.addValues(rv.build());
        }
        moonlink.Row.MoonlinkRow row = rowBuilder.build();
        return row.toByteArray();
    }
}


