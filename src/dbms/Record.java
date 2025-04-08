package dbms;

import dbms.util.ByteUtils;
import dbms.util.Constants;

import java.util.ArrayList;
import java.util.List;

public class Record {
    private byte nullBitmap;
    private List<String> fieldValues;
    private List<Integer> fieldLengths;
    private List<String> fieldNames;
    private int nextPointer; // 다음 레코드를 가리키는 포인터

    public Record(List<String> fieldValues, List<Integer> fieldLengths, List<String> fieldNames) {
        this.fieldValues = fieldValues;
        this.fieldLengths = fieldLengths;
        this.fieldNames = fieldNames;
        this.nextPointer = -1;

        boolean[] isNull = new boolean[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            isNull[i] = fieldValues.get(i) == null || Constants.NULL_VALUE.equals(fieldValues.get(i));
        }
        this.nullBitmap = ByteUtils.createNullBitmap(isNull);
    }

    public byte[] toBytes() {

        boolean[] isNull = new boolean[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            isNull[i] = fieldValues.get(i) == null;
        }
        byte nullBitmap = ByteUtils.createNullBitmap(isNull);

        int totalSize = Constants.NULL_BITMAP_SIZE;
        for (int i = 0; i < fieldValues.size(); i++) {
            if (!isNull[i]) {
                totalSize += fieldLengths.get(i);
            }
        }
        totalSize += Constants.POINTER_SIZE;

        byte[] bytes = new byte[totalSize];

        bytes[0] = nullBitmap;

        int currentPos = Constants.NULL_BITMAP_SIZE;

        for (int i = 0; i < fieldValues.size(); i++) {
            if (!isNull[i]) {
                byte[] fieldBytes = ByteUtils.stringToBytes(fieldValues.get(i), fieldLengths.get(i));
                System.arraycopy(fieldBytes, 0, bytes, currentPos, fieldLengths.get(i));
                currentPos += fieldLengths.get(i);
            }
        }

        byte[] pointerBytes = ByteUtils.fileOffsetToPointerBytes(nextPointer);
        System.arraycopy(pointerBytes, 0, bytes, currentPos, Constants.POINTER_SIZE);

        return bytes;
    }
    public static Record fromBytes(byte[] data, List<Integer> fieldLengths, List<String> fieldNames) {
        if (data.length < Constants.NULL_BITMAP_SIZE + Constants.POINTER_SIZE) {
            System.err.println("데이터 길이가 너무 짧습니다: " + data.length);
            return null;
        }

        byte nullBitmap = data[0];

        List<String> fieldValues = new ArrayList<>();
        int currentPos = Constants.NULL_BITMAP_SIZE;

        for (int i = 0; i < fieldLengths.size(); i++) {
            boolean isNull = ByteUtils.isFieldNull(nullBitmap, i);
            if (isNull) {
                fieldValues.add(null);
            } else {
                String value = ByteUtils.bytesToString(data, currentPos, fieldLengths.get(i));
                fieldValues.add(value);
                currentPos += fieldLengths.get(i);
            }
        }

        byte[] pointerBytes = new byte[Constants.POINTER_SIZE];
        System.arraycopy(data, data.length - Constants.POINTER_SIZE, pointerBytes, 0, Constants.POINTER_SIZE);
        int nextPointer = ByteUtils.pointerBytesToFileOffset(pointerBytes);

        Record record = new Record(fieldValues, fieldLengths, fieldNames);
        record.setNextPointer(nextPointer);

        return record;
    }

    public int calculateRecordSize() {
        int size = Constants.NULL_BITMAP_SIZE;

        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldValues.get(i) != null) {
                size += fieldLengths.get(i);
            }
        }
        size += Constants.POINTER_SIZE;

        return size;
    }

    public String getFieldValue(String fieldName) {
        int index = fieldNames.indexOf(fieldName);
        if (index != -1) {
            return fieldValues.get(index);
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldNames.size(); i++) {
            sb.append(fieldNames.get(i)).append(": ").append(fieldValues.get(i));
            if (i < fieldNames.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public int getNextPointer() {
        return nextPointer;
    }

    public void setNextPointer(int nextPointer) {
        this.nextPointer = nextPointer;
    }

    public String getSearchKey() {
        return fieldValues.get(0);
    }
}