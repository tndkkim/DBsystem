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
        this.nextPointer = -1; // 초기값은 -1 (다음 레코드 없음)

        // Null 비트맵 생성
        boolean[] isNull = new boolean[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            isNull[i] = fieldValues.get(i) == null || Constants.NULL_VALUE.equals(fieldValues.get(i));
        }
        this.nullBitmap = ByteUtils.createNullBitmap(isNull);
    }

    /**
     * 레코드를 바이트 배열로 변환
     */
    public byte[] toBytes() {
        int recordSize = calculateRecordSize();
        byte[] recordBytes = new byte[recordSize];

        // 1. Null 비트맵 추가
        recordBytes[0] = nullBitmap;

        int offset = Constants.NULL_BITMAP_SIZE;

        // 2. 각 필드 값 추가 (null이 아닌 경우만)
        for (int i = 0; i < fieldValues.size(); i++) {
            if (!ByteUtils.isFieldNull(nullBitmap, i)) {
                byte[] fieldBytes = ByteUtils.stringToBytes(fieldValues.get(i), fieldLengths.get(i));
                System.arraycopy(fieldBytes, 0, recordBytes, offset, fieldLengths.get(i));
                offset += fieldLengths.get(i);
            }
        }

        // 3. 다음 레코드 포인터 추가
        byte[] pointerBytes = ByteUtils.intToBytes(nextPointer);
        System.arraycopy(pointerBytes, 0, recordBytes, offset, Constants.POINTER_SIZE);

        return recordBytes;
    }

    /**
     * 바이트 배열에서 레코드 파싱
     */
    public static Record fromBytes(byte[] bytes, List<Integer> fieldLengths, List<String> fieldNames) {
        // 1. Null 비트맵 읽기
        byte nullBitmap = bytes[0];

        // 2. 필드 값 읽기
        List<String> fieldValues = new ArrayList<>();
        int offset = Constants.NULL_BITMAP_SIZE;

        for (int i = 0; i < fieldLengths.size(); i++) {
            if (ByteUtils.isFieldNull(nullBitmap, i)) {
                fieldValues.add(Constants.NULL_VALUE);
            } else {
                // bytes 배열의 경계 검사
                if (offset + fieldLengths.get(i) > bytes.length) {
                    System.out.println("경고: 필드 " + i + " 읽기 중 배열 범위 초과, null로 처리됨");
                    fieldValues.add(Constants.NULL_VALUE);
                    continue;
                }

                String value = ByteUtils.bytesToString(bytes, offset, fieldLengths.get(i));
                fieldValues.add(value);
                offset += fieldLengths.get(i);
            }
        }

        // 3. 다음 레코드 포인터 읽기
        int nextPointer;
        // 포인터 읽기 전 경계 검사
        if (offset + Constants.POINTER_SIZE <= bytes.length) {
            nextPointer = ByteUtils.bytesToInt(bytes, offset);
        } else {
            System.out.println("경고: 포인터 읽기 중 배열 범위 초과, -1로 설정됨");
            nextPointer = -1;
        }

        Record record = new Record(fieldValues, fieldLengths, fieldNames);
        record.setNextPointer(nextPointer);

        return record;
    }

    /**
     * 레코드 크기 계산
     */
    public int calculateRecordSize() {
        int size = Constants.NULL_BITMAP_SIZE;

        // null이 아닌 필드만 크기에 포함
        for (int i = 0; i < fieldValues.size(); i++) {
            if (!ByteUtils.isFieldNull(nullBitmap, i)) {
                size += fieldLengths.get(i);
            }
        }

        // 다음 레코드 포인터 크기 추가
        size += Constants.POINTER_SIZE;

        return size;
    }

    /**
     * 특정 필드의 값 가져오기
     */
    public String getFieldValue(String fieldName) {
        int index = fieldNames.indexOf(fieldName);
        if (index != -1) {
            return fieldValues.get(index);
        }
        return null;
    }

    /**
     * 레코드 문자열 표현
     */
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

    // Getter와 Setter 메서드
    public int getNextPointer() {
        return nextPointer;
    }

    public void setNextPointer(int nextPointer) {
        // 자기 참조 방지 (무한 루프 방지)
        if (nextPointer >= 0 && nextPointer == Constants.BLOCK_SIZE) {
            System.out.println("경고: 자기 참조 포인터 감지. 다음 포인터를 -1로 설정합니다.");
            this.nextPointer = -1;
        } else {
            this.nextPointer = nextPointer;
        }
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public String getSearchKey() {
        // 첫 번째 필드가 검색 키
        return fieldValues.get(0);
    }
}