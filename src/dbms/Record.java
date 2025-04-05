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
    /**
     * 바이트 배열에서 Record 객체 생성
     */
    public static Record fromBytes(byte[] bytes, List<Integer> fieldLengths, List<String> fieldNames) {
        if (bytes == null || bytes.length < Constants.NULL_BITMAP_SIZE + Constants.POINTER_SIZE) {
            System.err.println("유효하지 않은 레코드 바이트 배열");
            return null;
        }

        try {
            // null 비트맵 읽기 (첫 바이트)
            byte nullBitmap = bytes[0];

            // 필드 값 목록 초기화
            List<String> fieldValues = new ArrayList<>();

            // 현재 바이트 위치
            int bytePos = Constants.NULL_BITMAP_SIZE;

            // 각 필드 처리
            for (int i = 0; i < fieldNames.size(); i++) {
                // 해당 비트 확인 (1이면 null)
                boolean isNull = ((nullBitmap >> i) & 1) == 1;

                if (isNull) {
                    // null 필드는 값을 null로 설정
                    fieldValues.add(null);
                } else {
                    // 필드 길이 확인
                    int length = fieldLengths.get(i);

                    // 바이트 배열 범위 검사
                    if (bytePos + length > bytes.length - Constants.POINTER_SIZE) {
                        System.err.println("필드 데이터가 레코드 바이트 배열 범위를 초과합니다");
                        // 손상된 데이터 처리 - 빈 문자열로 대체
                        fieldValues.add("");
                        // 다음 필드 위치를 추정
                        bytePos += length;
                        continue;
                    }

                    // 필드 바이트 배열 추출
                    byte[] fieldBytes = new byte[length];
                    System.arraycopy(bytes, bytePos, fieldBytes, 0, length);

                    // 필드 바이트 배열을 문자열로 변환
                    String fieldValue = ByteUtils.bytesToString(fieldBytes);

                    // 문자열 검증 - 비인쇄 문자나 비정상 문자가 포함된 경우 필터링
                    if (fieldValue != null && !isValidString(fieldValue)) {
                        System.err.println("필드 '" + fieldNames.get(i) + "'에 비정상 문자가 포함됨: " + fieldValue);
                        // 비정상 문자 제거
                        fieldValue = sanitizeString(fieldValue);
                    }

                    fieldValues.add(fieldValue);

                    // 다음 필드 위치로 이동
                    bytePos += length;
                }
            }

            // 다음 레코드 포인터 읽기 (레코드 끝에서 4바이트)
            int pointerOffset = bytes.length - Constants.POINTER_SIZE;
            byte[] pointerBytes = new byte[Constants.POINTER_SIZE];
            System.arraycopy(bytes, pointerOffset, pointerBytes, 0, Constants.POINTER_SIZE);
            int nextPointer = ByteUtils.bytesToInt(pointerBytes);

            // 포인터 값이 비정상적이면 수정
            if (nextPointer < -1 || nextPointer == Integer.MIN_VALUE) {
                System.err.println("비정상적인 다음 포인터 값: " + nextPointer + " => -1로 수정");
                nextPointer = -1; // 마지막 레코드로 설정
            }

            // Record 객체 생성
            Record record = new Record(fieldValues, fieldLengths, fieldNames);
            record.setNextPointer(nextPointer);

            return record;
        } catch (Exception e) {
            System.err.println("레코드 생성 중 예외 발생: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 문자열이 유효한지 검사 (비인쇄 문자나 이상한 문자가 없는지)
     */
    private static boolean isValidString(String str) {
        if (str == null) return false;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            // 알파벳, 숫자, 기본 구두점, 공백만 허용
            if (!Character.isLetterOrDigit(c) && !Character.isSpaceChar(c) && ".,;:-_()[]{}".indexOf(c) == -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * 문자열에서 비정상 문자를 제거하고 정상 문자만 남김
     */
    private static String sanitizeString(String str) {
        if (str == null) return "";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            // 알파벳, 숫자, 기본 구두점, 공백만 유지
            if (Character.isLetterOrDigit(c) || Character.isSpaceChar(c) || ".,;:-_()[]{}".indexOf(c) != -1) {
                sb.append(c);
            }
        }
        return sb.toString();
    }
    /**
     * 레코드의 크기를 계산
     * (null bitmap + 필드 데이터 + 다음 레코드 포인터)
     */
    public int calculateRecordSize() {
        int size = Constants.NULL_BITMAP_SIZE; // null bitmap 크기

        // null이 아닌 필드 데이터 크기 합산
        for (int i = 0; i < fieldNames.size(); i++) {
            // null이 아닌 필드만 크기에 포함
            if (fieldValues.get(i) != null) {
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

    public int getNextPointer() {
        return nextPointer;
    }

    public void setNextPointer(int nextPointer) {
        // 자기 참조 방지 (무한 루프 방지)
        this.nextPointer = nextPointer;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public String getSearchKey() {
        // 첫 번째 필드가 검색 키
        return fieldValues.get(0);
    }
}