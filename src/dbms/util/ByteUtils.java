package dbms.util;

import java.nio.ByteBuffer;

public class ByteUtils {

    // integer값 -> 4byte 배열 변환
    public static byte[] intToBytes(int value) {
        byte[] result = new byte[4];
        result[0] = (byte) ((value >> 24) & 0xFF);
        result[1] = (byte) ((value >> 16) & 0xFF);
        result[2] = (byte) ((value >> 8) & 0xFF);
        result[3] = (byte) (value & 0xFF);
        return result;
    }

    // byte 배열 -> int값
    public static int bytesToInt(byte[] bytes) {
        // 바이트 배열 길이 검사
        if (bytes == null || bytes.length < 4) {
            System.err.println("경고: int로 변환하기에 부적절한 바이트 배열 (길이: " +
                    (bytes == null ? "null" : bytes.length) + ")");
            return -1; // 오류 값 반환
        }

        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        // 바이트 배열 범위 검사
        if (bytes == null || offset < 0 || offset + 4 > bytes.length) {
            System.err.println("경고: int로 변환하기에 부적절한 바이트 배열 범위 (배열 길이: " +
                    (bytes == null ? "null" : bytes.length) +
                    ", 오프셋: " + offset + ")");
            return -1; // 오류 값 반환
        }

        return ((bytes[offset] & 0xFF) << 24) |
                ((bytes[offset + 1] & 0xFF) << 16) |
                ((bytes[offset + 2] & 0xFF) << 8) |
                (bytes[offset + 3] & 0xFF);
    }

    // 필드 값(str)-> byte
    public static byte[] stringToBytes(String str, int length) {
        byte[] result = new byte[length];

        // 널 체크
        if (str == null) {
            return result; // 길이가 length인 0으로 채워진 배열 반환
        }

        // 문자열을 바이트로 변환
        byte[] strBytes = str.getBytes();

        // 길이 제한
        int copyLength = Math.min(strBytes.length, length);

        // 바이트 복사
        System.arraycopy(strBytes, 0, result, 0, copyLength);

        return result;
    }

    /**
     * 고정 길이 바이트 배열을 문자열로 변환
     */
    public static String bytesToString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        // 끝의 0 바이트 제거 (null 종료 문자열처럼)
        int length = bytes.length;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0) {
                length = i;
                break;
            }
        }

        return new String(bytes, 0, length);
    }

    /**
     * 특정 위치의 바이트 배열을 문자열로 변환
     */
    public static String bytesToString(byte[] bytes, int offset, int length) {
        if (bytes == null || offset < 0 || offset + length > bytes.length) {
            return null;
        }

        // 유효한 데이터 길이 계산 (0 바이트까지)
        int validLength = length;
        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] == 0) {
                validLength = i;
                break;
            }
        }

        return new String(bytes, offset, validLength);
    }

    //null bitmap 생성
    public static byte createNullBitmap(boolean[] isNull) {
        // i번째 필드값이 null인 경우 i번째 null bit를 1로 설정
        byte bitmap = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (isNull[i]) {
                bitmap |= (1 << i);
            }
        }
        return bitmap;
    }

    // null bitmap에서 레코드값이 null인지 확인용
    public static boolean isFieldNull(byte bitmap, int fieldIndex) {
        return (bitmap & (1 << fieldIndex)) != 0;
    }
}