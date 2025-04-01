package dbms.util;

import java.nio.ByteBuffer;

public class ByteUtils {

    // integer값 -> 4byte 배열 변환
    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(Constants.POINTER_SIZE).putInt(value).array();
    }

    // byte 배열 -> int값
    public static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        byte[] intBytes = new byte[Constants.POINTER_SIZE];
        System.arraycopy(bytes, offset, intBytes, 0, Constants.POINTER_SIZE);
        return bytesToInt(intBytes);
    }

    // 필드 값(str)-> byte
    public static byte[] stringToBytes(String value, int length) {
        byte[] bytes = new byte[length];
        if (value != null && !value.equals(Constants.NULL_VALUE)) {
            byte[] valueBytes = value.getBytes();
            System.arraycopy(valueBytes, 0, bytes, 0, Math.min(valueBytes.length, length));
        }
        return bytes;
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes).trim();
    }

    public static String bytesToString(byte[] bytes, int offset, int length) {
        byte[] stringBytes = new byte[length];
        System.arraycopy(bytes, offset, stringBytes,0, length);
        return bytesToString(stringBytes);
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