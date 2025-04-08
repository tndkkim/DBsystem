package dbms.util;

import java.nio.ByteBuffer;

public class ByteUtils {

    public static byte[] pointerToBytes(int blockNumber, short offsetInBlock) {
        byte[] result = new byte[Constants.POINTER_SIZE]; // 6바이트
        result[0] = (byte) ((blockNumber >> 24) & 0xFF);
        result[1] = (byte) ((blockNumber >> 16) & 0xFF);
        result[2] = (byte) ((blockNumber >> 8) & 0xFF);
        result[3] = (byte) (blockNumber & 0xFF);

        result[4] = (byte) ((offsetInBlock >> 8) & 0xFF);
        result[5] = (byte) (offsetInBlock & 0xFF);

        return result;
    }

    public static int getBlockNumberFromPointer(byte[] pointerBytes) {
        if (pointerBytes == null || pointerBytes.length < 4) {
            return -1;
        }

        return ((pointerBytes[0] & 0xFF) << 24) |
                ((pointerBytes[1] & 0xFF) << 16) |
                ((pointerBytes[2] & 0xFF) << 8) |
                (pointerBytes[3] & 0xFF);
    }

    public static short getOffsetFromPointer(byte[] pointerBytes) {
        if (pointerBytes == null || pointerBytes.length < 6) {
            return -1;
        }

        return (short) (((pointerBytes[4] & 0xFF) << 8) |
                (pointerBytes[5] & 0xFF));
    }

    public static byte[] fileOffsetToPointerBytes(int fileOffset) {
        int blockNumber = fileOffset / Constants.BLOCK_SIZE;
        short offsetInBlock = (short) (fileOffset % Constants.BLOCK_SIZE);

        return pointerToBytes(blockNumber, offsetInBlock);
    }

    public static int pointerBytesToFileOffset(byte[] pointerBytes) {
        int blockNumber = getBlockNumberFromPointer(pointerBytes);
        int offsetInBlock = getOffsetFromPointer(pointerBytes);

        if (blockNumber < 0 || offsetInBlock < 0) {
            return -1;
        }

        return blockNumber * Constants.BLOCK_SIZE + offsetInBlock;
    }

    public static byte[] stringToBytes(String str, int length) {
        byte[] result = new byte[length];

        if (str == null) {
            return result;
        }

        byte[] strBytes = str.getBytes();

        int copyLength = Math.min(strBytes.length, length);

        System.arraycopy(strBytes, 0, result, 0, copyLength);

        return result;
    }

    public static String bytesToString(byte[] bytes, int offset, int length) {
        if (bytes == null || offset < 0 || offset + length > bytes.length) {
            return null;
        }
        int validLength = length;
        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] == 0) {
                validLength = i;
                break;
            }
        }

        return new String(bytes, offset, validLength);
    }

    public static byte createNullBitmap(boolean[] isNull) {
        byte bitmap = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (isNull[i]) {
                bitmap |= (1 << i);
            }
        }
        return bitmap;
    }

    public static boolean isFieldNull(byte bitmap, int fieldIndex) {
        return (bitmap & (1 << fieldIndex)) != 0;
    }
}