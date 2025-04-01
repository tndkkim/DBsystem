package dbms;

import dbms.util.ByteUtils;
import dbms.util.Constants;

import java.util.Arrays;

public class Block {
    private byte[] data;

    public Block() { //모든 바이트가 0인 블록 생성
        this.data = new byte[Constants.BLOCK_SIZE];
        Arrays.fill(data, (byte) 0);
    }

    public Block(byte[] data) {
        if (data.length != Constants.BLOCK_SIZE) { // 데이터 길이 == 블록 크기면 데이터를 블록에 저장
            throw new IllegalArgumentException("Block data must be " + Constants.BLOCK_SIZE + " bytes");
        }
        this.data = data;
    }

    //헤더 블록 초기화
    public void initializeHeaderBlock() {
        // 헤더 블록의 초기값: 첫 레코드 포인터는 -1
        setPointer(Constants.HEADER_POINTER_OFFSET, -1);
    }

    //헤더블록에서 첫 레코드 포인터값 반환
    public int getFirstRecordPointer() {
        return getPointer(Constants.HEADER_POINTER_OFFSET);
    }

    public void setFirstRecordPointer(int pointer) {
        setPointer(Constants.HEADER_POINTER_OFFSET, pointer);
    }

    //블록 내의 특정 오프셋에 포인터 설정
    public void setPointer(int offset, int pointer) {
        byte[] pointerBytes = ByteUtils.intToBytes(pointer);
        System.arraycopy(pointerBytes, 0, data, offset, Constants.POINTER_SIZE);
    }

    //특정 오프셋에서 포인터 읽기
    public int getPointer(int offset) {
        return ByteUtils.bytesToInt(data, offset);
    }

    //블록 내 가용 공간 검색
    private int findFreeSpaceOffset() {
        // 단순 구현: 첫 0 바이트를 찾음
        for (int i = 0; i < data.length; i++) {
            if (data[i] == 0) {
                // 첫 번째 오프셋이 0인 경우는 예외 처리 (헤더 블록에서 포인터가 -1일 경우)
                if (i == 0 && i + Constants.POINTER_SIZE <= data.length) {
                    boolean isAllZero = true;
                    for (int j = 0; j < Constants.POINTER_SIZE; j++) {
                        if (data[i + j] != 0) {
                            isAllZero = false;
                            break;
                        }
                    }
                    if (!isAllZero) {
                        continue;
                    }
                }
                return i;
            }
        }
        return data.length; // 모든 바이트가 사용 중
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        if (data.length != Constants.BLOCK_SIZE) {
            throw new IllegalArgumentException("Block data must be " + Constants.BLOCK_SIZE + " bytes");
        }
        this.data = data;
    }
}