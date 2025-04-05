package dbms;

import dbms.util.Constants;
import java.util.Arrays;

/**
 * 디스크 블록을 표현하는 클래스
 */
public class Block {
    private byte[] data;

    /**
     * 기본 생성자 - 빈 블록 생성
     */
    public Block() {
        // 블록 크기만큼 바이트 배열 생성
        this.data = new byte[Constants.BLOCK_SIZE];
        // 0으로 초기화
        Arrays.fill(this.data, (byte) 0);
    }

    /**
     * 바이트 배열로 블록 생성
     */
    public Block(byte[] data) {
        if (data == null) {
            this.data = new byte[Constants.BLOCK_SIZE];
            Arrays.fill(this.data, (byte) 0);
        } else if (data.length != Constants.BLOCK_SIZE) {
            // 데이터 길이가 블록 크기와 다른 경우 경고
            System.err.println("경고: 데이터 길이가 블록 크기와 다릅니다");
            this.data = new byte[Constants.BLOCK_SIZE];
            // 데이터 복사 (가능한 만큼)
            System.arraycopy(data, 0, this.data, 0, Math.min(data.length, Constants.BLOCK_SIZE));
        } else {
            this.data = data;
        }
    }

    /**
     * 헤더 블록 초기화
     */
    public void initializeHeaderBlock() {
        // 블록 데이터 0으로 초기화
        Arrays.fill(data, (byte) 0);

        // 첫 레코드 포인터를 -1로 설정 (레코드 없음)
        setFirstRecordPointer(-1);
    }

    /**
     * 첫 레코드 포인터 설정
     */
    public void setFirstRecordPointer(int pointer) {
        setPointer(pointer);
    }

    /**
     * 포인터 값 설정 (블록의 첫 부분에 4바이트)
     */
    public void setPointer(int value) {
        // 데이터 배열 크기 검사
        if (data.length < Constants.POINTER_SIZE) {
            System.err.println("오류: 데이터 배열 크기가 포인터 크기보다 작습니다");
            return;
        }

        // 포인터 값을 바이트 배열로 변환하여 직접 설정
        data[0] = (byte)((value >> 24) & 0xFF);
        data[1] = (byte)((value >> 16) & 0xFF);
        data[2] = (byte)((value >> 8) & 0xFF);
        data[3] = (byte)(value & 0xFF);
    }

    /**
     * 첫 레코드 포인터 가져오기
     */
    public int getFirstRecordPointer() {
        return getPointer();
    }

    /**
     * 포인터 값 가져오기 (블록의 첫 부분에 4바이트)
     */
    public int getPointer() {
        if (data.length < Constants.POINTER_SIZE) {
            System.err.println("경고: 블록 데이터가 포인터 크기보다 작습니다");
            return -1;
        }

        // 바이트 배열에서 int 값 추출
        return ((data[0] & 0xFF) << 24) |
                ((data[1] & 0xFF) << 16) |
                ((data[2] & 0xFF) << 8) |
                (data[3] & 0xFF);
    }

    /**
     * 블록 데이터 가져오기
     */
    public byte[] getData() {
        return data;
    }
}