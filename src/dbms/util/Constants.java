package dbms.util;

public class Constants {
    public static final int BLOCK_SIZE = 64;

    public static final int POINTER_SIZE = 6; // byte
    public static final int NULL_BITMAP_SIZE = 1; // byte

    // 경로 설정
    public static final String DATA_DIRECTORY = "src/data/";
    public static final String RESOURCE_DIRECTORY = "src/dbms/resources/";

    // 헤더 블록에서 첫 레코드 포인터의 위치, 메타데이터는 JDBC를 이용하여 처리
    public static final int HEADER_POINTER_OFFSET = 0;

    // 필드 - 고정길이 문자열로 고정
    public static final String CHAR_TYPE = "CHAR";

    //txt파일(레코드 bulk load) 처리용 상수 설정
    public static final String DELIMITER = ";";
    public static final String NULL_VALUE = "null";
}