package dbms.util;

public class Constants {
    public static final int BLOCK_SIZE = 32;

    public static final int POINTER_SIZE = 4; // byte
    public static final int NULL_BITMAP_SIZE = 1; // byte

    public static final String METADATA_TABLE = "metadata";

    // 경로 설정
    public static final String DATA_DIRECTORY = "src/data/";
    public static final String RESOURCE_DIRECTORY = "src/dbms/resources/";

    // 헤더 블록에서 첫 레코드 포인터의 위치
    public static final int HEADER_POINTER_OFFSET = 0;

    // 필드 - 고정길이 문자열로 고정
    public static final String CHAR_TYPE = "CHAR";

    //txt파일(레코드 bulk load) 처리용 상수 설정
    public static final String DELIMITER = ";";
    public static final String NULL_VALUE = "null";
}