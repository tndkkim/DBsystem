package dbms;

import dbms.util.Constants;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/DBsystem";
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "2264";

    private static MetadataManager metadataManager;
    private static DiskFileManager diskFileManager;
    private static RecordManager recordManager;
    private static QueryManager queryManager;

    public static void main(String[] args) {
        try {
            metadataManager = new MetadataManager(JDBC_URL, DB_USERNAME, DB_PASSWORD);
            diskFileManager = new DiskFileManager(metadataManager);
            recordManager = new RecordManager(diskFileManager, metadataManager);
            queryManager = new QueryManager(recordManager, metadataManager);

            startUserInterface();
            metadataManager.close();

        } catch (SQLException e) {
            System.err.println("데이터베이스 연결 오류: " + e.getMessage());
        }
    }

    private static void startUserInterface() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n======= 데이터베이스 시스템 =======");
            System.out.println("0. 종료");
            System.out.println("1. 파일 생성");
            System.out.println("2. 레코드 삽입 (bulk load)");
            System.out.println("3. 필드 검색");
            System.out.println("4. 레코드 검색 (search key 범위 지정)");
            System.out.print("선택하세요: ");

            String choice = scanner.nextLine();

            switch (choice) {
                case "1":
                    createFile(scanner);
                    break;
                case "2":
                    insertRecords(scanner);
                    break;
                case "3":
                    searchField(scanner);
                    break;
                case "4":
                    searchRecords(scanner);
                    break;
                case "0":
                    System.out.println("프로그램을 종료합니다.");
                    return;
                default:
                    System.out.println("잘못된 입력입니다. 다시 시도하세요.");
            }
        }
    }

    private static void createFile(Scanner scanner) {
        try {
            System.out.println("\n=== 파일 생성 ===");

            System.out.print("파일명 입력: ");
            String fileName = scanner.nextLine();

            System.out.print("필드 수 입력: ");
            int fieldCount = Integer.parseInt(scanner.nextLine());

            List<String> fieldNames = new ArrayList<>();
            List<String> fieldTypes = new ArrayList<>();
            List<Integer> fieldLengths = new ArrayList<>();

            for (int i = 0; i < fieldCount; i++) {
                System.out.println((i + 1) + "번째 필드 정보 입력");

                System.out.print("필드 이름: ");
                String fieldName = scanner.nextLine();
                fieldNames.add(fieldName);

                // 고정길이 문자열 타입만 사용
                fieldTypes.add(Constants.CHAR_TYPE);

                System.out.print("필드 길이 (문자 수): ");
                int fieldLength = Integer.parseInt(scanner.nextLine());
                fieldLengths.add(fieldLength);
            }

            // 순차 파일 생성 + 헤더블록 초기화
            diskFileManager.createSequentialFile(fileName, fieldNames, fieldTypes, fieldLengths);

            // MySQL 테이블 생성
            metadataManager.createTable(fileName, fieldNames, fieldLengths);

            System.out.println("파일이 성공적으로 생성되었습니다.");

        } catch (NumberFormatException e) {
            System.err.println("잘못된 숫자 형식입니다.");
        } catch (IOException | SQLException e) {
            System.err.println("파일 생성 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 레코드 삽입 기능
     */
    private static void insertRecords(Scanner scanner) {
        try {
            System.out.println("\n=== 레코드 삽입 ===");

            System.out.print("파일명 입력: ");
            String fileName = scanner.nextLine();

            // 파일 존재 확인
            if (!metadataManager.fileExists(fileName)) {
                System.out.println("존재하지 않는 파일입니다.");
                return;
            }

            System.out.print("데이터 파일 경로 입력: ");
            String dataFilePath = scanner.nextLine();
            dataFilePath = Constants.RESOURCE_DIRECTORY + dataFilePath;

            // 레코드 삽입
            recordManager.bulkInsertRecords(fileName, dataFilePath);

        } catch (IOException | SQLException e) {
            System.err.println("레코드 삽입 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 필드 검색 기능
     */
    private static void searchField(Scanner scanner) {
        System.out.println("\n=== 필드 검색 ===");

        System.out.print("파일명 입력: ");
        String fileName = scanner.nextLine();

        try {
            // 파일 존재 확인
            if (!metadataManager.fileExists(fileName)) {
                System.out.println("존재하지 않는 파일입니다.");
                return;
            }

            // 필드 목록 출력
            List<String> fieldNames = metadataManager.getFieldNames(fileName);
            System.out.println("사용 가능한 필드:");
            for (int i = 0; i < fieldNames.size(); i++) {
                System.out.println((i + 1) + ". " + fieldNames.get(i));
            }

            System.out.print("검색할 필드 이름 입력: ");
            String fieldName = scanner.nextLine();

            // 필드 검색 실행
            queryManager.processFieldSearch(fileName, fieldName);

        } catch (SQLException e) {
            System.err.println("필드 검색 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 레코드 검색 기능
     */
    private static void searchRecords(Scanner scanner) {
        System.out.println("\n=== 레코드 검색 (범위) ===");

        System.out.print("파일명 입력: ");
        String fileName = scanner.nextLine();

        try {
            // 파일 존재 확인
            if (!metadataManager.fileExists(fileName)) {
                System.out.println("존재하지 않는 파일입니다.");
                return;
            }

            // 첫 번째 필드가 검색 키
            String searchKeyField = metadataManager.getFieldNames(fileName).get(0);
            System.out.println("검색 키 필드: " + searchKeyField);

            System.out.print("최소값 입력: ");
            String minKey = scanner.nextLine();

            System.out.print("최대값 입력: ");
            String maxKey = scanner.nextLine();

            // 레코드 검색 실행
            queryManager.processRecordSearch(fileName, minKey, maxKey);

        } catch (SQLException e) {
            System.err.println("레코드 검색 중 오류 발생: " + e.getMessage());
        }
    }
}