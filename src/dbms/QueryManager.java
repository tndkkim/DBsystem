package dbms;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class QueryManager {
    private RecordManager recordManager;
    private MetadataManager metadataManager;

    public QueryManager(RecordManager recordManager, MetadataManager metadataManager) {
        this.recordManager = recordManager;
        this.metadataManager = metadataManager;
    }

    public void processFieldSearch(String fileName, String fieldName) {
        try {
            // 필드 인덱스 확인
            int fieldIndex = metadataManager.getFieldIndex(fileName, fieldName);
            if (fieldIndex == -1) {
                System.out.println("존재하지 않는 필드입니다: " + fieldName);
                return;
            }

            // 검색 실행
            List<String> results = recordManager.searchField(fileName, fieldName);

            // 결과 출력
            System.out.println("파일: " + fileName + ", 필드: " + fieldName);
            System.out.println("검색 결과:");
            for (int i = 0; i < results.size(); i++) {
                System.out.println((i + 1) + ". " + (results.get(i) == null ? "null" : results.get(i)));
            }

        } catch (IOException | SQLException e) {
            System.err.println("필드 검색 중 오류 발생: " + e.getMessage());
        }
    }

    public void processRecordSearch(String fileName, String minKey, String maxKey) {
        try {
            // 검색 실행
            List<Record> results = recordManager.searchRecords(fileName, minKey, maxKey);

            // 결과 출력
            System.out.println("파일: " + fileName + ", 검색 범위: " + minKey + " ~ " + maxKey);
            System.out.println("검색 결과:");
            for (int i = 0; i < results.size(); i++) {
                System.out.println((i + 1) + ". " + results.get(i).toString());
            }

            System.out.println("총 " + results.size() + "개의 레코드가 검색되었습니다.");

        } catch (IOException | SQLException e) {
            System.err.println("레코드 검색 중 오류 발생: " + e.getMessage());
        }
    }
}