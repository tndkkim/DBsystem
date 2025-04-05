package dbms;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MetadataManager {
    private Connection connection;

    public MetadataManager(String jdbcUrl, String username, String password) throws SQLException {
        try {
            // JDBC 드라이버 로드
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 데이터베이스 연결
            this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC 드라이버를 찾을 수 없습니다.", e);
        }
    }

    public void createTable(String fileName, List<String> fieldNames, List<Integer> fieldLengths) throws SQLException {
        // 파일명이 겹치면 기존 테이블 삭제
        String dropTableSQL = "DROP TABLE IF EXISTS " + fileName;
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropTableSQL);
        }

        // 새 테이블 생성
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE " + fileName + " (");
        for (int i = 0; i < fieldNames.size(); i++) {
            createTableSQL.append(fieldNames.get(i)).append(" VARCHAR(").append(fieldLengths.get(i)).append(")");
            if (i < fieldNames.size() - 1) {
                createTableSQL.append(", ");
            }
        }
        createTableSQL.append(")");

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSQL.toString());
        }
    }

    public List<String> getFieldNames(String fileName) throws SQLException {
        List<String> fieldNames = new ArrayList<>();

        // JDBC 메타데이터 기능을 사용하여 테이블 컬럼 정보 가져오기
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, fileName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                fieldNames.add(columnName);
            }
        }

        return fieldNames;
    }

    public List<Integer> getFieldLengths(String fileName) throws SQLException {
        List<Integer> fieldLengths = new ArrayList<>();

        // JDBC 메타데이터 기능을 사용하여 테이블 컬럼 정보 가져오기
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, fileName, null)) {
            while (rs.next()) {
                // VARCHAR 타입의 길이 가져오기
                int columnSize = rs.getInt("COLUMN_SIZE");
                fieldLengths.add(columnSize);
            }
        }

        return fieldLengths;
    }

    public int getFieldCount(String fileName) throws SQLException {
        int count = 0;

        // JDBC 메타데이터 기능을 사용하여 테이블 컬럼 수 계산
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, fileName, null)) {
            while (rs.next()) {
                count++;
            }
        }

        return count;
    }

    public int getFieldIndex(String fileName, String fieldName) throws SQLException {
        int index = -1;
        int currentIndex = 0;

        // JDBC 메타데이터 기능을 사용하여 필드 인덱스 찾기
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, fileName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnName.equals(fieldName)) {
                    index = currentIndex;
                    break;
                }
                currentIndex++;
            }
        }

        return index;
    }

    public boolean fileExists(String fileName) throws SQLException {
        boolean exists = false;

        // JDBC 메타데이터 기능을 사용하여 테이블 존재 여부 확인
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, fileName, new String[]{"TABLE"})) {
            exists = rs.next(); // 결과가 있으면 테이블이 존재함
        }

        return exists;
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("데이터베이스 연결 종료 중 오류 발생: " + e.getMessage());
            }
        }
    }
}
