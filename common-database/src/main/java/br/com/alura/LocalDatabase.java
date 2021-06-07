package br.com.alura;

import java.sql.*;

public class LocalDatabase {
    private final Connection connection;

    public LocalDatabase(String databaseName) throws SQLException {
        var url = "jdbc:sqlite:" + databaseName + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            this.connection.createStatement().execute(sql);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public boolean update(String statement, String... params) throws SQLException {
        return getPreparedStatement(statement, params).execute();
    }

    private PreparedStatement getPreparedStatement(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i++, params[i]);
        }
        return preparedStatement;
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return getPreparedStatement(query, params).executeQuery();
    }
}
