package com.mangatayy.tools.data.aggregate.util;

import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;
import org.springframework.jdbc.support.JdbcUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

/**
 * @author yuyong
 * @date 2022/9/1 20:00
 */
public class SqlUtil {

    private final static String sp = " | ";
    private final static String swl = "\n";

    private final static MyScanner scanner = new MyScanner();

    private final static int maxLineSize = 200;

    public static void executeScript(String[] args, Properties info, String url) {
        Driver driver = new Driver();
        try (PgConnection con = (PgConnection) driver.connect(url, info)) {
            assert con != null;
            byte[] fileBytes = Files.readAllBytes(Paths.get(args[0]));
            writeOut("execute start");
            Statement statement = con.createStatement();
            boolean flag = statement.execute(new String(fileBytes));
            if (flag) {
                writeResultSet(statement.getResultSet());
            }
            writeOut("execute finish");
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        String url = "jdbc:postgresql://10.159.55.40:5432/netoptdatabase?reWriteBatchedInserts=true";
        String user = "airetina";
        String password = "a_9R92gqKT";

        writeOut("jdbc-url:");
        String urlInput = scanner.nextLine();
        if (isNotBlankStr(urlInput)) {
            url = urlInput;
        }
        writeOut("user:");
        String userInput = scanner.nextLine();
        if (isNotBlankStr(userInput)) {
            user = userInput;
        }
        writeOut("password:");
        String passwordInput = scanner.nextLine();
        if (isNotBlankStr(passwordInput)) {
            password = passwordInput;
        }
        Driver driver = new Driver();
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        if (args != null && args.length > 0) {
            executeScript(args, info, url);
            System.exit(0);
        }
        try (PgConnection con = (PgConnection) driver.connect(url, info)) {
            assert con != null;
            writeOut("connect success, please input sql: ");
            Statement statement = con.createStatement();
            while (scanner.hasNextLine()) {
                String sql = scanner.nextLine();
                if (isNotBlankStr(sql)) {
                    writeOut("please wait ......");
                    try {
                        boolean result = statement.execute(sql);
                        if (!result) {
                            int updateCount = statement.getUpdateCount();
                            writeOut("update rows: {" + updateCount + "}, please input new sql...");
                            continue;
                        }
                        writeResultSet(statement.getResultSet());
                        writeOut("query finish, please input new sql: ");
                    } catch (Exception e) {
                        writeOut("ERROR : query error: " + e.getCause());
                        writeOut("wait to query new sql");
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder out = new StringBuilder();
        out.append(swl);
        for (int i = 0; i < columnCount - 1; ++i) {
            out.append(JdbcUtils.lookupColumnName(metaData, i + 1)).append(sp);
        }
        out.append(JdbcUtils.lookupColumnName(metaData, columnCount)).append(swl);
        out.append(partResultSet(resultSet, columnCount));
        writeOut(out.toString());
        writeOut("enter Y to print more lines, other keys will close this query...");
        while (scanner.hasNextLine() && !resultSet.isAfterLast()) {
            String continueFlag = scanner.nextLine();
            if ("Y".equals(continueFlag) || "y".equals(continueFlag)) {
                writeOut(partResultSet(resultSet, columnCount).toString());
            } else {
                break;
            }
            writeOut("enter Y to print more lines, other keys will close this query...");
        }
        resultSet.close();
    }

    private static StringBuilder partResultSet(ResultSet resultSet, int totalColumnCount) throws SQLException {
        StringBuilder out = new StringBuilder();
        int row = 0;
        while (row < SqlUtil.maxLineSize && resultSet.next()) {
            for (int i = 0; i < totalColumnCount - 1; ++i) {
                out.append(resultSet.getObject(i + 1)).append(sp);
            }
            out.append(resultSet.getObject(totalColumnCount)).append(swl);
            row++;
        }
        return out;
    }

    private static void writeOut(String content) {
        System.out.println("gp-sql>> " + content);
    }

    private static boolean isNotBlankStr(String str) {
        return str != null && !str.isEmpty();
    }

    public static class MyScanner {
        private final Scanner scanner = new Scanner(System.in);

        public String nextLine() {

            String input = scanner.nextLine();
            if ("exit".equals(input) || "bye".equals(input) || "quit".equals(input)) {
                writeOut("quit");
                System.exit(0);
            }

            return input;
        }

        public boolean hasNextLine() {
            return scanner.hasNextLine();
        }
    }

}
