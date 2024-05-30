package hu.lanoga;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Start {
    private static final Logger logger = LoggerFactory.getLogger(Start.class);
    private static Configuration config;

    public static void main(String[] args) {
        try {
            Configurations configs = new Configurations();
            config = configs.properties("application.properties");

            String dbUrl = config.getString("db.url");
            String dbUser = config.getString("db.user");
            String dbPassword = config.getString("db.password");

            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            logger.info("Connected to the database.");

            List<String> ddlScripts = new ArrayList<>();
            List<String> dmlScripts = new ArrayList<>();

            // Get tables
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
            while (rs.next()) {
                String table = rs.getString("tablename");
                ddlScripts.add(getTableDDL(connection, table));
                dmlScripts.addAll(getTableData(connection, table));
            }

            // Get views
            rs = stmt.executeQuery("SELECT viewname FROM pg_views WHERE schemaname = 'public'");
            while (rs.next()) {
                String view = rs.getString("viewname");
                ddlScripts.add(getViewDDL(connection, view));
            }

            // Get functions
            rs = stmt.executeQuery(
                    "SELECT proname FROM pg_proc JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid) WHERE ns.nspname = 'public'");
            while (rs.next()) {
                String function = rs.getString("proname");
                ddlScripts.add(getFunctionDDL(connection, function));
            }

            // Get triggers
            rs = stmt.executeQuery("SELECT tgname FROM pg_trigger WHERE NOT tgisinternal");
            while (rs.next()) {
                String trigger = rs.getString("tgname");
                ddlScripts.add(getTriggerDDL(connection, trigger));
            }

            // Save DDL to file
            String ddlFile = "schema_name_ddl.sql";
            try (FileWriter writer = new FileWriter(ddlFile)) {
                for (String ddl : ddlScripts) {
                    writer.write(ddl + ";\n");
                }
            }
            logger.info("DDL script saved to " + ddlFile);

            // Save DML to file
            String dmlFile = "schema_name_dml.sql";
            try (FileWriter writer = new FileWriter(dmlFile)) {
                for (String dml : dmlScripts) {
                    writer.write(dml + ";\n");
                }
            }
            logger.info("DML script saved to " + dmlFile);

            // Upload files via SFTP
            uploadFileSFTP(ddlFile);
            uploadFileSFTP(dmlFile);

            connection.close();
            logger.info("Disconnected from the database.");
        } catch (SQLException | IOException | ConfigurationException e) {
            logger.error("Error occurred", e);
        }
    }

    private static String getTableDDL(Connection connection, String tableName) throws SQLException {
        String ddl = "";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT pg_get_tabledef('" + tableName + "')");
        if (rs.next()) {
            ddl = rs.getString(1);
        }
        return ddl;
    }

    private static List<String> getTableData(Connection connection, String tableName) throws SQLException {
        List<String> dmls = new ArrayList<>();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            StringBuilder dml = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
            for (int i = 1; i <= columnCount; i++) {
                int columnType = metaData.getColumnType(i);
                switch (columnType) {
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.LONGVARCHAR:
                        dml.append("'").append(rs.getString(i)).append("'");
                        break;
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.BIGINT:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.TINYINT:
                        dml.append(rs.getInt(i));
                        break;
                    case java.sql.Types.DATE:
                        dml.append("to_date('").append(rs.getString(i)).append("', 'YYYY-MM-DD')");
                        break;
                    case java.sql.Types.TIMESTAMP:
                        dml.append("to_timestamp('").append(rs.getString(i)).append("', 'YYYY-MM-DD HH24:MI:SS')");
                        break;
                    case java.sql.Types.BOOLEAN:
                        dml.append(rs.getBoolean(i));
                        break;
                    default:
                        dml.append("'").append(rs.getString(i)).append("'");
                }
                if (i < columnCount)
                    dml.append(", ");
            }
            dml.append(")");
            dmls.add(dml.toString());
        }
        return dmls;
    }

    private static String getViewDDL(Connection connection, String viewName) throws SQLException {
        String ddl = "";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT pg_get_viewdef('" + viewName + "', true)");
        if (rs.next()) {
            ddl = rs.getString(1);
        }
        return ddl;
    }

    private static String getFunctionDDL(Connection connection, String functionName) throws SQLException {
        String ddl = "";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
                "SELECT pg_get_functiondef(pg_proc.oid) FROM pg_proc WHERE proname = '" + functionName + "'");
        if (rs.next()) {
            ddl = rs.getString(1);
        }
        return ddl;
    }

    private static String getTriggerDDL(Connection connection, String triggerName) throws SQLException {
        String ddl = "";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
                "SELECT pg_get_triggerdef(pg_trigger.oid) FROM pg_trigger WHERE tgname = '" + triggerName + "'");
        if (rs.next()) {
            ddl = rs.getString(1);
        }
        return ddl;
    }

    private static void uploadFileSFTP(String filePath) {
        String sftpHost = config.getString("sftp.host");
        String sftpUser = config.getString("sftp.user");
        String sftpPassword = config.getString("sftp.password");
        int sftpPort = config.getInt("sftp.port");
        String sftpRemoteDir = config.getString("sftp.remote.dir");

        com.jcraft.jsch.JSch jsch = new com.jcraft.jsch.JSch();
        try {
            com.jcraft.jsch.Session session = jsch.getSession(sftpUser, sftpHost, sftpPort);
            session.setPassword(sftpPassword);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            logger.info("Connected to SFTP server.");

            com.jcraft.jsch.Channel channel = session.openChannel("sftp");
            channel.connect();
            com.jcraft.jsch.ChannelSftp sftpChannel = (com.jcraft.jsch.ChannelSftp) channel;
            sftpChannel.cd(sftpRemoteDir);
            sftpChannel.put(filePath, Paths.get(filePath).getFileName().toString());
            sftpChannel.disconnect();
            session.disconnect();
            logger.info("File uploaded: " + filePath);
        } catch (com.jcraft.jsch.JSchException | com.jcraft.jsch.SftpException e) {
            logger.error("SFTP upload failed", e);
        }
    }
}
