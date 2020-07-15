package de.biosphere.spoticord.database.impl.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory;

import de.biosphere.spoticord.Configuration;
import de.biosphere.spoticord.database.Database;
import de.biosphere.spoticord.database.dao.AlbumDao;
import de.biosphere.spoticord.database.dao.ArtistDao;
import de.biosphere.spoticord.database.dao.TrackDao;
import de.biosphere.spoticord.database.dao.UserDao;

public class MySqlDatabase implements Database {

    private final HikariDataSource dataSource;

    // DAOS
    private final AlbumDao albumDao;
    private final ArtistDao artistDao;
    private final TrackDao trackDao;
    private final UserDao userDao;

    public MySqlDatabase(final String host, final String username, final String password, final String database,
            final int port) {

        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        config.setUsername(username);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        if (Configuration.PROMETHEUS_PORT != null) {
            config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
        }

        dataSource = new HikariDataSource(config);

        initDatabase();

        // InitDAOs
        albumDao = new AlbumImplMySql(dataSource);
        artistDao = new ArtistImplMySql(dataSource);
        trackDao = new TrackImplMySql(dataSource);
        userDao = new UserImplMySql(dataSource);
    }

    private void initDatabase() {
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS `Listens` ( `Id` INT NOT NULL AUTO_INCREMENT , `Timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , `TrackId` VARCHAR(22) NOT NULL , `GuildId` VARCHAR(100) NOT NULL , `UserId` VARCHAR(100) NOT NULL , INDEX `Id` (`Id`), INDEX `listens_idx_guildid_userid` (`GuildId`, `UserId`))");
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS `Tracks` ( `Id` VARCHAR(22) NOT NULL , `Artists` VARCHAR(200) NOT NULL , `AlbumImageUrl` VARCHAR(2083) NOT NULL , `AlbumTitle` VARCHAR(200) NOT NULL , `TrackTitle` VARCHAR(200) NOT NULL , `Duration` BIGINT UNSIGNED NOT NULL , PRIMARY KEY (`Id`))");
    }

    private void executeUpdate(final String query) {
        try (final Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.executeUpdate();
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
    }

    public String getSizeOfTable(final String table) {
        try (final Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT table_name AS `Table`, round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` FROM information_schema.TABLES WHERE table_schema = \""
                            + (Configuration.DATABASE_NAME == null ? "Tracks" : Configuration.DATABASE_NAME)
                            + "\" AND table_name = \"" + table + "\";");
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("Size in MB");
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public AlbumDao getAlbumDao() {
        return albumDao;
    }

    @Override
    public ArtistDao getArtistDao() {
        return artistDao;
    }

    @Override
    public TrackDao getTrackDao() {
        return trackDao;
    }

    @Override
    public UserDao getUserDao() {
        return userDao;
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }

}