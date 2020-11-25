package de.biosphere.spoticord.database.impl.mysql;

import com.zaxxer.hikari.HikariDataSource;
import de.biosphere.spoticord.database.dao.TrackDao;
import de.biosphere.spoticord.database.model.SpotifyListen;
import de.biosphere.spoticord.database.model.SpotifyTrack;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TrackImplMySql implements TrackDao {

    private final HikariDataSource hikariDataSource;

    public TrackImplMySql(HikariDataSource hikariDataSource) {
        this.hikariDataSource = hikariDataSource;
    }

    @Override
    public Integer getTrackAmount() {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT COUNT(*) AS Count FROM Tracks")) {
                final ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return resultSet.getInt("Count");
                }
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return 0;
    }

    @Override
    public Integer getListensAmount(String guildId) {
        return getListensAmount(guildId, null);
    }

    @Override
    public Integer getListensAmount() {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT COUNT(*) AS Count FROM `Listens`")) {
                final ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return resultSet.getInt("Count");
                }
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return 0;
    }

    @Override
    public Integer getListensAmount(String guildId, String userId) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection
                    .prepareStatement(userId == null ? "SELECT COUNT(*) AS Count FROM `Listens` WHERE GuildId=?"
                            : "SELECT COUNT(*) AS Count FROM `Listens` WHERE GuildId=? AND UserId=?")) {
                preparedStatement.setString(1, guildId);
                if (userId != null) {
                    preparedStatement.setString(2, userId);
                }
                final ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return resultSet.getInt("Count");
                }
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return 0;
    }

    @Override
    public void insertTrack(SpotifyTrack spotifyTrack, String userId, String guildId) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT IGNORE INTO `Tracks` (`Id`, `Artists`, `AlbumImageUrl`, `AlbumTitle`, `TrackTitle`, `Duration`) VALUES (?, ?, ?, ?, ?, ?)")) {
                preparedStatement.setString(1, spotifyTrack.id());
                preparedStatement.setString(2, spotifyTrack.artists());
                preparedStatement.setString(3, spotifyTrack.albumImageUrl());
                preparedStatement.setString(4, spotifyTrack.albumTitle());
                preparedStatement.setString(5, spotifyTrack.trackTitle());
                preparedStatement.setLong(6, spotifyTrack.duration());
                preparedStatement.execute();
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public int insertListening(final SpotifyTrack spotifyTrack, final String userId, final String guildId) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO `Listens` (`Id`, `Timestamp`, `TrackId`, `GuildId`, `UserId`, `ListeningTime`) VALUES (NULL, CURRENT_TIMESTAMP, ?, ?, ?, ?);")) {
                preparedStatement.setString(1, spotifyTrack.id());
                preparedStatement.setString(2, guildId);
                preparedStatement.setString(3, userId);
                preparedStatement.setLong(4, spotifyTrack.duration());
                preparedStatement.execute();
            }
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT LAST_INSERT_ID() AS last_id")) {
                final ResultSet resultSet = preparedStatement.executeQuery();
                if(resultSet.next()) return resultSet.getInt("last_id");
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return -1;
    }

    @Override
    public Map<SpotifyTrack, Integer> getTopTracks(String guildId, String userId, Integer count, Integer lastDays) {
        final Map<SpotifyTrack, Integer> topMap = new LinkedHashMap<>();
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(userId == null
                    ? "SELECT Tracks.*, COUNT(*) AS Listener FROM `Listens` INNER JOIN Tracks ON Listens.TrackId=Tracks.Id WHERE Listens.GuildId=? "
                            + MySqlDatabase.getTimestampQuery(lastDays)
                            + "GROUP BY Listens.`TrackId` ORDER BY COUNT(*) DESC LIMIT ?"
                    : "SELECT Tracks.*, COUNT(*) AS Listener FROM `Listens` INNER JOIN Tracks ON Listens.TrackId=Tracks.Id WHERE Listens.GuildId=? AND Listens.UserId=? "
                            + MySqlDatabase.getTimestampQuery(lastDays)
                            + "GROUP BY Listens.`TrackId` ORDER BY COUNT(*) DESC LIMIT ?")) {
                preparedStatement.setString(1, guildId);
                if (userId != null) {
                    preparedStatement.setString(2, userId);
                    preparedStatement.setInt(3, count);
                } else {
                    preparedStatement.setInt(2, count);
                }

                final ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    final SpotifyTrack spotifyTrack = getTrackFromResultSet(resultSet);
                    final Integer listener = resultSet.getInt("Listener");
                    topMap.put(spotifyTrack, listener);
                }
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        return topMap;
    }

    private SpotifyTrack getTrackFromResultSet(final ResultSet resultSet) throws SQLException {
        return new SpotifyTrack(resultSet.getString("Id"), resultSet.getString("Artists"),
                resultSet.getString("AlbumTitle"), resultSet.getString("TrackTitle"),
                resultSet.getString("AlbumImageUrl"), resultSet.getLong("Duration"));
    }

    @Override
    public List<SpotifyTrack> getLastTracks(String guildId) {
        return getLastTracks(guildId, null);
    }

    @Override
    public List<SpotifyTrack> getLastTracks(String guildId, String userId) {
        final List<SpotifyTrack> tracks = new LinkedList<>();
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(userId == null
                    ? "SELECT Tracks.* FROM `Listens` INNER JOIN Tracks ON Listens.TrackId=Tracks.Id WHERE GuildId=? ORDER BY `Listens`.`Timestamp`  DESC LIMIT 10"
                    : "SELECT Tracks.* FROM `Listens` INNER JOIN Tracks ON Listens.TrackId=Tracks.Id WHERE GuildId=? AND UserId=? ORDER BY `Listens`.`Timestamp`  DESC LIMIT 10")) {
                preparedStatement.setString(1, guildId);
                if (userId != null) {
                    preparedStatement.setString(2, userId);
                }
                final ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    final SpotifyTrack spotifyTrack = getTrackFromResultSet(resultSet);
                    tracks.add(spotifyTrack);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return tracks;
    }

    @Override
    public SpotifyListen getSpotifyListen(final int listenId) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM Listens WHERE Id=?;")) {
               preparedStatement.setInt(1, listenId);
                final ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next())
                    return getListenFromResultSet(resultSet);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public SpotifyListen getLastListen(final String guildId, final String userId) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT * FROM Listens WHERE GuildId=? AND UserId=? ORDER BY Timestamp DESC LIMIT 1;")) {
                preparedStatement.setString(1, guildId);
                preparedStatement.setString(2, userId);
                final ResultSet resultSet = preparedStatement.executeQuery();
                if(resultSet.next())
                    return getListenFromResultSet(resultSet);
            }
        }catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public void updateListeningTime(final int listenId, final long listeningTime) {
        try (final Connection connection = hikariDataSource.getConnection()) {
            try (final PreparedStatement preparedStatement = connection
                    .prepareStatement("UPDATE Listens SET ListeningTime=? WHERE Id=?;")) {
                preparedStatement.setLong(1, listeningTime);
                preparedStatement.setInt(2, listenId);
                preparedStatement.execute();
            }
        }catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    private SpotifyListen getListenFromResultSet(final ResultSet resultSet) throws SQLException {
        return new SpotifyListen(resultSet.getInt("Id"), resultSet.getDate("Timestamp"), resultSet.getString("GuildId"),
                resultSet.getString("TrackId"), resultSet.getString("UserId"), resultSet.getLong("ListeningTime"));
    }

}