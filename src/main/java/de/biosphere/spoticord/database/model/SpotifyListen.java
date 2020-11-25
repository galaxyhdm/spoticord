package de.biosphere.spoticord.database.model;

import java.sql.Date;

public class SpotifyListen {

    private final int id;
    private final Date timestamp;
    private final String guildId;
    private final String trackId;
    private final String userId;
    private long listeningTime;

    public SpotifyListen(int id, Date timestamp, String guildId, String trackId, String userId, long listeningTime) {
        this.id = id;
        this.timestamp = timestamp;
        this.guildId = guildId;
        this.trackId = trackId;
        this.userId = userId;
        this.listeningTime = listeningTime;
    }

    public int getId() {
        return id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getGuildId() {
        return guildId;
    }

    public String getTrackId() {
        return trackId;
    }

    public String getUserId() {
        return userId;
    }

    public long getListeningTime() {
        return listeningTime;
    }

    public void setListeningTime(final long listeningTime) {
        this.listeningTime = listeningTime;
    }
}