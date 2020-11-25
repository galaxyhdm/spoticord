package de.biosphere.spoticord.handler;

import de.biosphere.spoticord.Spoticord;
import de.biosphere.spoticord.database.model.SpotifyListen;
import de.biosphere.spoticord.database.model.SpotifyTrack;
import de.biosphere.spoticord.utils.BiKey;
import de.biosphere.spoticord.utils.Metrics;
import net.dv8tion.jda.api.entities.Activity;
import net.dv8tion.jda.api.entities.RichPresence;
import net.dv8tion.jda.api.events.user.UserActivityEndEvent;
import net.dv8tion.jda.api.events.user.UserActivityStartEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DiscordUserUpdateGameListener extends ListenerAdapter {

    private final Spoticord bot;
    private final Map<String, String> lastActivitiesMap;
    private final Map<BiKey<String, String>, SpotifyListen> lastListenCache;

    public DiscordUserUpdateGameListener(final Spoticord instance) {
        this.bot = instance;
        this.lastActivitiesMap = ExpiringMap.builder().expiration(7, TimeUnit.MINUTES)
                .expirationPolicy(ExpirationPolicy.ACCESSED).build();
        this.lastListenCache = ExpiringMap.builder().expiration(10, TimeUnit.MINUTES)
                .expirationPolicy(ExpirationPolicy.ACCESSED).build();
    }

    @Override
    public void onUserActivityStart(@Nonnull final UserActivityStartEvent event) {
        if (checkActivity(event.getNewActivity())) {
            return;
        }
        final RichPresence richPresence = event.getNewActivity().asRichPresence();
        if (checkRichPresence(richPresence)) {
            return;
        }
        System.out.println("------ Start: " + richPresence.getSyncId());
        System.out.println(richPresence.getTimestamps().getElapsedTime(ChronoUnit.SECONDS));
        System.out.println(
                "Duration: " + (richPresence.getTimestamps().getEnd() - richPresence.getTimestamps().getStart()));
        System.out.println("------");
        if (checkCache(event.getMember().getId(), richPresence.getSyncId())) {
            return;
        }
        lastActivitiesMap.put(event.getMember().getId(), richPresence.getSyncId());
        Metrics.TRACKS_PER_MINUTE.labels(event.getGuild().getId()).inc();

        final long duration = (richPresence.getTimestamps().getEnd() - richPresence.getTimestamps().getStart());
        final long elapsedTime = Objects.requireNonNull(richPresence.getTimestamps()).getElapsedTime(ChronoUnit.MILLIS);

        final SpotifyTrack spotifyTrack = new SpotifyTrack(richPresence.getSyncId(), richPresence.getState(),
                richPresence.getLargeImage().getText(), richPresence.getDetails(),
                richPresence.getLargeImage().getUrl(), duration);
        bot.getDatabase().getTrackDao().insertTrack(spotifyTrack, event.getMember().getId(), event.getGuild().getId());

        //Create BiKey
        final BiKey<String, String> key = BiKey.of(event.getGuild().getId(), event.getMember().getId());
        // Get last listen from database
        final SpotifyListen lastListen = this.getLastListen(key, false);

        // Check if lastListen = null or both trackId are not the same then insert to db / cache
        if(lastListen == null || !lastListen.getTrackId().equals(richPresence.getSyncId())){
            final int id = bot.getDatabase().getTrackDao()
                    .insertListening(spotifyTrack, event.getMember().getId(), event.getGuild().getId());
            this.lastListenCache.put(key, this.bot.getDatabase().getTrackDao().getSpotifyListen(id));
            System.out.println("1. new id = " + id);
            return;
        }

        final long listeningTime = lastListen.getListeningTime();
        final double offset = percentValue(duration, 0.02);
        System.out.println("listeningTime = " + listeningTime);
        System.out.println("offset = " + offset);

        // Check elapsedTime ± offset if not create new instance and insert to db / cache
        if (!(elapsedTime >= (listeningTime - offset) && elapsedTime <= (listeningTime + offset))) {
            final int id = bot.getDatabase().getTrackDao()
                    .insertListening(spotifyTrack, event.getMember().getId(), event.getGuild().getId());
            this.lastListenCache.put(key, this.bot.getDatabase().getTrackDao().getSpotifyListen(id));
            System.out.println("2. new id = " + id);
        }
    }

    @Override
    public void onUserActivityEnd(@NotNull final UserActivityEndEvent event) {
        if (checkActivity(event.getOldActivity())) return;
        final RichPresence richPresence = event.getOldActivity().asRichPresence();
        if (checkRichPresence(richPresence)) return;
        final long elapsedTime = Objects.requireNonNull(richPresence.getTimestamps()).getElapsedTime(ChronoUnit.MILLIS);

        System.out.println("------ End: " + richPresence.getSyncId());
        System.out.println("End: " + richPresence.getTimestamps().getElapsedTime(ChronoUnit.SECONDS));

        final SpotifyListen lastListen =
                this.getLastListen(BiKey.of(event.getGuild().getId(), event.getMember().getId()), false);
        if (lastListen == null) return;
        System.out.println("Id: " + lastListen.getId());
        if (!Objects.equals(richPresence.getSyncId(), lastListen.getTrackId())) return;
        System.out.println("Found same entry!");
        lastListen.setListeningTime(elapsedTime);
        this.bot.getDatabase().getTrackDao().updateListeningTime(lastListen.getId(), elapsedTime);

        System.out.println("------");
    }

    private boolean checkActivity(final Activity activity) {
        return activity == null || !activity.isRich() || activity.getType() != Activity.ActivityType.LISTENING;
    }

    private boolean checkRichPresence(final RichPresence richPresence) {
        return richPresence == null || richPresence.getDetails() == null || richPresence.getSyncId() == null;
    }

    private boolean checkCache(final String memberId, final String spotifyId) {
        return lastActivitiesMap.containsKey(memberId) && lastActivitiesMap.get(memberId).equalsIgnoreCase(spotifyId);
    }

    private SpotifyListen getLastListen(final BiKey<String, String> key, final boolean force) {
        if (!this.lastListenCache.containsKey(key) || force) {
            this.lastListenCache.put(key,
                    this.bot.getDatabase().getTrackDao().getLastListen(key.getFirst(), key.getSecond()));
        }
        return this.lastListenCache.get(key);
    }

    private double percentValue(final long elapsedTime, final double percent){
        return elapsedTime * percent;
    }

}
