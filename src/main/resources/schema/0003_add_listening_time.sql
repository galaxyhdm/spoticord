-- liquibase formatted sql

-- changeset markusk:1606070067815-1
alter table Listens add ListeningTime bigint default 0 not null;
UPDATE Listens SET ListeningTime = (SELECT Tracks.Duration FROM Tracks WHERE Tracks.Id = Listens.TrackId);
-- rollback alter table Listens drop column `ListeningTime`;