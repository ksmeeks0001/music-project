create database airflow;
drop database if exists music;
CREATE DATABASE music; 
use music;

CREATE TABLE `BillboardChart` (
  `Id` int PRIMARY KEY NOT NULL auto_increment,
  `ChartType` varchar(32),
  `ChartDate` date,
  `CreatedAt` datetime not null default current_timestamp
);

CREATE TABLE `Artist` (
  `Id` int PRIMARY KEY NOT NULL auto_increment,
  `Name` varchar(64) UNIQUE NOT NULL,
  `SpotifyId` varchar(32) UNIQUE,
  `SpotifyFollowers` int unsigned,
  `SpotifyPopularity` int unsigned,
  `SpotifyUpdateTime` datetime,
  `ImageURL` varchar(255),
  `CreatedAt` datetime NOT NULL default current_timestamp
);

CREATE TABLE `Genre` (
  `Id` int PRIMARY KEY NOT NULL auto_increment,
  `Name` varchar(64) UNIQUE NOT NULL,
  `CreatedAt` datetime NOT NULL default current_timestamp
);

CREATE TABLE `ArtistGenre` (
  `ArtistId` int NOT NULL,
  `GenreId` int NOT NULL,
  PRIMARY KEY (`ArtistId`, `GenreId`)
);

CREATE TABLE `ArtistLog` (
  `Id` int PRIMARY KEY NOT NULL auto_increment,
  `ArtistId` int NOT NULL,
  `SpotifyFollowers` int unsigned,
  `SpotifyPopularity` int unsigned,
  `UpdateTime` datetime NOT NULL
);

CREATE TABLE `ChartArtist` (
  `ChartId` int NOT NULL,
  `ArtistId` int NOT NULL,
  `Position` int NOT NULL,
  `Weeks` int NOT NULL DEFAULT 0,
  PRIMARY KEY (`ChartId`, `ArtistId`)
);

CREATE UNIQUE INDEX `ArtistLog_index_0` ON `ArtistLog` (`ArtistId`, `UpdateTime`);
ALTER TABLE `ChartArtist` ADD CONSTRAINT `fk_chart` FOREIGN KEY (`ChartId`) REFERENCES `BillboardChart` (`Id`);
ALTER TABLE `ChartArtist` ADD CONSTRAINT `fk_artist` FOREIGN KEY (`ArtistId`) REFERENCES `Artist` (`Id`);
ALTER TABLE `ArtistLog` ADD CONSTRAINT `fk_artist2` FOREIGN KEY (`ArtistId`) REFERENCES `Artist` (`Id`);
ALTER TABLE `ArtistGenre` ADD CONSTRAINT `fk_artist3` FOREIGN KEY (`ArtistId`) REFERENCES `Artist` (`Id`);
ALTER TABLE `ArtistGenre` ADD CONSTRAINT `fk_genre` FOREIGN KEY (`GenreId`) REFERENCES `Genre` (`Id`);

-- when spotify data changes log old values
DELIMITER $$
USE `music`$$
CREATE DEFINER=`admin`@`%` TRIGGER `Artist_AFTER_UPDATE` AFTER UPDATE ON `Artist` FOR EACH ROW BEGIN
	if NEW.SpotifyUpdateTime is not null then
		insert into ArtistLog
		(ArtistId, SpotifyFollowers, SpotifyPopularity, UpdateTime)
		values
		(OLD.Id, NEW.SpotifyFollowers, NEW.SpotifyPopularity, NEW.SpotifyUpdateTime)
		;
	end if;
END$$
DELIMITER ;


-- stored proc
DROP procedure IF EXISTS `add_chart`;

DELIMITER $$
USE `music`$$
CREATE DEFINER=`admin`@`%` PROCEDURE `add_chart`(pChartType varchar(32), pChartDate date)
BEGIN

	insert into music.BillboardChart
    (ChartType, ChartDate)
    values
    (pChartType, pChartDate)
    ;
    
    select last_insert_id() chart_id; 

END$$

DELIMITER ;


DROP procedure IF EXISTS `add_chart_artist`;

DELIMITER $$
USE `music`$$
CREATE DEFINER=`admin`@`%` PROCEDURE `add_chart_artist`(pChartId int, pArtistName varchar(64), pPosition int, pWeeks int)
BEGIN
	declare artist_id int;
	select Id into artist_id from music.Artist where `Name` = pArtistName;
    
    if artist_id is null then
		insert into music.Artist
        (`Name`)
        values
        (pArtistName);
        
		set artist_id = last_insert_id();
    end if;
    
    insert into music.ChartArtist
    (ChartId, ArtistId, Position, Weeks)
    values
    (pChartId, artist_id, pPosition, pWeeks)
    ; 
END$$

DELIMITER ;

USE `music`;
DROP procedure IF EXISTS `update_genre`;

DELIMITER $$
USE `music`$$
CREATE DEFINER=`admin`@`%` PROCEDURE `update_genre`(pArtistId int, pGenre varchar(64))
BEGIN
	declare pGenreId int;
    
    select Id into pGenreId from music.Genre where Name = pGenre;
    
    if pGenreId is null then 
		insert into music.Genre 
        (Name) values (pGenre);
        
        set pGenreId = last_insert_id();
    end if;
    
    insert into ArtistGenre
    (ArtistId, GenreId)
    values
    (pArtistId, pGenreId)
    ; 
END$$

DELIMITER ;

