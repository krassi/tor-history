CREATE DATABASE tor_history;
USE tor_history;

CREATE TABLE TorQueries (
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL, 
	Version CHAR(6) NOT NULL,
	queryTime TIMESTAMP NOT NULL,
	Relays_published DATETIME NOT NULL, 
	Bridges_published DATETIME NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(Fingerprint)
);

CREATE TABLE NodeFingerprints(
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL, 
	Fingerprint CHAR(40) NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(Fingerprint)
);

CREATE TABLE Countries (
	CC CHAR(2) NOT NULL,
	CountryName CHAR(45),
	PRIMARY KEY (CC),
	UNIQUE(CountryName)
);

CREATE TABLE Regions (
	ID SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL, 
	RegionName CHAR(50),
	PRIMARY KEY (ID),
	UNIQUE(RegionName)
);

CREATE TABLE Cities (
	ID SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL, 
	CityName CHAR(40),
	PRIMARY KEY (ID),
	UNIQUE(CityName)
);

CREATE TABLE Platforms (
	ID SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL, 
	PlatformName CHAR(45) NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(PlatformName)
);

CREATE TABLE Versions (
	ID SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL, 
	VersionName CHAR(20) NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(VersionName)
);

CREATE TABLE Contacts (
	ID SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL, 
	ContactName VARCHAR(600) NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(ContactName)
);

CREATE TABLE ExitPolicies(
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL, 
	ExitPolicy TEXT NOT NULL,
	PRIMARY KEY (ID)
);

CREATE TABLE ExitPolicySummaries(
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL, 
	ExitPolicySummary TEXT NOT NULL,
	PRIMARY KEY (ID)
);


CREATE TABLE ExitPolicyV6Summaries(
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL,
	ExitPolicyV6Summary TEXT NOT NULL,
	PRIMARY KEY (ID)
);


CREATE TABLE NodeFingerprints(
	ID INT UNSIGNED AUTO_INCREMENT NOT NULL, 
	Fingerprint CHAR(40) NOT NULL,
	PRIMARY KEY (ID),
	UNIQUE(Fingerprint)
);

GRANT ALL PRIVILEGES ON tor_history.* TO 'tor-admin'@'%' IDENTIFIED BY <password> WITH GRANT OPTION;
GRANT INSERT, DELETE, SELECT ON tor_history.TorQueries TO 'tor-rw'@'%' IDENTIFIED BY <password>;
GRANT INSERT, DELETE, SELECT ON tor_history.TorQueries TO 'tor-rw'@'localhost' IDENTIFIED BY <password>;

GRANT INSERT, SELECT ON tor_history.NodeFingerprints TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.TorRelays TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Countries TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Regions TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Cities TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Platforms TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Versions TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.Contacts TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.ExitPolicies TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.ExitPolicySummaries TO 'tor-rw'@'%';
GRANT INSERT, SELECT ON tor_history.ExitPolicyV6Summaries TO 'tor-rw'@'%';
