CREATE TABLE IF NOT EXISTS business (
	"id" INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
	"name" varchar(100) UNIQUE check(length(name) < 100),
	"customers" INTEGER DEFAULT 0 check(customers>=0),
	"modified_on" TEXT NULL,
	"created_on" TEXT NULL
)

DROP TABLE IF EXISTS business

INSERT INTO business (name, customers)
VALUES ("Google", 15000);

INSERT INTO business (name, customers)
VALUES
	("Microsoft", 16900), 
	("X", 1000), 
	("Instagram", 1345),
	("Pinterest", 12045),
	("TikTok", 56790),
	("Facebook", 3456),
	("Youtube", 223243),
	("Douyin", 2323),
	("Snapchat", 23235),
	("Threads", 45232)


SELECT *, CASE name WHEN "Facebook" THEN "Face"  ELSE "Tok" END othername
FROM business
ORDER BY name;

SELECT *, count(name) as name_count, length(name) as name_length 
FROM business
GROUP BY name
ORDER BY name ASC


SELECT DISTINCT name
FROM business


INSERT INTO business (name, customers)
VALUES(8, "Google", 34000)
ON CONFLICT(name)
DO UPDATE SET customers=34000


UPDATE business
SET created_on=datetime('now'), modified_on=datetime('now')
WHERE id=1


CREATE INDEX idx_name 
ON business (name)

PRAGMA foreign_key_list(polls_answer)


CREATE TABLE celebrities(
	id integer NOT NULL PRIMARY KEY AUTOINCREMENT, 
	firstname varchar(200) NULL,
	lastname varchar(200) NULL,
	created_on date NULL
)

CREATE TABLE socials(
	id integer NOT NULL PRIMARY KEY AUTOINCREMENT, 
	name varchar(500) NULL,
	followers INTEGER NULL DEFAULT 0 check(followers>0),
	celebrity_id NOT NULL,
	FOREIGN KEY (celebrity_id) REFERENCES athlete(id) DEFERRABLE INITIALLY DEFERRED
)



CREATE TABLE "accounts_myuser" (
	"id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, 
	"password" varchar(128) NOT NULL, 
	"last_login" datetime NULL, 
	"is_superuser" bool NOT NULL, 
	"username" varchar(150) NOT NULL UNIQUE, 
	"first_name" varchar(150) NOT NULL, 
	"last_name" varchar(150) NOT NULL, 
	"email" varchar(254) NOT NULL, 
	"is_staff" bool NOT NULL, 
	"is_active" bool NOT NULL, 
	"date_joined" datetime NOT NULL, 
	"is_moderator" bool NOT NULL
)

CREATE TABLE "accounts_myuser_groups" (
	"id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, 
	"myuser_id" bigint NOT NULL REFERENCES 
	"accounts_myuser" ("id") DEFERRABLE INITIALLY DEFERRED, 
	"group_id" integer NOT NULL REFERENCES "auth_group" ("id") DEFERRABLE INITIALLY DEFERRED
)
