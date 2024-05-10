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
