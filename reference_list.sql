CREATE TABLE "business" (
	"id" INTEGER NOT NULL UNIQUE,
	"name" varchar(100) UNIQUE check(length(name) < 100),
	"customers" INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT)
);

INSERT INTO business (name, customers)
VALUES ("Google", 15000);

INSERT INTO business (name, customers)
VALUES
	("Microsoft", 16900), 
	("X", 1000), 
	("Instagram", 1345),
	("Pinterest", 12045),
	("TikTok", 56790),
	("Facebook", 3456);

INSERT INTO business (name, customers)
VALUES ("is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum", 45);


SELECT *, CASE name WHEN "Facebook" THEN "Face"  ELSE "Tok" END othername
FROM business
ORDER BY name;

SELECT *, count(name) as name_count, length(name) as name_length 
FROM business
GROUP BY name
ORDER BY name ASC
