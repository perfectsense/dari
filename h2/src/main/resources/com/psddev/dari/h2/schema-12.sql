CREATE TABLE IF NOT EXISTS "Record" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "data" LONGVARBINARY NOT NULL,
    PRIMARY KEY ("typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_Record_id" ON "Record" ("id");

CREATE TRIGGER IF NOT EXISTS "t_Record_searchUpdate" BEFORE INSERT, UPDATE, DELETE ON "Record" FOR EACH ROW CALL "com.psddev.dari.h2.SearchUpdateTrigger";

CREATE TABLE IF NOT EXISTS "RecordSearch" (
    "id" UUID NOT NULL,
    "fieldName" VARCHAR(100) NOT NULL,
    "value" LONGVARCHAR NOT NULL,
    PRIMARY KEY ("id", "fieldName")
);

CREATE ALIAS IF NOT EXISTS FT_INIT FOR "org.h2.fulltext.FullText.init";

CALL FT_INIT();

CALL FT_CREATE_INDEX('PUBLIC', 'RecordSearch', 'value');

CREATE TABLE IF NOT EXISTS "RecordLocation3" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" GEOMETRY NOT NULL,
    PRIMARY KEY ("symbolId", "value", "typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_RecordLocation3_id" ON "RecordLocation3" ("id");

CREATE SPATIAL INDEX IF NOT EXISTS "k_RecordLocation3_value" ON "RecordLocation3" ("value");

CREATE TABLE IF NOT EXISTS "RecordNumber3" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" DOUBLE NOT NULL,
    PRIMARY KEY ("symbolId", "value", "typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_RecordNumber3_id" ON "RecordNumber3" ("id");

CREATE TABLE IF NOT EXISTS "RecordRegion2" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" GEOMETRY NOT NULL,
    PRIMARY KEY ("symbolId", "value", "typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_RecordRegion2_id" ON "RecordRegion2" ("id");

CREATE SPATIAL INDEX IF NOT EXISTS "k_RecordRegion2_value" ON "RecordRegion2" ("value");

CREATE TABLE IF NOT EXISTS "RecordString4" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" VARCHAR(500) NOT NULL,
    PRIMARY KEY ("symbolId", "value", "typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_RecordString4_id" ON "RecordString4" ("id");

CREATE TABLE IF NOT EXISTS "RecordUpdate" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "updateDate" DOUBLE NOT NULL,
    PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "k_RecordUpdate_typeId_updateDate" ON "RecordUpdate" ("typeId", "updateDate");

CREATE INDEX IF NOT EXISTS "k_RecordUpdate_updateDate" ON "RecordUpdate" ("updateDate");

CREATE TABLE IF NOT EXISTS "RecordUuid3" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" UUID NOT NULL,
    PRIMARY KEY ("symbolId", "value", "typeId", "id")
);

CREATE INDEX IF NOT EXISTS "k_RecordUuid3_id" ON "RecordUuid3" ("id");

CREATE TABLE IF NOT EXISTS "Symbol" (
    "symbolId" INT NOT NULL AUTO_INCREMENT,
    "value" VARCHAR(500) NOT NULL,
    PRIMARY KEY ("symbolId")
);

CREATE UNIQUE INDEX IF NOT EXISTS "k_Symbol_value" ON "Symbol" ("value");
