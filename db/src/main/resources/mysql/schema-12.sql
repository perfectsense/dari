CREATE TABLE IF NOT EXISTS Record (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    data LONGBLOB NOT NULL,
    PRIMARY KEY (typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW Record_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, DATA FROM Record;

CREATE TABLE IF NOT EXISTS RecordLocation3 (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE= MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW RecordLocation3_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, symbolId, VALUE FROM RecordLocation3;

CREATE TABLE IF NOT EXISTS RecordNumber3 (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW RecordNumber3_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, symbolId, VALUE FROM RecordNumber3;

CREATE TABLE IF NOT EXISTS RecordRegion2 (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value POLYGON NOT NULL,
    PRIMARY KEY (symbolId, value(25), typeId, id),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW RecordRegion2_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, symbolId, VALUE FROM RecordRegion2;

CREATE TABLE IF NOT EXISTS RecordString4 (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW RecordString4_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, symbolId, VALUE FROM RecordString4;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW RecordUpdate_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, updateDate FROM RecordUpdate;

CREATE TABLE IF NOT EXISTS RecordUuid3 (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW RecordUuid3_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, symbolId, hex(VALUE) AS VALUE FROM RecordUuid3;

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL AUTO_INCREMENT,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId),
    UNIQUE KEY k_value (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE TABLE IF NOT EXISTS Metric (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    dimensionId BINARY(16) NOT NULL,
    data BINARY(20) NOT NULL,
    PRIMARY KEY (symbolId, id, dimensionId, data(4)),
    KEY k_metricAllDims (symbolId, typeId, id, data, dimensionId),
    KEY k_metricData (symbolId, typeId, id, dimensionId, data)
) ENGINE=InnoDB DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED;

CREATE TABLE IF NOT EXISTS MetricDimension (
    dimensionId BINARY(16) NOT NULL PRIMARY KEY,
    value VARBINARY(500) NOT NULL,
    UNIQUE KEY k_metricDimensionValue (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=COMPRESSED;

CREATE OR REPLACE VIEW Metric_n AS
SELECT c.id
, c.typeId
, c.symbolId
, c.dimensionId
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, -10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, -10) / 1000000, 6) cumulativeAmount
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventDate
, data AS data
FROM Metric c;

CREATE OR REPLACE VIEW Metric_d AS
SELECT hex(c.id) AS id
, hex(c.typeId) as typeId
, c.symbolId
, hex(c.dimensionId) as dimensionId
, d.value AS dimension
, ls.value as symbol
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, -10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, -10) / 1000000, 6) cumulativeAmount
, FROM_UNIXTIME(CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60) eventDate
, HEX(data) AS data
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventTimestamp
FROM Metric c
JOIN Symbol ls ON (c.symbolId = ls.symbolId)
LEFT JOIN MetricDimension d ON (c.dimensionId = d.dimensionId);
