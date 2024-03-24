-- create graph nodes
DROP TABLE IF EXISTS node CASCADE;
CREATE TABLE IF NOT EXISTS node (
    id Serial PRIMARY KEY,
    geom Geometry(Point, 6668)
);

-- decompose line and remove duplicated points
INSERT INTO
    node (geom) 
SELECT 
    (ST_DumpPoints(ST_RemoveRepeatedPoints(ST_Union(geom)))).geom
FROM 
    (SELECT (ST_DumpPoints(geom)).geom FROM rdcl);

-- create spartial index for joining
DROP INDEX IF EXISTS node_idx;
CREATE INDEX IF NOT EXISTS node_idx ON node USING GIST(geom);

-- create edge
DROP TABLE IF EXISTS edge CASCADE;
CREATE TABLE IF NOT EXISTS edge (
    id Serial PRIMARY KEY,
    n1 Int4 REFERENCES node (id),
    n2 Int4 REFERENCES node (id),
    distance Float8,
    geom Geometry(LineString, 6668)
);

-- link node to node as edge
INSERT INTO
    edge (n1, n2, distance, geom)
SELECT
    t2.id,
    t3.id,
    ST_Length(t1.geom::geography),
    t1.geom
FROM 
    (SELECT (ST_DumpSegments(geom)).geom FROM rdcl) AS t1
LEFT JOIN
    node AS t2
ON
    ST_Contains(ST_StartPoint(t1.geom), t2.geom)
LEFT JOIN
    node AS t3
ON
    ST_Contains(ST_EndPoint(t1.geom), t3.geom);
