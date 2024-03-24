-- subdivide into line segments and compute radian
DROP TABLE IF EXISTS edge_seg;
CREATE TEMPORARY TABLE IF NOT EXISTS edge_seg AS (
    SELECT
        id,
        ST_Centroid(seg_geom) AS rayorigin_geom,
        azimuth
    FROM (
        SELECT
            id,
            (ST_DumpSegments(ST_Segmentize(geom::geography, 4.0)::geometry)).geom AS seg_geom,
            ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom)) AS azimuth
        FROM
            edge
    )
);

DROP TABLE IF EXISTS normal;
CREATE TEMPORARY TABLE IF NOT EXISTS normal (
    id Int4,
    rayorigin_geom Geometry(Point, 6668),
    normal_geom Geometry(LineString, 6668),
    face Bool
);

-- line normal (front face)
INSERT INTO
    normal
    (id, rayorigin_geom, normal_geom, face)
SELECT
    id,
    rayorigin_geom,
    ST_MakeLine(
        rayorigin_geom,
        ST_Project(
            rayorigin_geom::geography,
            15.0,
            azimuth + 0.5 * pi()
        )::geometry
    ),
    True
FROM
    edge_seg;

-- line normal (back face)
INSERT INTO
    normal
    (id, rayorigin_geom, normal_geom, face)
SELECT
    id,
    rayorigin_geom,
    ST_MakeLine(
        rayorigin_geom,
        ST_Project(
            rayorigin_geom::geography,
            15.0,
            azimuth - 0.5 * pi()
        )::geometry
    ),
    False
FROM 
    edge_seg;

-- collision fgd
DROP TABLE IF EXISTS col_fgd;
CREATE TEMPORARY TABLE IF NOT EXISTS col_fgd AS (
    SELECT
        geom
    FROM
        fgd
    WHERE
        type = '真幅道路' OR type = '庭園路等' OR type = '徒歩道'
);

DROP INDEX idx_col_fgd;
CREATE INDEX idx_col_fgd ON col_fgd USING GIST(geom);

DROP INDEX idx_normal;
CREATE INDEX idx_normal ON normal USING GIST(normal_geom);

-- segmented one side width set
DROP TABLE IF EXISTS width__seg_one_set;
CREATE TEMPORARY TABLE IF NOT EXISTS width__seg_one_set AS (
    SELECT
        t1.id,
        t1.rayorigin_geom,
        ST_Distance(
            t1.rayorigin_geom::geography,
            ST_Intersection(t1.normal_geom, t2.geom)::geography
        ) AS width,
        t1.face
    FROM
        normal AS t1
    LEFT JOIN
        col_fgd AS t2
    ON
        ST_Intersects(t1.normal_geom, t2.geom)
);

-- segmented one side width
DROP TABLE IF EXISTS width__seg_one;
CREATE TEMPORARY TABLE IF NOT EXISTS width__seg_one AS (
    SELECT
        id,
        rayorigin_geom,
        face,
        min(width) AS width
    FROM 
        width__seg_one_set
    GROUP BY
       id,
       rayorigin_geom,
       face
);

-- segmented width
DROP TABLE IF EXISTS width__seg;
CREATE TEMPORARY TABLE IF NOT EXISTS width__seg AS (
    SELECT
        id,
        rayorigin_geom,
        sum(width) AS width
    FROM
        width__seg_one
    GROUP BY
        id,
        rayorigin_geom
);


-- create width table
DROP TABLE IF EXISTS width;
CREATE TABLE IF NOT EXISTS width (
    id Int4 REFERENCES edge (id),
    width Float8
);

-- insert width records
INSERT INTO
    width
    (id, width)
SELECT
    id,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY width) AS width
FROM
    width__seg
GROUP BY
    id;
