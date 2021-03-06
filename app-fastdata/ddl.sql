-- replicated table for cluster centers, will be populated periodically by OLAP
-- systems
CREATE TABLE clusters
(
  id       integer        NOT NULL,
  src      integer        NOT NULL,
  dest     integer        NOT NULL,
  referral integer        NOT NULL,
  agent    integer        NOT NULL,
  CONSTRAINT clusters_pkey PRIMARY KEY (id)
);

-- replicated table for destinations
CREATE TABLE dests
(
  id       integer      NOT NULL,
  url      varchar(127) UNIQUE, -- referral can be null
  CONSTRAINT dests_pkey PRIMARY KEY (id)
);

-- replicated table for user agents
CREATE TABLE agents
(
  id       integer             NOT NULL,
  name     varchar(142) UNIQUE NOT NULL,
  CONSTRAINT agents_pkey PRIMARY KEY (id)
);

-- events
CREATE TABLE events
(
  src      integer        NOT NULL,
  dest     integer        NOT NULL,
  method   varchar(3)     NOT NULL,
  ts       timestamp      NOT NULL,
  key      bigint         NOT NULL,
  size     bigint         NOT NULL,
  referral integer        NOT NULL,
  agent    integer        NOT NULL,
  cluster  integer,
  PRIMARY KEY (ts, key, src),
  LIMIT PARTITION ROWS 10000
	EXECUTE (
  	    DELETE FROM events
  	    ORDER BY ts, key, src
  	    LIMIT 1
    )
);
PARTITION TABLE events ON COLUMN src;

CREATE INDEX event_src_dest_index ON events (src, dest);

-- Agg views
CREATE VIEW events_sessions
(
  src,
  dest,
  counts,
  last_ts
)
AS SELECT src, dest, COUNT(*), MAX(ts)
   FROM events
   GROUP BY src, dest;

CREATE VIEW events_by_second
(
  second_ts,
  src,
  count_values
)
AS SELECT TRUNCATE(SECOND, ts), src, COUNT(*)
   FROM events
   GROUP BY TRUNCATE(SECOND, ts), src;

CREATE VIEW dests_by_second
(
  second_ts,
  dest,
  count_values
)
AS SELECT TRUNCATE(SECOND, ts), dest, COUNT(*)
   FROM events
   GROUP BY TRUNCATE(SECOND, ts), dest;

CREATE VIEW events_by_cluster
(
  second_ts,
  cluster,
  count_values
)
AS SELECT TRUNCATE(SECOND, ts), cluster, COUNT(*)
   FROM events
   WHERE cluster IS NOT NULL
   GROUP BY TRUNCATE(SECOND, ts), cluster;

-- stored procedures
LOAD CLASSES events-procs.jar;

CREATE PROCEDURE FROM CLASS events.NewEvent;
PARTITION PROCEDURE NewEvent ON TABLE events COLUMN src;

CREATE PROCEDURE FROM CLASS events.GetTopUsers;

CREATE PROCEDURE FROM CLASS events.LoadNewClusters;

CREATE PROCEDURE GetTopDests AS
SELECT dests.url AS url, SUM(count_values) AS counts
FROM dests_by_second, dests
WHERE TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?) <= second_ts AND dest = dests.id
GROUP BY url
ORDER BY counts DESC, url LIMIT ?;

CREATE PROCEDURE GetEventsByCluster AS
SELECT cluster, SUM(count_values) AS counts
FROM events_by_cluster
WHERE TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?) <= second_ts
GROUP BY cluster
ORDER BY cluster;
