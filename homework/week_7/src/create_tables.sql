-- Q4: Tumbling window results
CREATE TABLE IF NOT EXISTS tumbling_window_results (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

-- Q5: Session window results
CREATE TABLE IF NOT EXISTS session_window_results (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

-- Q6: Tips window results
CREATE TABLE IF NOT EXISTS tips_window_results (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_tips DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
