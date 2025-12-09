CREATE TABLE IF NOT EXISTS youtube_trending (
    id VARCHAR(255) PRIMARY KEY,
    kind VARCHAR(255),
    etag VARCHAR(255),
    "publishedAt" TIMESTAMP,
    "channelId" VARCHAR(255),
    title TEXT,
    description TEXT,
    "channelTitle" VARCHAR(255),
    "viewCount" BIGINT,
    "likeCount" BIGINT,
    "commentCount" BIGINT
);