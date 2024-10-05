select
    dp.post_sk,
    dp.post_id,
    dp.post_text,
    dp.post_timestamp,
    dp.tags,
    dp.location,
    COALESCE(SUM(fpi.like_count), 0) AS total_likes,  -- This ensures no NULLs in total_likes
    COALESCE(SUM(fpi.shares), 0) AS total_shares,
    COALESCE(COUNT(fpi.comment_text), 0) AS total_comments,
    COALESCE(SUM(fpi.angry), 0) AS total_angry,
    COALESCE(SUM(fpi.haha), 0) AS total_haha,
    COALESCE(SUM(fpi.love), 0) AS total_love,
    COALESCE(SUM(fpi.sad), 0) AS total_sad,
    COALESCE(SUM(fpi.wow), 0) AS total_wow,
    COALESCE(COUNT(DISTINCT fpi.user_sk), 0) AS total_users_interacted
FROM {{ source('social_media_sources', 'dim_posts') }} dp
LEFT JOIN {{ source('social_media_sources', 'fact_post_interactions') }} fpi
    ON dp.post_sk = fpi.post_sk
GROUP BY dp.post_sk, dp.post_id, dp.post_text, dp.post_timestamp, dp.tags, dp.location
