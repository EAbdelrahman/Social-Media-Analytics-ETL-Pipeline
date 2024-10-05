WITH location_performance AS (
    SELECT 
        COALESCE(dp.location, 'Unknown Location') AS location,  -- Use location from dim_posts
        SUM(fpi.like_count) AS total_likes,
        SUM(fpi.shares) AS total_shares,
        COUNT(fpi.comment_text) AS total_comments,
        SUM(fpi.angry) AS total_angry,
        SUM(fpi.haha) AS total_haha,
        SUM(fpi.love) AS total_love,
        SUM(fpi.sad) AS total_sad,
        SUM(fpi.wow) AS total_wow,
        COUNT(DISTINCT fpi.user_sk) AS total_users_in_location
    FROM {{ source('social_media_sources', 'fact_post_interactions') }} fpi
    JOIN {{ source('social_media_sources', 'dim_posts') }} dp ON fpi.post_sk = dp.post_sk  -- Join with dim_posts
    GROUP BY dp.location  -- Group by location from dim_posts
)

SELECT * FROM location_performance
