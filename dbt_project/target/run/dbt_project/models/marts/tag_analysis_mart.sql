
  create view "warehouse"."public"."tag_analysis_mart__dbt_tmp"
    
    
  as (
    WITH tag_analysis AS (
    SELECT 
        COALESCE(dp.tags, '') AS tags,
        COALESCE(SUM(fpi.like_count), 0) AS total_likes,
        SUM(fpi.shares) AS total_shares,
        COUNT(fpi.comment_text) AS total_comments,
        SUM(fpi.angry) AS total_angry,
        SUM(fpi.haha) AS total_haha,
        SUM(fpi.love) AS total_love,
        SUM(fpi.sad) AS total_sad,
        SUM(fpi.wow) AS total_wow,
        COUNT(DISTINCT fpi.post_sk) AS total_posts_with_tag,
        AVG(fpi.like_count) AS avg_likes_per_tag
    FROM "warehouse"."public"."fact_post_interactions" fpi
    JOIN "warehouse"."public"."dim_posts" dp ON fpi.post_sk = dp.post_sk
    GROUP BY dp.tags
)

SELECT * FROM tag_analysis
  );