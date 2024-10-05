
  create view "warehouse"."public"."user_engagement_mart__dbt_tmp"
    
    
  as (
    WITH user_engagement AS (
    SELECT 
        fpi.user_sk,
        du.username,
        SUM(fpi.shares) AS total_shares,
        SUM(fpi.like_count) AS total_likes,
        COUNT(fpi.comment_text) AS total_comments,
        SUM(fpi.angry) AS total_angry,
        SUM(fpi.haha) AS total_haha,
        SUM(fpi.love) AS total_love,
        SUM(fpi.sad) AS total_sad,
        SUM(fpi.wow) AS total_wow,
        RANK() OVER (ORDER BY SUM(fpi.like_count) DESC) AS engagement_rank
    FROM "warehouse"."public"."fact_post_interactions" fpi
    JOIN "warehouse"."public"."dim_users" du ON fpi.user_sk = du.user_sk
    GROUP BY fpi.user_sk, du.username
)

SELECT * FROM user_engagement
  );