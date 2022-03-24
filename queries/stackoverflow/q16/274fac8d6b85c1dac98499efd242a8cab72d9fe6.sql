SELECT COUNT(*)
FROM site AS s,
     so_user AS u1,
     tag AS t1,
     tag_question AS tq1,
     question AS q1,
     badge AS b1,
     account AS acc
WHERE s.site_id = u1.site_id
  AND s.site_id = b1.site_id
  AND s.site_id = t1.site_id
  AND s.site_id = tq1.site_id
  AND s.site_id = q1.site_id
  AND t1.id = tq1.tag_id
  AND q1.id = tq1.question_id
  AND q1.owner_user_id = u1.id
  AND acc.id = u1.account_id
  AND b1.user_id = u1.id
  AND (q1.score >= 1)
  AND (q1.score <= 10)
  AND s.site_name = 'stackoverflow'
  AND (t1.name in ('classpath',
                   'code-behind',
                   'dbcontext',
                   'evaluation',
                   'exchange-server',
                   'html5-video',
                   'jenkins-plugins',
                   'laravel-routing',
                   'oracle12c',
                   'playframework-2.0',
                   'service'))
  AND (LOWER(acc.website_url) LIKE ('%in'))