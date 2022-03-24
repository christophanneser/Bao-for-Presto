SELECT b1.name,
       count(*)
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
  AND (q1.view_count >= 100)
  AND (q1.view_count <= 100000)
  AND (s.site_name in ('stackoverflow',
                       'superuser'))
  AND (t1.name in ('biopython',
                   'dashboard',
                   'ejb-3.1',
                   'impersonation',
                   'select',
                   'size',
                   'svm',
                   'swagger-2.0',
                   'tampermonkey',
                   'wcf-client'))
  AND (LOWER(acc.website_url) LIKE ('%code%'))
GROUP BY b1.name
ORDER BY COUNT(*) DESC
LIMIT 100