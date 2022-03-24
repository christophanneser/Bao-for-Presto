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
  AND (q1.score >= 0)
  AND (q1.score <= 1000)
  AND s.site_name = 'stackoverflow'
  AND (t1.name in ('dup2',
                   'google-query-language',
                   'hubot',
                   'magnific-popup',
                   'model-view',
                   'objective-c-category',
                   'oh-my-zsh',
                   'pocketsphinx',
                   'provider',
                   'special-folders',
                   'ubuntu-10.04',
                   'unsigned-char',
                   'xml-twig'))
  AND (LOWER(acc.website_url) LIKE ('%me'))