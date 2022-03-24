
SELECT count(DISTINCT account.id)
FROM account,
     site,
     so_user,
     question q,
     post_link pl,
     tag,
     tag_question tq
WHERE NOT EXISTS
    (SELECT *
     FROM answer a
     WHERE a.site_id = q.site_id
       AND a.question_id = q.id)
  AND site.site_name = 'stackoverflow'
  AND site.site_id = q.site_id
  AND pl.site_id = q.site_id
  AND pl.post_id_to = q.id
  AND tag.name = 'gnu-make'
  AND tag.site_id = q.site_id
  AND q.creation_date > date('2015-01-01')
  AND tq.site_id = tag.site_id
  AND tq.tag_id = tag.id
  AND tq.question_id = q.id
  AND q.owner_user_id = so_user.id
  AND q.site_id = so_user.site_id
  AND so_user.reputation > 63
  AND account.id = so_user.account_id
  AND account.website_url != '';