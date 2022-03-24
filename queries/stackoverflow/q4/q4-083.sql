
SELECT COUNT(DISTINCT account.display_name)
FROM tag t1,
     site s1,
     question q1,
     answer a1,
     tag_question tq1,
     so_user u1,
     account
WHERE -- answerers posted at least 1 yr after the question was asked
s1.site_name='apple'
  AND t1.name = 'macos'
  AND t1.site_id = s1.site_id
  AND q1.site_id = s1.site_id
  AND tq1.site_id = s1.site_id
  AND tq1.question_id = q1.id
  AND tq1.tag_id = t1.id
  AND a1.site_id = q1.site_id
  AND a1.question_id = q1.id
  AND a1.owner_user_id = u1.id
  AND a1.site_id = u1.site_id
  AND a1.creation_date >= q1.creation_date + INTERVAL '1' YEAR
  AND -- to get the display name
account.id = u1.account_id;