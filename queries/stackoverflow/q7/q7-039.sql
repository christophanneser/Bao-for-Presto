
SELECT count(DISTINCT account.display_name)
FROM account,
     so_user,
     badge b1,
     badge b2
WHERE account.website_url != ''
  AND account.id = so_user.account_id
  AND b1.site_id = so_user.site_id
  AND b1.user_id = so_user.id
  AND b1.name = 'Commentator'
  AND b2.site_id = so_user.site_id
  AND b2.user_id = so_user.id
  AND b2.name = 'Legendary'
  AND b2.date > b1.date + INTERVAL '5' MONTH