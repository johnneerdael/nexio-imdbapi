import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const db = useDb()

  const [snapshot, counts, keys] = await Promise.all([
    db.query(
      `
        select id, status, source_updated_at, imported_at, dataset_version
        from imdb_snapshots
        order by imported_at desc nulls last
        limit 1
      `
    ),
    db.query(
      `
        select
          (select count(*) from titles) as title_count,
          (select count(*) from title_ratings) as rating_count,
          (select count(*) from title_episodes) as episode_count,
          (select count(*) from names) as name_count
      `
    ),
    db.query(
      `
        select id, key_prefix, label, created_at, last_used_at, revoked_at
        from api_keys
        where created_by_user_id = $1
        order by created_at desc
      `,
      [session.user.id]
    )
  ])

  return {
    user: session.user,
    snapshot: snapshot.rows[0] || null,
    stats: counts.rows[0] || {
      title_count: 0,
      rating_count: 0,
      episode_count: 0,
      name_count: 0
    },
    apiKeys: keys.rows
  }
})

