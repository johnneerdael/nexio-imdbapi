import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const db = useDb()

  const [snapshot, counts, keys] = await Promise.all([
    db.query(
      `
        select
          id,
          dataset_name,
          source_url,
          notes,
          imported_at,
          completed_at,
          is_active,
          title_count,
          name_count,
          rating_count
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
        select id, key_prefix, name as label, created_at, last_used_at, revoked_at
        from api_keys
        where user_id = $1
        order by created_at desc
      `,
      [session.user.id]
    )
  ])

  const snapshotRow = snapshot.rows[0]
    ? {
        ...snapshot.rows[0],
        is_active: Boolean(snapshot.rows[0].is_active),
        title_count: Number(snapshot.rows[0].title_count || 0),
        name_count: Number(snapshot.rows[0].name_count || 0),
        rating_count: Number(snapshot.rows[0].rating_count || 0),
        status: snapshot.rows[0].is_active ? 'active' : 'staged'
      }
    : null

  const statsRow = counts.rows[0]
    ? {
        title_count: Number(counts.rows[0].title_count || 0),
        rating_count: Number(counts.rows[0].rating_count || 0),
        episode_count: Number(counts.rows[0].episode_count || 0),
        name_count: Number(counts.rows[0].name_count || 0)
      }
    : {
        title_count: 0,
        rating_count: 0,
        episode_count: 0,
        name_count: 0
      }

  return {
    user: session.user,
    snapshot: snapshotRow,
    stats: statsRow,
    apiKeys: keys.rows
  }
})
