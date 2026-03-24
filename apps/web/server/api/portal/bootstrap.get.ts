import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const db = useDb()

  const [snapshot, episodeEstimate, keys] = await Promise.all([
    db.query(
      `
        select
          id,
          dataset_name,
          status,
          source_url,
          notes,
          imported_at,
          completed_at,
          is_active,
          sync_mode,
          title_count,
          name_count,
          rating_count,
          episode_count,
          principal_count,
          crew_member_count,
          aka_count
        from imdb_snapshots
        order by is_active desc, imported_at desc nulls last
        limit 1
      `
    ),
    db.query(
      `
        select
          coalesce(
            (
              select case
                when n_live_tup > 0 then n_live_tup::bigint
                else 0::bigint
              end
              from pg_stat_all_tables
              where schemaname = 'public'
                and relname = 'title_episodes'
              limit 1
            ),
            0::bigint
          ) as episode_count
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
        episode_count: Number(snapshot.rows[0].episode_count || 0),
        principal_count: Number(snapshot.rows[0].principal_count || 0),
        crew_member_count: Number(snapshot.rows[0].crew_member_count || 0),
        aka_count: Number(snapshot.rows[0].aka_count || 0),
        status: String(snapshot.rows[0].status || (snapshot.rows[0].is_active ? 'active' : 'staged'))
      }
    : null

  const statsRow = snapshot.rows[0]
    ? {
        title_count: Number(snapshot.rows[0].title_count || 0),
        rating_count: Number(snapshot.rows[0].rating_count || 0),
        episode_count: Number(snapshot.rows[0].episode_count || episodeEstimate.rows[0]?.episode_count || 0),
        name_count: Number(snapshot.rows[0].name_count || 0)
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
