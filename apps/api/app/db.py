from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import settings

pool: ConnectionPool | None = None

_SCHEMA_COMPAT_QUERIES = (
    """
    CREATE TABLE IF NOT EXISTS assistant_turns (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      turn_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      conversation_id TEXT NOT NULL,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      route TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'RUNNING',
      current_phase TEXT NOT NULL DEFAULT 'understand',
      response_type TEXT NOT NULL DEFAULT 'direct_answer',
      user_message TEXT NOT NULL,
      assistant_message TEXT,
      task_id UUID,
      trace_id TEXT NOT NULL,
      runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, turn_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_episodes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      episode_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      conversation_id TEXT,
      turn_id TEXT,
      task_id UUID,
      normalized_goal TEXT NOT NULL,
      task_summary TEXT NOT NULL,
      chosen_strategy TEXT NOT NULL,
      action_types TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
      tool_names TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
      outcome_status TEXT NOT NULL DEFAULT '',
      final_outcome TEXT NOT NULL DEFAULT '',
      useful_lessons TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
      episode_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, episode_id)
    )
    """,
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS conversation_id TEXT",
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assistant_turn_id TEXT",
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS goal_id TEXT",
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'task_api'",
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB",
    "ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS last_task_result JSONB NOT NULL DEFAULT '{}'::JSONB",
    "ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS last_tool_result JSONB NOT NULL DEFAULT '{}'::JSONB",
    "ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS user_preferences JSONB NOT NULL DEFAULT '{}'::JSONB",
    "ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS title TEXT",
    "ALTER TABLE assistant_turns ADD COLUMN IF NOT EXISTS response_type TEXT NOT NULL DEFAULT 'direct_answer'",
    "ALTER TABLE assistant_turns ADD COLUMN IF NOT EXISTS runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_assistant_turns_tenant_id ON assistant_turns (tenant_id, turn_id)",
    "CREATE INDEX IF NOT EXISTS idx_assistant_turns_conversation_created_at ON assistant_turns (tenant_id, conversation_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_assistant_turns_task_id ON assistant_turns (tenant_id, task_id, updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_assistant_episodes_tenant_id ON assistant_episodes (tenant_id, episode_id)",
    "CREATE INDEX IF NOT EXISTS idx_assistant_episodes_user_created_at ON assistant_episodes (tenant_id, user_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_assistant_episodes_task_id ON assistant_episodes (tenant_id, task_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_conversation_created_at ON tasks (tenant_id, conversation_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_assistant_turn_id ON tasks (tenant_id, assistant_turn_id)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_goal_id ON tasks (tenant_id, goal_id, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS agent_goals (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      goal_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      conversation_id TEXT,
      normalized_goal TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVE',
      goal_state JSONB NOT NULL DEFAULT '{}'::JSONB,
      current_task_id UUID,
      last_turn_id TEXT,
      continuation_count INT NOT NULL DEFAULT 0,
      policy_version_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, goal_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_policy_versions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      version_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      version_tag TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'CANDIDATE',
      base_version_id TEXT,
      source TEXT NOT NULL DEFAULT 'manual',
      memory_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
      comparison_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
      created_by TEXT,
      activated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, version_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_policy_eval_runs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      eval_run_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      candidate_version_id TEXT NOT NULL,
      baseline_version_id TEXT NOT NULL,
      summary JSONB NOT NULL DEFAULT '{}'::JSONB,
      verdict JSONB NOT NULL DEFAULT '{}'::JSONB,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, eval_run_id)
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_goals_tenant_goal_id ON agent_goals (tenant_id, goal_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_goals_user_updated_at ON agent_goals (tenant_id, user_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_goals_status ON agent_goals (tenant_id, status, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS agent_subgoals (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      subgoal_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      goal_id TEXT NOT NULL,
      sequence_no INT NOT NULL DEFAULT 0,
      title TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING',
      depends_on JSONB NOT NULL DEFAULT '[]'::JSONB,
      checkpoint_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
      wake_condition JSONB NOT NULL DEFAULT '{}'::JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, subgoal_id),
      UNIQUE (tenant_id, goal_id, sequence_no)
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_policy_versions_tenant_version_id ON agent_policy_versions (tenant_id, version_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_policy_versions_status ON agent_policy_versions (tenant_id, status, updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_policy_eval_runs_tenant_eval_id ON agent_policy_eval_runs (tenant_id, eval_run_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_subgoals_tenant_subgoal_id ON agent_subgoals (tenant_id, subgoal_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_subgoals_goal_sequence ON agent_subgoals (tenant_id, goal_id, sequence_no ASC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_subgoals_status ON agent_subgoals (tenant_id, status, updated_at DESC)",
    "ALTER TABLE agent_subgoals ADD COLUMN IF NOT EXISTS depends_on JSONB NOT NULL DEFAULT '[]'::JSONB",
)


def init_pool() -> None:
    global pool
    if pool is None:
        pool = ConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=10,
            kwargs={"autocommit": True},
            open=True,
        )


def close_pool() -> None:
    global pool
    if pool is not None:
        pool.close()
        pool = None


def ensure_schema_compat() -> None:
    p = _ensure_pool()
    with p.connection() as conn:
        with conn.cursor() as cur:
            for query in _SCHEMA_COMPAT_QUERIES:
                cur.execute(query)


def _ensure_pool() -> ConnectionPool:
    if pool is None:
        init_pool()
    assert pool is not None
    return pool


def execute(query: str, params: Any = None) -> int:
    p = _ensure_pool()
    with p.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.rowcount


def fetchone(query: str, params: Any = None) -> dict[str, Any] | None:
    p = _ensure_pool()
    with p.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchone()


def fetchall(query: str, params: Any = None) -> list[dict[str, Any]]:
    p = _ensure_pool()
    with p.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            return list(rows)


@contextmanager
def transaction_cursor():
    p = _ensure_pool()
    with p.connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                yield cur
