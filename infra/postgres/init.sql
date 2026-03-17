CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
  CREATE TYPE user_role AS ENUM ('owner', 'operator', 'user');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
  CREATE TYPE task_status AS ENUM (
    'RECEIVED',
    'VALIDATING',
    'PLANNING',
    'QUEUED',
    'RUNNING',
    'WAITING_TOOL',
    'WAITING_HUMAN',
    'REVIEWING',
    'SUCCEEDED',
    'FAILED_RETRYABLE',
    'FAILED_FINAL',
    'CANCELLED',
    'TIMED_OUT'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
  CREATE TYPE approval_status AS ENUM ('WAITING_HUMAN', 'APPROVED', 'REJECTED', 'EDITED');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  email TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role user_role NOT NULL DEFAULT 'user',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, email)
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS assistant_conversations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  message_history JSONB NOT NULL DEFAULT '[]'::JSONB,
  last_task_result JSONB NOT NULL DEFAULT '{}'::JSONB,
  last_tool_result JSONB NOT NULL DEFAULT '{}'::JSONB,
  user_preferences JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, conversation_id)
);

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
);

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
);

CREATE TABLE IF NOT EXISTS roles (
  tenant_id TEXT NOT NULL DEFAULT 'default',
  role_name user_role NOT NULL,
  description TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, role_name)
);

CREATE TABLE IF NOT EXISTS policies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  name TEXT NOT NULL,
  effect TEXT NOT NULL CHECK (effect IN ('allow', 'deny')),
  role_min user_role NOT NULL DEFAULT 'user',
  task_type TEXT,
  tool_id TEXT,
  environment TEXT NOT NULL DEFAULT 'local',
  is_write_action BOOLEAN NOT NULL DEFAULT FALSE,
  requires_approval BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  client_request_id TEXT NOT NULL,
  task_type TEXT NOT NULL,
  status task_status NOT NULL DEFAULT 'RECEIVED',
  created_by UUID NOT NULL REFERENCES users(id),
  input_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  output_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  error_code TEXT,
  error_message TEXT,
  trace_id TEXT NOT NULL,
  cost_total NUMERIC(14, 6) NOT NULL DEFAULT 0,
  task_cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
  budget NUMERIC(14, 6) NOT NULL DEFAULT 1.0,
  requires_hitl BOOLEAN NOT NULL DEFAULT FALSE,
  input_raw_encrypted TEXT NOT NULL DEFAULT '',
  runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, client_request_id)
);

CREATE TABLE IF NOT EXISTS runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  run_no INTEGER NOT NULL,
  status task_status NOT NULL DEFAULT 'RECEIVED',
  workflow_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  assigned_worker TEXT NOT NULL DEFAULT 'worker-local',
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  run_cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (task_id, run_no)
);

CREATE TABLE IF NOT EXISTS steps (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_key TEXT NOT NULL,
  status task_status NOT NULL,
  payload_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  trace_id TEXT NOT NULL,
  span_id TEXT,
  attempt INTEGER NOT NULL DEFAULT 1,
  status_event_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tool_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  tool_id TEXT NOT NULL,
  version TEXT NOT NULL,
  description TEXT NOT NULL,
  required_scopes TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
  input_schema JSONB NOT NULL,
  output_schema JSONB NOT NULL,
  auth_type TEXT NOT NULL,
  rate_limit_rpm INTEGER NOT NULL,
  run_limit INTEGER NOT NULL DEFAULT 20,
  timeout_connect_s INTEGER NOT NULL DEFAULT 5,
  timeout_read_s INTEGER NOT NULL DEFAULT 10,
  timeout_overall_s INTEGER NOT NULL DEFAULT 15,
  idempotency_strategy TEXT NOT NULL,
  audit_fields TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
  masking_rules JSONB NOT NULL DEFAULT '{}'::JSONB,
  egress_policy JSONB NOT NULL DEFAULT '{}'::JSONB,
  risk_level TEXT NOT NULL DEFAULT 'low',
  requires_approval BOOLEAN NOT NULL DEFAULT FALSE,
  supported_use_cases TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_by UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, tool_id, version)
);

CREATE TABLE IF NOT EXISTS tool_calls (
  tool_call_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  tool_id TEXT NOT NULL,
  caller_user_id UUID NOT NULL REFERENCES users(id),
  request_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  response_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  status TEXT NOT NULL,
  reason_code TEXT,
  trace_id TEXT NOT NULL,
  idempotency_key TEXT,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS approvals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  status approval_status NOT NULL DEFAULT 'WAITING_HUMAN',
  requested_by UUID NOT NULL REFERENCES users(id),
  decided_by UUID REFERENCES users(id),
  edited_output TEXT,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS approval_signal_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  approval_id UUID NOT NULL REFERENCES approvals(id) ON DELETE CASCADE,
  run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  workflow_id TEXT NOT NULL,
  signal_name TEXT NOT NULL DEFAULT 'approval_signal',
  signal_payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error TEXT,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, approval_id)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  run_id UUID REFERENCES runs(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL,
  uri TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cost_ledger (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  run_id UUID REFERENCES runs(id) ON DELETE CASCADE,
  category TEXT NOT NULL,
  amount NUMERIC(14, 6) NOT NULL DEFAULT 0,
  currency TEXT NOT NULL DEFAULT 'USD',
  token_in INTEGER NOT NULL DEFAULT 0,
  token_out INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  actor_user_id UUID REFERENCES users(id),
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  detail_masked JSONB NOT NULL DEFAULT '{}'::JSONB,
  trace_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created_at ON tasks (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_task_created_at ON runs (task_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_tenant_id_id ON users (tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_tenant_id_id ON tasks (tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_tenant_id_id ON runs (tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_approvals_tenant_id_id ON approvals (tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tool_registry_tenant_id_id ON tool_registry (tenant_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_refresh_tokens_tenant_token_hash ON refresh_tokens (tenant_id, token_hash);
CREATE UNIQUE INDEX IF NOT EXISTS ux_assistant_conversations_tenant_id ON assistant_conversations (tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_assistant_conversations_user_updated_at
  ON assistant_conversations (tenant_id, user_id, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_assistant_turns_tenant_id ON assistant_turns (tenant_id, turn_id);
CREATE INDEX IF NOT EXISTS idx_assistant_turns_conversation_created_at
  ON assistant_turns (tenant_id, conversation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_assistant_turns_task_id
  ON assistant_turns (tenant_id, task_id, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_assistant_episodes_tenant_id ON assistant_episodes (tenant_id, episode_id);
CREATE INDEX IF NOT EXISTS idx_assistant_episodes_user_created_at
  ON assistant_episodes (tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_assistant_episodes_task_id
  ON assistant_episodes (tenant_id, task_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_steps_run_created_at ON steps (run_id, created_at ASC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_steps_tenant_run_status_event ON steps (tenant_id, run_id, status_event_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_run_id ON tool_calls (run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tool_calls_task_id ON tool_calls (task_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tool_calls_tenant_tool_call_id ON tool_calls (tenant_id, tool_call_id);
CREATE INDEX IF NOT EXISTS idx_approvals_status_created_at ON approvals (status, created_at DESC);
DO $$
DECLARE
  _idxdef TEXT;
BEGIN
  SELECT indexdef
  INTO _idxdef
  FROM pg_indexes
  WHERE schemaname = current_schema()
    AND indexname = 'idx_approval_signal_outbox_pending';

  IF _idxdef IS NOT NULL AND POSITION('FAILED' IN _idxdef) > 0 THEN
    EXECUTE 'DROP INDEX IF EXISTS idx_approval_signal_outbox_pending';
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_approval_signal_outbox_pending
  ON approval_signal_outbox (status, next_attempt_at, created_at)
  WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log (created_at DESC);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_refresh_tokens_tenant_user') THEN
    ALTER TABLE refresh_tokens
      ADD CONSTRAINT fk_refresh_tokens_tenant_user
      FOREIGN KEY (tenant_id, user_id)
      REFERENCES users (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_assistant_conversations_tenant_user') THEN
    ALTER TABLE assistant_conversations
      ADD CONSTRAINT fk_assistant_conversations_tenant_user
      FOREIGN KEY (tenant_id, user_id)
      REFERENCES users (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_assistant_turns_tenant_user') THEN
    ALTER TABLE assistant_turns
      ADD CONSTRAINT fk_assistant_turns_tenant_user
      FOREIGN KEY (tenant_id, user_id)
      REFERENCES users (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_assistant_episodes_tenant_user') THEN
    ALTER TABLE assistant_episodes
      ADD CONSTRAINT fk_assistant_episodes_tenant_user
      FOREIGN KEY (tenant_id, user_id)
      REFERENCES users (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_tasks_tenant_created_by') THEN
    ALTER TABLE tasks
      ADD CONSTRAINT fk_tasks_tenant_created_by
      FOREIGN KEY (tenant_id, created_by)
      REFERENCES users (tenant_id, id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_runs_tenant_task') THEN
    ALTER TABLE runs
      ADD CONSTRAINT fk_runs_tenant_task
      FOREIGN KEY (tenant_id, task_id)
      REFERENCES tasks (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_steps_tenant_run') THEN
    ALTER TABLE steps
      ADD CONSTRAINT fk_steps_tenant_run
      FOREIGN KEY (tenant_id, run_id)
      REFERENCES runs (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_tool_calls_tenant_run') THEN
    ALTER TABLE tool_calls
      ADD CONSTRAINT fk_tool_calls_tenant_run
      FOREIGN KEY (tenant_id, run_id)
      REFERENCES runs (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_tool_calls_tenant_task') THEN
    ALTER TABLE tool_calls
      ADD CONSTRAINT fk_tool_calls_tenant_task
      FOREIGN KEY (tenant_id, task_id)
      REFERENCES tasks (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_approvals_tenant_task') THEN
    ALTER TABLE approvals
      ADD CONSTRAINT fk_approvals_tenant_task
      FOREIGN KEY (tenant_id, task_id)
      REFERENCES tasks (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_approvals_tenant_run') THEN
    ALTER TABLE approvals
      ADD CONSTRAINT fk_approvals_tenant_run
      FOREIGN KEY (tenant_id, run_id)
      REFERENCES runs (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_artifacts_tenant_task') THEN
    ALTER TABLE artifacts
      ADD CONSTRAINT fk_artifacts_tenant_task
      FOREIGN KEY (tenant_id, task_id)
      REFERENCES tasks (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_artifacts_tenant_run') THEN
    ALTER TABLE artifacts
      ADD CONSTRAINT fk_artifacts_tenant_run
      FOREIGN KEY (tenant_id, run_id)
      REFERENCES runs (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_cost_ledger_tenant_task') THEN
    ALTER TABLE cost_ledger
      ADD CONSTRAINT fk_cost_ledger_tenant_task
      FOREIGN KEY (tenant_id, task_id)
      REFERENCES tasks (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_cost_ledger_tenant_run') THEN
    ALTER TABLE cost_ledger
      ADD CONSTRAINT fk_cost_ledger_tenant_run
      FOREIGN KEY (tenant_id, run_id)
      REFERENCES runs (tenant_id, id)
      ON DELETE CASCADE;
  END IF;
END
$$;

INSERT INTO roles (tenant_id, role_name, description)
VALUES
  ('default', 'owner', 'Full access to config, users, and tools'),
  ('default', 'operator', 'Can approve/reject/rerun/cancel and read audit'),
  ('default', 'user', 'Can create tasks and read own tasks')
ON CONFLICT DO NOTHING;

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS input_raw_encrypted TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS conversation_id TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assistant_turn_id TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'task_api';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS assigned_worker TEXT NOT NULL DEFAULT 'worker-local';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS run_cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS status_event_id TEXT;
ALTER TABLE tool_registry ADD COLUMN IF NOT EXISTS risk_level TEXT NOT NULL DEFAULT 'low';
ALTER TABLE tool_registry ADD COLUMN IF NOT EXISTS requires_approval BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE tool_registry ADD COLUMN IF NOT EXISTS supported_use_cases TEXT[] NOT NULL DEFAULT '{}'::TEXT[];
ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS last_task_result JSONB NOT NULL DEFAULT '{}'::JSONB;
ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS last_tool_result JSONB NOT NULL DEFAULT '{}'::JSONB;
ALTER TABLE assistant_conversations ADD COLUMN IF NOT EXISTS user_preferences JSONB NOT NULL DEFAULT '{}'::JSONB;
ALTER TABLE assistant_turns ADD COLUMN IF NOT EXISTS response_type TEXT NOT NULL DEFAULT 'direct_answer';
ALTER TABLE assistant_turns ADD COLUMN IF NOT EXISTS runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runtime_state JSONB NOT NULL DEFAULT '{}'::JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS goal_id TEXT;
CREATE INDEX IF NOT EXISTS idx_tasks_conversation_created_at ON tasks (tenant_id, conversation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_assistant_turn_id ON tasks (tenant_id, assistant_turn_id);
CREATE INDEX IF NOT EXISTS idx_tasks_goal_id ON tasks (tenant_id, goal_id, updated_at DESC);

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
);

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
);

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
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_goals_tenant_goal_id ON agent_goals (tenant_id, goal_id);
CREATE INDEX IF NOT EXISTS idx_agent_goals_user_updated_at ON agent_goals (tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_goals_status ON agent_goals (tenant_id, status, updated_at DESC);
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
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_policy_versions_tenant_version_id ON agent_policy_versions (tenant_id, version_id);
CREATE INDEX IF NOT EXISTS idx_agent_policy_versions_status ON agent_policy_versions (tenant_id, status, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_policy_eval_runs_tenant_eval_id ON agent_policy_eval_runs (tenant_id, eval_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_subgoals_tenant_subgoal_id ON agent_subgoals (tenant_id, subgoal_id);
CREATE INDEX IF NOT EXISTS idx_agent_subgoals_goal_sequence ON agent_subgoals (tenant_id, goal_id, sequence_no ASC);
CREATE INDEX IF NOT EXISTS idx_agent_subgoals_status ON agent_subgoals (tenant_id, status, updated_at DESC);

INSERT INTO policies (
  tenant_id,
  name,
  effect,
  role_min,
  task_type,
  tool_id,
  environment,
  is_write_action,
  requires_approval
)
VALUES
  ('default', 'allow_rag_for_user', 'allow', 'user', 'rag_qa', NULL, 'local', FALSE, FALSE),
  ('default', 'allow_internal_write_with_approval', 'allow', 'operator', NULL, 'internal_rest_api', 'local', TRUE, TRUE),
  ('default', 'deny_user_email_write', 'deny', 'user', NULL, 'email_ticketing', 'local', TRUE, TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO policies (
  tenant_id,
  name,
  effect,
  role_min,
  task_type,
  tool_id,
  environment,
  is_write_action,
  requires_approval
)
VALUES
  ('default', 'allow_internal_read_for_user', 'allow', 'user', NULL, 'internal_rest_api', 'local', FALSE, FALSE),
  ('default', 'allow_web_search_for_user', 'allow', 'user', NULL, 'web_search', 'local', FALSE, FALSE),
  ('default', 'allow_email_write_operator_approval', 'allow', 'operator', NULL, 'email_ticketing', 'local', TRUE, TRUE),
  ('default', 'allow_object_storage_write_operator_approval', 'allow', 'operator', NULL, 'object_storage', 'local', TRUE, TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO tool_registry (
  tenant_id,
  tool_id,
  version,
  description,
  required_scopes,
  input_schema,
  output_schema,
  auth_type,
  rate_limit_rpm,
  run_limit,
  timeout_connect_s,
  timeout_read_s,
  timeout_overall_s,
  idempotency_strategy,
  audit_fields,
  masking_rules,
  egress_policy,
  risk_level,
  requires_approval,
  supported_use_cases,
  enabled
)
VALUES
  (
    'default',
    'internal_rest_api',
    'v1',
    'Call fake internal REST service with strict allowlist and service token.',
    ARRAY['tool:internal_rest_api:read', 'tool:internal_rest_api:write'],
    '{
      "type":"object",
      "required":["method","path"],
      "properties":{
        "method":{"type":"string","enum":["GET","POST","PUT"]},
        "path":{"type":"string","pattern":"^/records"},
        "params":{"type":"object","additionalProperties":true},
        "body":{"type":"object","additionalProperties":true},
        "idempotency_key":{"type":"string"}
      },
      "additionalProperties":false
    }'::jsonb,
    '{
      "type":"object",
      "required":["status_code","result"],
      "properties":{
        "status_code":{"type":"integer"},
        "result":{"type":"object"}
      },
      "additionalProperties":true
    }'::jsonb,
    'service_token',
    60,
    60,
    2,
    10,
    10,
    'tool_call_id',
    ARRAY['method', 'path', 'status_code'],
    '{"mask_fields":["password","token","secret","authorization"]}'::jsonb,
    '{"allow_domains":[],"deny_private_networks":true}'::jsonb,
    'medium',
    FALSE,
    ARRAY['internal_data_lookup', 'records_query'],
    TRUE
  ),
  (
    'default',
    'web_search',
    'v1',
    'Controlled web search with domain allowlist and private-network egress guard.',
    ARRAY['tool:web_search:read'],
    '{
      "type":"object",
      "required":["query","domain"],
      "properties":{
        "query":{"type":"string","minLength":2},
        "domain":{"type":"string"},
        "top_k":{"type":"integer","minimum":1,"maximum":5}
      },
      "additionalProperties":false
    }'::jsonb,
    '{
      "type":"object",
      "required":["results"],
      "properties":{
        "results":{
          "type":"array",
          "items":{
            "type":"object",
            "required":["title","url","snippet"],
            "properties":{
              "title":{"type":"string"},
              "url":{"type":"string"},
              "snippet":{"type":"string"}
            }
          }
        }
      }
    }'::jsonb,
    'none',
    20,
    20,
    2,
    10,
    15,
    'tool_call_id',
    ARRAY['query', 'domain'],
    '{"mask_fields":[]}'::jsonb,
    '{"allow_domains":["example.com","docs.python.org","developer.mozilla.org"],"deny_private_networks":true}'::jsonb,
    'low',
    FALSE,
    ARRAY['knowledge_lookup', 'docs_search'],
    TRUE
  ),
  (
    'default',
    'email_ticketing',
    'v1',
    'Mock email sender/ticket creator. Write actions require approval.',
    ARRAY['tool:email_ticketing:write'],
    '{
      "type":"object",
      "required":["action","target","subject","body"],
      "properties":{
        "action":{"type":"string","enum":["send_email","create_ticket"]},
        "target":{"type":"string"},
        "subject":{"type":"string"},
        "body":{"type":"string"},
        "approval_id":{"type":"string"}
      },
      "additionalProperties":false
    }'::jsonb,
    '{
      "type":"object",
      "required":["status","message"],
      "properties":{
        "status":{"type":"string"},
        "message":{"type":"string"},
        "ticket_id":{"type":"string"}
      },
      "additionalProperties":true
    }'::jsonb,
    'none',
    30,
    30,
    2,
    10,
    15,
    'tool_call_id',
    ARRAY['action', 'target', 'subject', 'status'],
    '{"mask_fields":["body"]}'::jsonb,
    '{"allow_domains":[],"deny_private_networks":true}'::jsonb,
    'high',
    TRUE,
    ARRAY['ticket_action', 'notification_send'],
    TRUE
  ),
  (
    'default',
    'object_storage',
    'v1',
    'Local artifact writer.',
    ARRAY['tool:object_storage:write'],
    '{
      "type":"object",
      "required":["object_key","content"],
      "properties":{
        "object_key":{"type":"string"},
        "content":{"type":"string"},
        "content_type":{"type":"string"}
      },
      "additionalProperties":false
    }'::jsonb,
    '{
      "type":"object",
      "required":["uri","size"],
      "properties":{
        "uri":{"type":"string"},
        "size":{"type":"integer"}
      }
    }'::jsonb,
    'none',
    120,
    100,
    2,
    30,
    60,
    'tool_call_id',
    ARRAY['object_key', 'uri', 'size'],
    '{"mask_fields":["content"]}'::jsonb,
    '{"allow_domains":[],"deny_private_networks":true}'::jsonb,
    'medium',
    TRUE,
    ARRAY['artifact_write', 'report_persist'],
    TRUE
  )
ON CONFLICT DO NOTHING;
