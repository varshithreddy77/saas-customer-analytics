CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_users (
  user_id     TEXT PRIMARY KEY,
  created_at  DATE NOT NULL,
  industry    TEXT,
  region      TEXT,
  sales_rep   TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_plans (
  plan_id        TEXT PRIMARY KEY,
  plan_name      TEXT NOT NULL,
  price_usd      NUMERIC(10,2) NOT NULL,
  billing_period TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.raw_subscriptions (
  subscription_id TEXT PRIMARY KEY,
  user_id         TEXT NOT NULL REFERENCES raw.raw_users(user_id),
  plan_id         TEXT NOT NULL REFERENCES raw.raw_plans(plan_id),
  start_at        DATE NOT NULL,
  end_at          DATE,
  status          TEXT NOT NULL,   -- active / canceled
  cancel_reason   TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_nps (
  nps_id    TEXT PRIMARY KEY,
  user_id   TEXT NOT NULL REFERENCES raw.raw_users(user_id),
  survey_at DATE NOT NULL,
  nps_score INTEGER
);

CREATE INDEX IF NOT EXISTS idx_users_created_at ON raw.raw_users(created_at);
CREATE INDEX IF NOT EXISTS idx_subs_status ON raw.raw_subscriptions(status);
