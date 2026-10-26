use std::collections::{BTreeMap, BTreeSet};

use apalis_core::backend::{Backend, ListQueues, QueueInfo, StatType, Statistic};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Connection, types::SurrealValue};

use crate::{SurrealStorage, error::Error};

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
pub(crate) struct JobStatRow {
    pub job_type: String,
    pub status:   String,
    pub run_at:   DateTime<Utc>,
    pub done_at:  Option<DateTime<Utc>>,
    pub lock_by:  Option<String>,
}

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
struct WorkerTypeRow {
    pub worker_type: String,
}

pub const SELECT_JOB_STATS_SURQL: &str = r#"
SELECT job_type, status, run_at, done_at, lock_by FROM apalis_jobs;
SELECT worker_type FROM apalis_workers;
"#;

pub(crate) fn compute_statistics_from_rows(rows: &[JobStatRow], include_extended: bool) -> Vec<Statistic> {
    let now = Utc::now();
    let today = now.date_naive();
    let past_hour = now - Duration::hours(1);
    let past_24h = now - Duration::days(1);
    let past_7d = now - Duration::days(7);

    let total_jobs = rows.len();
    let mut running_jobs = 0usize;
    let mut pending_jobs = 0usize;
    let mut failed_jobs = 0usize;
    let mut done_jobs = 0usize;
    let mut killed_jobs = 0usize;
    let mut active_jobs = 0usize;
    let mut stale_running_jobs = 0usize;
    let mut jobs_past_hour = 0usize;
    let mut jobs_today = 0usize;
    let mut killed_jobs_today = 0usize;
    let mut jobs_past_24h = 0usize;
    let mut done_past_24h = 0usize;
    let mut jobs_past_7d = 0usize;
    let mut killed_past_7d = 0usize;
    let mut queue_backlog = 0usize;

    let mut duration_mins_sum = 0.0f64;
    let mut duration_mins_count = 0usize;
    let mut longest_running_mins = 0.0f64;
    let mut most_recent_job: Option<DateTime<Utc>> = None;
    let mut oldest_pending_job: Option<DateTime<Utc>> = None;
    let mut hourly_counts: BTreeMap<u32, usize> = BTreeMap::new();

    for row in rows {
        match row.status.as_str() {
        "Running" => {
            running_jobs += 1;
            active_jobs += 1;
            if row.run_at < past_hour {
                stale_running_jobs += 1;
            }
            let mins = (now - row.run_at).num_seconds().max(0) as f64 / 60.0;
            if mins > longest_running_mins {
                longest_running_mins = mins;
            }
        },
        "Pending" => {
            pending_jobs += 1;
            active_jobs += 1;
            if row.run_at <= now {
                queue_backlog += 1;
                oldest_pending_job = Some(match oldest_pending_job {
                Some(prev) => prev.min(row.run_at),
                None => row.run_at,
                });
            }
        },
        "Queued" => {
            active_jobs += 1;
        },
        "Failed" => {
            failed_jobs += 1;
        },
        "Done" => {
            done_jobs += 1;
        },
        "Killed" => {
            killed_jobs += 1;
        },
        _ => {},
        }

        if matches!(row.status.as_str(), "Done" | "Failed" | "Killed")
            && let Some(done_at) = row.done_at
        {
            let mins = (done_at - row.run_at).num_seconds() as f64 / 60.0;
            duration_mins_sum += mins;
            duration_mins_count += 1;
        }

        if row.run_at >= past_hour {
            jobs_past_hour += 1;
        }
        if row.run_at.date_naive() == today {
            jobs_today += 1;
            if row.status == "Killed" {
                killed_jobs_today += 1;
            }
        }
        if row.run_at >= past_24h {
            jobs_past_24h += 1;
            if row.status == "Done" {
                done_past_24h += 1;
            }
            use chrono::Timelike;
            *hourly_counts.entry(row.run_at.hour()).or_insert(0) += 1;
        }
        if row.run_at >= past_7d {
            jobs_past_7d += 1;
            if row.status == "Killed" {
                killed_past_7d += 1;
            }
        }

        most_recent_job = Some(match most_recent_job {
        Some(prev) => prev.max(row.run_at),
        None => row.run_at,
        });
    }

    let round2 = |v: f64| ((v * 100.0).round() / 100.0).to_string();
    let kill_rate = if total_jobs > 0 {
        round2(100.0 * killed_jobs as f64 / total_jobs as f64)
    }
    else {
        "0".to_owned()
    };
    let success_rate = if total_jobs > 0 {
        round2(100.0 * done_jobs as f64 / total_jobs as f64)
    }
    else {
        "0".to_owned()
    };
    let avg_duration = if duration_mins_count > 0 {
        round2(duration_mins_sum / duration_mins_count as f64)
    }
    else {
        "0".to_owned()
    };

    let mut stats = vec![
        Statistic {
            title:     "RUNNING_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     running_jobs.to_string(),
            priority:  Some(1),
        },
        Statistic {
            title:     "PENDING_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     pending_jobs.to_string(),
            priority:  Some(1),
        },
        Statistic {
            title:     "FAILED_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     failed_jobs.to_string(),
            priority:  Some(if include_extended { 2 } else { 1 }),
        },
        Statistic {
            title:     "ACTIVE_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     active_jobs.to_string(),
            priority:  Some(2),
        },
        Statistic {
            title:     "STALE_RUNNING_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     stale_running_jobs.to_string(),
            priority:  Some(2),
        },
        Statistic {
            title:     "KILL_RATE".to_owned(),
            stat_type: StatType::Percentage,
            value:     kill_rate,
            priority:  Some(2),
        },
        Statistic {
            title:     "JOBS_PAST_HOUR".to_owned(),
            stat_type: StatType::Number,
            value:     jobs_past_hour.to_string(),
            priority:  Some(3),
        },
        Statistic {
            title:     "JOBS_TODAY".to_owned(),
            stat_type: StatType::Number,
            value:     jobs_today.to_string(),
            priority:  Some(3),
        },
        Statistic {
            title:     "KILLED_JOBS_TODAY".to_owned(),
            stat_type: StatType::Number,
            value:     killed_jobs_today.to_string(),
            priority:  Some(3),
        },
        Statistic {
            title:     "AVG_JOBS_PER_MINUTE_PAST_HOUR".to_owned(),
            stat_type: StatType::Decimal,
            value:     round2(jobs_past_hour as f64 / 60.0),
            priority:  Some(3),
        },
        Statistic {
            title:     "TOTAL_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     total_jobs.to_string(),
            priority:  Some(4),
        },
        Statistic {
            title:     "DONE_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     done_jobs.to_string(),
            priority:  Some(4),
        },
        Statistic {
            title:     "KILLED_JOBS".to_owned(),
            stat_type: StatType::Number,
            value:     killed_jobs.to_string(),
            priority:  Some(4),
        },
        Statistic {
            title:     "SUCCESS_RATE".to_owned(),
            stat_type: StatType::Percentage,
            value:     success_rate,
            priority:  Some(4),
        },
        Statistic {
            title:     "AVG_JOB_DURATION_MINS".to_owned(),
            stat_type: StatType::Decimal,
            value:     avg_duration,
            priority:  Some(5),
        },
        Statistic {
            title:     "LONGEST_RUNNING_JOB_MINS".to_owned(),
            stat_type: StatType::Decimal,
            value:     round2(longest_running_mins),
            priority:  Some(5),
        },
        Statistic {
            title:     "JOBS_PAST_7_DAYS".to_owned(),
            stat_type: StatType::Number,
            value:     jobs_past_7d.to_string(),
            priority:  Some(6),
        },
        Statistic {
            title:     "MOST_RECENT_JOB".to_owned(),
            stat_type: StatType::Timestamp,
            value:     most_recent_job
                .map(|dt| {
                    if include_extended {
                        dt.timestamp().to_string()
                    }
                    else {
                        dt.to_rfc3339()
                    }
                })
                .unwrap_or_else(|| "0".to_owned()),
            priority:  Some(8),
        },
    ];

    if include_extended {
        let completed_jobs = done_jobs + failed_jobs + killed_jobs;
        let success_rate_24h = if jobs_past_24h > 0 {
            round2(100.0 * done_past_24h as f64 / jobs_past_24h as f64)
        }
        else {
            "0".to_owned()
        };
        let peak_hour = hourly_counts.values().copied().max().unwrap_or(0);

        stats.extend([
            Statistic {
                title:     "COMPLETED_JOBS".to_owned(),
                stat_type: StatType::Number,
                value:     completed_jobs.to_string(),
                priority:  Some(4),
            },
            Statistic {
                title:     "QUEUE_BACKLOG".to_owned(),
                stat_type: StatType::Number,
                value:     queue_backlog.to_string(),
                priority:  Some(5),
            },
            Statistic {
                title:     "JOBS_PAST_24_HOURS".to_owned(),
                stat_type: StatType::Number,
                value:     jobs_past_24h.to_string(),
                priority:  Some(6),
            },
            Statistic {
                title:     "KILLED_JOBS_PAST_7_DAYS".to_owned(),
                stat_type: StatType::Number,
                value:     killed_past_7d.to_string(),
                priority:  Some(6),
            },
            Statistic {
                title:     "SUCCESS_RATE_PAST_24H".to_owned(),
                stat_type: StatType::Percentage,
                value:     success_rate_24h,
                priority:  Some(6),
            },
            Statistic {
                title:     "AVG_JOBS_PER_HOUR_PAST_24H".to_owned(),
                stat_type: StatType::Decimal,
                value:     round2(jobs_past_24h as f64 / 24.0),
                priority:  Some(7),
            },
            Statistic {
                title:     "AVG_JOBS_PER_DAY_PAST_7D".to_owned(),
                stat_type: StatType::Decimal,
                value:     round2(jobs_past_7d as f64 / 7.0),
                priority:  Some(7),
            },
            Statistic {
                title:     "OLDEST_PENDING_JOB".to_owned(),
                stat_type: StatType::Timestamp,
                value:     oldest_pending_job
                    .map(|dt| dt.timestamp().to_string())
                    .unwrap_or_else(|| "0".to_owned()),
                priority:  Some(8),
            },
            Statistic {
                title:     "PEAK_HOUR_JOBS".to_owned(),
                stat_type: StatType::Number,
                value:     peak_hour.to_string(),
                priority:  Some(8),
            },
        ]);
    }

    stats.sort_by(|a, b| a.priority.cmp(&b.priority).then_with(|| a.title.cmp(&b.title)));
    stats
}

impl<Args, C> ListQueues for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let db = self.p.db.clone();

        async move {
            let mut response = db.query(SELECT_JOB_STATS_SURQL).await?.check()?;
            let job_rows: Vec<JobStatRow> = response.take(0)?;
            let worker_rows: Vec<WorkerTypeRow> = response.take(1)?;

            let mut all_queues: BTreeSet<String> = BTreeSet::new();
            let mut jobs_by_queue: BTreeMap<String, Vec<JobStatRow>> = BTreeMap::new();

            for w in worker_rows {
                all_queues.insert(w.worker_type);
            }
            for j in job_rows {
                all_queues.insert(j.job_type.clone());
                jobs_by_queue.entry(j.job_type.clone()).or_default().push(j);
            }

            let past_7d = Utc::now() - Duration::days(7);
            let mut result = Vec::with_capacity(all_queues.len());

            for queue_name in all_queues {
                let rows = jobs_by_queue.remove(&queue_name).unwrap_or_default();
                let stats = if rows.is_empty() {
                    Vec::new()
                }
                else {
                    compute_statistics_from_rows(&rows, false)
                };

                let mut lockers = BTreeSet::new();
                let mut daily_counts: BTreeMap<chrono::NaiveDate, usize> = BTreeMap::new();

                for r in &rows {
                    if let Some(ref lock_by) = r.lock_by {
                        lockers.insert(lock_by.clone());
                    }
                    if r.run_at >= past_7d {
                        *daily_counts.entry(r.run_at.date_naive()).or_insert(0) += 1;
                    }
                }

                result.push(QueueInfo {
                    name: queue_name,
                    stats,
                    workers: lockers.into_iter().collect(),
                    activity: daily_counts.into_values().collect(),
                });
            }

            Ok(result)
        }
    }
}
