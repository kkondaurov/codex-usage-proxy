use crate::{
    config::{AppConfig, PricingConfig},
    pricing_remote,
    storage::{
        AggregateTotals, DailyTokenTotal, MissingPriceDetail, ModelUsageRow, PriceRow, PricingMeta,
        SessionAggregate, SessionMessage, SessionTurn, Storage, ToolCountRow, TopModelShare,
    },
};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Local, LocalResult, Months, NaiveDate,
    TimeZone, Utc,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
};
use std::{
    collections::{HashMap, HashSet},
    io::{self, Stdout, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;

const TURN_VIEW_LIMIT: usize = 500;
const MESSAGE_VIEW_LIMIT: usize = 400;
const LIST_TITLE_MAX_CHARS: usize = 200;
const CWD_MAX_CHARS: usize = 22;
const BRANCH_MAX_CHARS: usize = 21;
const MODAL_TIME_COL_WIDTH: u16 = 10;
const MODAL_INFO_COL_WIDTH: u16 = 22;
const MODAL_COST_COL_WIDTH: u16 = 10;
const MODAL_TOKEN_COL_WIDTH: u16 = 7;
const MODAL_REASON_COL_WIDTH: u16 = 9;
const MODAL_CONTEXT_COL_WIDTH: u16 = 11;

const HEATMAP_COLORS: [Color; 7] = [
    Color::Rgb(35, 35, 35),
    Color::Rgb(55, 90, 70),
    Color::Rgb(70, 120, 85),
    Color::Rgb(85, 150, 100),
    Color::Rgb(105, 180, 120),
    Color::Rgb(130, 205, 145),
    Color::Rgb(155, 230, 170),
];
const FUTURE_HEATMAP_COLOR: Color = Color::Rgb(22, 22, 22);
const STREAK_COLORS: [Color; 7] = [
    Color::Rgb(35, 35, 35),
    Color::Rgb(60, 90, 140),
    Color::Rgb(75, 115, 175),
    Color::Rgb(95, 140, 210),
    Color::Rgb(115, 165, 230),
    Color::Rgb(135, 190, 245),
    Color::Rgb(155, 215, 255),
];
const SUMMARY_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const RECENT_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const STATS_REFRESH_INTERVAL: Duration = Duration::from_millis(3000);
const PRICING_REFRESH_INTERVAL: Duration = Duration::from_millis(8000);
const MODAL_MESSAGES_REFRESH_INTERVAL: Duration = Duration::from_millis(1000);
const WRAPPED_REFRESH_INTERVAL: Duration = Duration::from_millis(8000);

#[derive(Copy, Clone, Eq, PartialEq)]
enum ViewMode {
    Overview,
    Sessions,
    Stats,
    Pricing,
}

impl ViewMode {
    fn next(self) -> Self {
        match self {
            ViewMode::Overview => ViewMode::Sessions,
            ViewMode::Sessions => ViewMode::Stats,
            ViewMode::Stats => ViewMode::Pricing,
            ViewMode::Pricing => ViewMode::Overview,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum TimeRange {
    Day,
    Week,
    Month,
    Year,
    All,
}

impl TimeRange {
    fn from_key(ch: char) -> Option<Self> {
        match ch.to_ascii_lowercase() {
            'd' => Some(TimeRange::Day),
            'w' => Some(TimeRange::Week),
            'm' => Some(TimeRange::Month),
            'y' => Some(TimeRange::Year),
            'a' => Some(TimeRange::All),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum SessionSort {
    Recent,
    Cost,
}

impl SessionSort {
    fn from_key(ch: char) -> Option<Self> {
        match ch.to_ascii_lowercase() {
            'r' => Some(SessionSort::Recent),
            'c' => Some(SessionSort::Cost),
            _ => None,
        }
    }
}

struct TimeNavState {
    range: TimeRange,
    anchor: NaiveDate,
}

impl TimeNavState {
    fn new(range: TimeRange, today: NaiveDate) -> Self {
        Self {
            range,
            anchor: today,
        }
    }

    fn set_range(&mut self, range: TimeRange, today: NaiveDate) {
        if self.range != range {
            self.range = range;
            self.anchor = today;
        }
    }

    fn move_prev(&mut self) {
        match self.range {
            TimeRange::Day => {
                self.anchor = self
                    .anchor
                    .checked_sub_signed(ChronoDuration::days(1))
                    .unwrap_or(self.anchor);
            }
            TimeRange::Week => {
                let start = start_of_week_local(self.anchor);
                self.anchor = start
                    .checked_sub_signed(ChronoDuration::days(7))
                    .unwrap_or(start);
            }
            TimeRange::Month => {
                let first = first_day_of_month(self.anchor);
                self.anchor = first.checked_sub_months(Months::new(1)).unwrap_or(first);
            }
            TimeRange::Year => {
                let first =
                    NaiveDate::from_ymd_opt(self.anchor.year(), 1, 1).unwrap_or(self.anchor);
                self.anchor = first.with_year(first.year() - 1).unwrap_or(first);
            }
            TimeRange::All => {}
        }
    }

    fn move_next(&mut self) {
        match self.range {
            TimeRange::Day => {
                self.anchor = self
                    .anchor
                    .checked_add_signed(ChronoDuration::days(1))
                    .unwrap_or(self.anchor);
            }
            TimeRange::Week => {
                let start = start_of_week_local(self.anchor);
                self.anchor = start
                    .checked_add_signed(ChronoDuration::days(7))
                    .unwrap_or(start);
            }
            TimeRange::Month => {
                let first = first_day_of_month(self.anchor);
                self.anchor = first.checked_add_months(Months::new(1)).unwrap_or(first);
            }
            TimeRange::Year => {
                let first =
                    NaiveDate::from_ymd_opt(self.anchor.year(), 1, 1).unwrap_or(self.anchor);
                self.anchor = first.with_year(first.year() + 1).unwrap_or(first);
            }
            TimeRange::All => {}
        }
    }
}

struct UiDataCache {
    hero: HeroStats,
    hero_last: Option<Instant>,
    sessions_rows: Vec<SessionAggregate>,
    sessions_total: usize,
    sessions_offset: usize,
    sessions_limit: usize,
    sessions_last: Option<Instant>,
    sessions_key: Option<(TimeRange, NaiveDate, SessionSort)>,
    stats_data: Option<StatsRangeData>,
    stats_last: Option<Instant>,
    stats_key: Option<(TimeRange, NaiveDate)>,
    pricing_rows: Vec<PriceRow>,
    pricing_missing: Vec<MissingPriceDetail>,
    pricing_meta: Option<PricingMeta>,
    pricing_last: Option<Instant>,
    missing_last: Option<Instant>,
    last_ingest: Option<DateTime<Utc>>,
    ingest_last: Option<Instant>,
    ingest_flash: Option<Instant>,
    wrapped_data: Option<WrappedStats>,
    wrapped_year: Option<i32>,
    wrapped_last: Option<Instant>,
    wrapped_ingest_at: Option<DateTime<Utc>>,
    modal_messages: Vec<SessionMessage>,
    modal_message_total: usize,
    modal_unattributed: Option<SessionMessage>,
    modal_turns_by_message: HashMap<MessageGroupKey, Vec<SessionTurn>>,
    modal_turn_total: usize,
    modal_daily_totals: HashMap<NaiveDate, AggregateTotals>,
    modal_turn_totals: Option<AggregateTotals>,
    modal_model_mix: Vec<ModelUsageRow>,
    modal_tool_counts: Vec<ToolCountRow>,
    modal_ingest_at: Option<DateTime<Utc>>,
    modal_key: Option<String>,
    modal_last: Option<Instant>,
}

impl UiDataCache {
    fn new() -> Self {
        Self {
            hero: HeroStats::default(),
            hero_last: None,
            sessions_rows: Vec::new(),
            sessions_total: 0,
            sessions_offset: 0,
            sessions_limit: 0,
            sessions_last: None,
            sessions_key: None,
            stats_data: None,
            stats_last: None,
            stats_key: None,
            pricing_rows: Vec::new(),
            pricing_missing: Vec::new(),
            pricing_meta: None,
            pricing_last: None,
            missing_last: None,
            last_ingest: None,
            ingest_last: None,
            ingest_flash: None,
            wrapped_data: None,
            wrapped_year: None,
            wrapped_last: None,
            wrapped_ingest_at: None,
            modal_messages: Vec::new(),
            modal_message_total: 0,
            modal_unattributed: None,
            modal_turns_by_message: HashMap::new(),
            modal_turn_total: 0,
            modal_daily_totals: HashMap::new(),
            modal_turn_totals: None,
            modal_model_mix: Vec::new(),
            modal_tool_counts: Vec::new(),
            modal_ingest_at: None,
            modal_key: None,
            modal_last: None,
        }
    }

    fn should_refresh(last: Option<Instant>, interval: Duration, now: Instant) -> bool {
        last.map(|at| now.duration_since(at) >= interval)
            .unwrap_or(true)
    }

    fn invalidate_for_view(&mut self, view: ViewMode) {
        match view {
            ViewMode::Overview => {
                self.hero_last = None;
                self.wrapped_last = None;
            }
            ViewMode::Sessions => {
                self.sessions_last = None;
            }
            ViewMode::Stats => {
                self.stats_last = None;
            }
            ViewMode::Pricing => {
                self.pricing_last = None;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn refresh_overview(
        &mut self,
        now: Instant,
        today: NaiveDate,
        runtime: &Handle,
        storage: &Storage,
        alerts: &AlertSettings,
    ) {
        if Self::should_refresh(self.hero_last, SUMMARY_REFRESH_INTERVAL, now) {
            match runtime.block_on(HeroStats::gather(storage, today, alerts)) {
                Ok(stats) => self.hero = stats,
                Err(err) => tracing::warn!(error = %err, "failed to gather hero stats"),
            }
            self.hero_last = Some(now);
        }
    }

    fn refresh_sessions(
        &mut self,
        now: Instant,
        now_local: DateTime<Local>,
        runtime: &Handle,
        storage: &Storage,
        view: &mut SessionsViewState,
        max_limit: usize,
    ) {
        let key = (view.nav.range, view.nav.anchor, view.sort);
        let key_changed = self.sessions_key != Some(key);
        if key_changed {
            self.sessions_key = Some(key);
            self.sessions_last = None;
            view.reset();
        }

        let refresh_due = Self::should_refresh(self.sessions_last, RECENT_REFRESH_INTERVAL, now);
        let period = period_for_range(view.nav.range, view.nav.anchor, now_local);
        if refresh_due || self.sessions_total == 0 || key_changed {
            match runtime.block_on(storage.sessions_count_between(period.start, period.end)) {
                Ok(total) => self.sessions_total = total,
                Err(err) => tracing::warn!(error = %err, "failed to count sessions"),
            }
        }

        let (offset, limit) = sessions_window_for(view, self.sessions_total, max_limit);
        let window_changed = offset != self.sessions_offset || limit != self.sessions_limit;

        if refresh_due || window_changed {
            if self.sessions_total == 0 || limit == 0 {
                self.sessions_rows.clear();
                self.sessions_offset = 0;
                self.sessions_limit = 0;
                self.sessions_last = Some(now);
                return;
            }

            let result = match view.sort {
                SessionSort::Recent => runtime.block_on(storage.sessions_page_by_recent_between(
                    period.start,
                    period.end,
                    offset,
                    limit,
                )),
                SessionSort::Cost => runtime.block_on(storage.sessions_page_by_cost_between(
                    period.start,
                    period.end,
                    offset,
                    limit,
                )),
            };
            match result {
                Ok(rows) => {
                    self.sessions_rows = rows;
                    self.sessions_offset = offset;
                    self.sessions_limit = limit;
                }
                Err(err) => tracing::warn!(error = %err, "failed to load sessions page"),
            }
            self.sessions_last = Some(now);
        }
    }

    fn refresh_stats(
        &mut self,
        now: Instant,
        now_local: DateTime<Local>,
        runtime: &Handle,
        storage: &Storage,
        nav: &TimeNavState,
        alerts: &AlertSettings,
    ) {
        let key = (nav.range, nav.anchor);
        let key_changed = self.stats_key != Some(key);
        if key_changed {
            self.stats_key = Some(key);
            self.stats_last = None;
        }
        if Self::should_refresh(self.stats_last, STATS_REFRESH_INTERVAL, now) {
            let period = period_for_range(nav.range, nav.anchor, now_local);
            match runtime.block_on(StatsRangeData::gather(storage, &period, nav.range, alerts)) {
                Ok(breakdown) => self.stats_data = Some(breakdown),
                Err(err) => tracing::warn!(error = %err, "failed to gather stats data"),
            }
            self.stats_last = Some(now);
        }
    }

    fn refresh_wrapped(
        &mut self,
        now: Instant,
        now_local: DateTime<Local>,
        runtime: &Handle,
        storage: &Storage,
        view: &WrappedViewState,
    ) {
        let year = view.year;
        let year_changed = self.wrapped_year != Some(year);
        let ingest_changed =
            self.last_ingest.is_some() && self.wrapped_ingest_at != self.last_ingest;
        let refresh_due = year_changed
            || Self::should_refresh(self.wrapped_last, WRAPPED_REFRESH_INTERVAL, now)
            || ingest_changed;

        if refresh_due {
            match runtime.block_on(WrappedStats::gather(storage, year, now_local)) {
                Ok(stats) => self.wrapped_data = Some(stats),
                Err(err) => tracing::warn!(error = %err, "failed to gather wrapped stats"),
            }
            self.wrapped_year = Some(year);
            self.wrapped_last = Some(now);
            self.wrapped_ingest_at = self.last_ingest;
        }
    }

    fn refresh_pricing(&mut self, now: Instant, runtime: &Handle, storage: &Storage) {
        if Self::should_refresh(self.pricing_last, PRICING_REFRESH_INTERVAL, now) {
            match runtime.block_on(storage.list_prices()) {
                Ok(rows) => self.pricing_rows = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load price list"),
            }
            match runtime.block_on(storage.missing_price_details(10)) {
                Ok(rows) => self.pricing_missing = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load missing price details"),
            }
            match runtime.block_on(storage.pricing_meta()) {
                Ok(meta) => self.pricing_meta = meta,
                Err(err) => tracing::warn!(error = %err, "failed to load pricing metadata"),
            }
            self.pricing_last = Some(now);
            self.missing_last = Some(now);
        }
    }

    fn refresh_missing_prices(&mut self, now: Instant, runtime: &Handle, storage: &Storage) {
        if Self::should_refresh(self.missing_last, PRICING_REFRESH_INTERVAL, now) {
            match runtime.block_on(storage.missing_price_details(10)) {
                Ok(rows) => self.pricing_missing = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load missing price details"),
            }
            self.missing_last = Some(now);
        }
    }

    fn refresh_ingest(&mut self, now: Instant, runtime: &Handle, storage: &Storage) {
        if Self::should_refresh(self.ingest_last, SUMMARY_REFRESH_INTERVAL, now) {
            let previous = self.last_ingest;
            match runtime.block_on(storage.last_ingest_timestamp()) {
                Ok(value) => {
                    if value.is_some() && value != previous {
                        self.ingest_flash = Some(now);
                    }
                    self.last_ingest = value;
                }
                Err(err) => tracing::warn!(error = %err, "failed to load last ingest timestamp"),
            }
            self.ingest_last = Some(now);
        }
    }

    fn refresh_modal_messages(
        &mut self,
        now: Instant,
        runtime: &Handle,
        storage: &Storage,
        selected: Option<&SessionAggregate>,
        expanded: Option<MessageGroupKey>,
        last_ingest: Option<DateTime<Utc>>,
    ) {
        let Some(selected) = selected else {
            self.modal_messages.clear();
            self.modal_message_total = 0;
            self.modal_unattributed = None;
            self.modal_turns_by_message.clear();
            self.modal_turn_total = 0;
            self.modal_daily_totals.clear();
            self.modal_turn_totals = None;
            self.modal_model_mix.clear();
            self.modal_tool_counts.clear();
            self.modal_ingest_at = last_ingest;
            self.modal_key = None;
            self.modal_last = None;
            return;
        };

        let key = session_key(selected);
        if self.modal_key.as_deref() != Some(key.as_str()) {
            self.modal_key = Some(key);
            self.modal_last = None;
            self.modal_messages.clear();
            self.modal_message_total = 0;
            self.modal_unattributed = None;
            self.modal_turns_by_message.clear();
            self.modal_turn_total = 0;
            self.modal_daily_totals.clear();
            self.modal_turn_totals = None;
            self.modal_ingest_at = last_ingest;
        }

        let ingest_changed = last_ingest.is_some() && self.modal_ingest_at != last_ingest;
        let refresh_due = self.modal_last.is_none()
            || (ingest_changed
                && Self::should_refresh(self.modal_last, MODAL_MESSAGES_REFRESH_INTERVAL, now));
        if refresh_due {
            match runtime.block_on(
                storage.session_messages(selected.session_id.as_str(), MESSAGE_VIEW_LIMIT),
            ) {
                Ok(messages) => self.modal_messages = messages,
                Err(err) => tracing::warn!(error = %err, "failed to load session messages"),
            }
            match runtime.block_on(storage.session_messages_count(selected.session_id.as_str())) {
                Ok(total) => self.modal_message_total = total,
                Err(err) => tracing::warn!(error = %err, "failed to count session messages"),
            }
            match runtime
                .block_on(storage.session_unattributed_message(selected.session_id.as_str()))
            {
                Ok(summary) => self.modal_unattributed = summary,
                Err(err) => tracing::warn!(error = %err, "failed to load unattributed summary"),
            }
            match runtime.block_on(storage.session_turns_count(selected.session_id.as_str())) {
                Ok(total) => self.modal_turn_total = total,
                Err(err) => tracing::warn!(error = %err, "failed to count session turns"),
            }
            match runtime.block_on(storage.session_turn_daily_totals(selected.session_id.as_str()))
            {
                Ok(rows) => {
                    self.modal_daily_totals =
                        rows.into_iter().map(|row| (row.date, row.totals)).collect();
                }
                Err(err) => tracing::warn!(error = %err, "failed to load session daily totals"),
            }
            match runtime.block_on(storage.session_turn_totals(selected.session_id.as_str())) {
                Ok(totals) => self.modal_turn_totals = Some(totals),
                Err(err) => tracing::warn!(error = %err, "failed to load session turn totals"),
            }
            match runtime.block_on(storage.session_model_mix(selected.session_id.as_str())) {
                Ok(rows) => self.modal_model_mix = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load session model mix"),
            }
            match runtime.block_on(storage.session_tool_counts(selected.session_id.as_str())) {
                Ok(rows) => self.modal_tool_counts = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load session tool counts"),
            }
            if ingest_changed && let Some(key) = expanded {
                self.modal_turns_by_message.remove(&key);
            }
            self.modal_last = Some(now);
            self.modal_ingest_at = last_ingest;
        }

        if let Some(key) = expanded {
            if matches!(key, MessageGroupKey::Unattributed) && self.modal_unattributed.is_none() {
                self.modal_turns_by_message.remove(&key);
            } else if let std::collections::hash_map::Entry::Vacant(entry) =
                self.modal_turns_by_message.entry(key)
            {
                let message_id = match key {
                    MessageGroupKey::Message(id) => Some(id),
                    MessageGroupKey::Unattributed => None,
                };
                match runtime.block_on(storage.session_turns_for_message(
                    selected.session_id.as_str(),
                    message_id,
                    TURN_VIEW_LIMIT,
                )) {
                    Ok(turns) => {
                        entry.insert(turns);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to load message turns");
                    }
                }
            }
        } else if !self.modal_turns_by_message.is_empty() {
            self.modal_turns_by_message.clear();
        }
    }
}

pub async fn run(config: Arc<AppConfig>, storage: Storage) -> Result<()> {
    let refresh_hz = config.display.refresh_hz.max(1);
    let tick_rate = Duration::from_millis(1000 / refresh_hz);
    let runtime = Handle::current();

    tokio::task::spawn_blocking(move || run_blocking(runtime, config, storage, tick_rate)).await?
}

fn run_blocking(
    runtime: Handle,
    config: Arc<AppConfig>,
    storage: Storage,
    tick_rate: Duration,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let mut sessions_view = SessionsViewState::new();
    let mut stats_view = StatsViewState::new();
    let mut pricing_view = PricingViewState::new();
    let mut wrapped_view = WrappedViewState::new(Local::now().date_naive());
    let mut session_modal = SessionModalState::new();
    let mut missing_modal = MissingPriceModalState::new();
    let mut help_modal = HelpModalState::new();
    let mut view_mode = ViewMode::Overview;
    let mut previous_view_mode = view_mode;
    let mut cache = UiDataCache::new();
    let alerts = AlertSettings::from_config(&config.alerts);

    let loop_result: Result<()> = (|| -> Result<()> {
        loop {
            let now_local = Local::now();
            let today = now_local.date_naive();
            let mut should_quit = false;

            if event::poll(tick_rate)?
                && let Event::Key(key) = event::read()?
            {
                should_quit = handle_key_event(
                    key,
                    &mut view_mode,
                    &mut sessions_view,
                    &mut stats_view,
                    &mut pricing_view,
                    &mut wrapped_view,
                    &mut session_modal,
                    &mut missing_modal,
                    &mut help_modal,
                    &cache.sessions_rows,
                    cache.sessions_total,
                    cache.sessions_offset,
                    &cache.modal_messages,
                    cache.modal_unattributed.as_ref(),
                    &cache.modal_turns_by_message,
                    &cache.modal_daily_totals,
                    cache.modal_turn_totals.as_ref(),
                    cache.modal_turn_total,
                    &cache.modal_model_mix,
                    &cache.modal_tool_counts,
                    &cache.pricing_rows,
                    &cache.pricing_missing,
                    &runtime,
                    &storage,
                    today,
                    &config.pricing,
                );
            }

            while !should_quit && event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    should_quit = handle_key_event(
                        key,
                        &mut view_mode,
                        &mut sessions_view,
                        &mut stats_view,
                        &mut pricing_view,
                        &mut wrapped_view,
                        &mut session_modal,
                        &mut missing_modal,
                        &mut help_modal,
                        &cache.sessions_rows,
                        cache.sessions_total,
                        cache.sessions_offset,
                        &cache.modal_messages,
                        cache.modal_unattributed.as_ref(),
                        &cache.modal_turns_by_message,
                        &cache.modal_daily_totals,
                        cache.modal_turn_totals.as_ref(),
                        cache.modal_turn_total,
                        &cache.modal_model_mix,
                        &cache.modal_tool_counts,
                        &cache.pricing_rows,
                        &cache.pricing_missing,
                        &runtime,
                        &storage,
                        today,
                        &config.pricing,
                    );
                }
            }

            if should_quit {
                break Ok(());
            }

            if view_mode != previous_view_mode {
                cache.invalidate_for_view(view_mode);
                previous_view_mode = view_mode;
            }

            let now = Instant::now();
            let session_limit = config.display.recent_events_capacity.max(50);
            cache.refresh_ingest(now, &runtime, &storage);
            cache.refresh_missing_prices(now, &runtime, &storage);

            match view_mode {
                ViewMode::Overview => {
                    cache.refresh_overview(now, today, &runtime, &storage, &alerts);
                    cache.refresh_wrapped(now, now_local, &runtime, &storage, &wrapped_view);
                }
                ViewMode::Sessions => {
                    cache.refresh_sessions(
                        now,
                        now_local,
                        &runtime,
                        &storage,
                        &mut sessions_view,
                        session_limit,
                    );
                    sessions_view.sync_with(cache.sessions_total);
                }
                ViewMode::Stats => {
                    cache.refresh_stats(
                        now,
                        now_local,
                        &runtime,
                        &storage,
                        &stats_view.nav,
                        &alerts,
                    );
                }
                ViewMode::Pricing => {
                    cache.refresh_pricing(now, &runtime, &storage);
                    pricing_view.sync(cache.pricing_rows.len());
                }
            }

            let selected_session = match view_mode {
                ViewMode::Sessions => {
                    sessions_view.selected(&cache.sessions_rows, cache.sessions_offset)
                }
                _ => None,
            }
            .cloned();

            if session_modal.is_open() && selected_session.is_none() {
                session_modal.close();
            }

            if session_modal.is_open() {
                cache.refresh_modal_messages(
                    now,
                    &runtime,
                    &storage,
                    selected_session.as_ref(),
                    session_modal.expanded_key(),
                    cache.last_ingest,
                );
            } else {
                cache.refresh_modal_messages(
                    now,
                    &runtime,
                    &storage,
                    None,
                    None,
                    cache.last_ingest,
                );
            }

            let stats_data = if matches!(view_mode, ViewMode::Stats) {
                cache.stats_data.as_ref()
            } else {
                None
            };
            let wrapped_data = if matches!(view_mode, ViewMode::Overview) {
                cache.wrapped_data.as_ref()
            } else {
                None
            };
            let (pricing_rows, pricing_missing, pricing_meta) =
                if matches!(view_mode, ViewMode::Pricing) {
                    (
                        Some(cache.pricing_rows.as_slice()),
                        Some(cache.pricing_missing.as_slice()),
                        cache.pricing_meta.as_ref(),
                    )
                } else {
                    (None, None, None)
                };
            terminal.draw(|frame| {
                draw_ui(
                    frame,
                    &cache.hero,
                    &cache.sessions_rows,
                    cache.sessions_total,
                    cache.sessions_offset,
                    &mut sessions_view,
                    &stats_view,
                    &mut pricing_view,
                    &wrapped_view,
                    &missing_modal,
                    &help_modal,
                    selected_session.as_ref(),
                    &cache.modal_messages,
                    cache.modal_message_total,
                    cache.modal_unattributed.as_ref(),
                    &cache.modal_turns_by_message,
                    cache.modal_turn_total,
                    &cache.modal_model_mix,
                    &cache.modal_tool_counts,
                    &mut session_modal,
                    stats_data,
                    wrapped_data,
                    pricing_rows,
                    pricing_missing,
                    pricing_meta,
                    view_mode,
                    &cache,
                );
            })?;
        }
    })();

    let restore_result = restore_terminal(terminal);

    match (loop_result, restore_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(loop_err), Ok(())) => Err(loop_err),
        (Ok(()), Err(restore_err)) => Err(restore_err),
        (Err(loop_err), Err(restore_err)) => Err(loop_err.context(restore_err.to_string())),
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;
    Ok(terminal)
}

fn restore_terminal(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    terminal.show_cursor()?;
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn draw_ui(
    frame: &mut Frame,
    hero: &HeroStats,
    sessions_rows: &[SessionAggregate],
    sessions_total: usize,
    sessions_offset: usize,
    sessions_view: &mut SessionsViewState,
    stats_view: &StatsViewState,
    pricing_view: &mut PricingViewState,
    wrapped_view: &WrappedViewState,
    missing_modal: &MissingPriceModalState,
    help_modal: &HelpModalState,
    selected: Option<&SessionAggregate>,
    messages: &[SessionMessage],
    message_total: usize,
    unattributed: Option<&SessionMessage>,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    turn_total: usize,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
    session_modal: &mut SessionModalState,
    stats_data: Option<&StatsRangeData>,
    wrapped_data: Option<&WrappedStats>,
    pricing_rows: Option<&[PriceRow]>,
    pricing_missing: Option<&[MissingPriceDetail]>,
    pricing_meta: Option<&PricingMeta>,
    view_mode: ViewMode,
    cache: &UiDataCache,
) {
    let dim_background = session_modal.is_open() || missing_modal.is_open() || help_modal.is_open();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(frame.size());
    render_navbar(frame, layout[0], view_mode, dim_background);

    match view_mode {
        ViewMode::Overview => draw_overview(
            frame,
            layout[1],
            hero,
            wrapped_data,
            wrapped_view,
            dim_background,
        ),
        ViewMode::Sessions => draw_sessions_view(
            frame,
            layout[1],
            sessions_rows,
            sessions_total,
            sessions_offset,
            sessions_view,
            dim_background,
        ),
        ViewMode::Stats => {
            draw_stats_view(frame, layout[1], stats_data, stats_view, dim_background)
        }
        ViewMode::Pricing => draw_pricing_view(
            frame,
            layout[1],
            pricing_rows.unwrap_or(&[]),
            pricing_missing.unwrap_or(&[]),
            pricing_meta,
            pricing_view,
            dim_background,
        ),
    }

    render_status_bar(
        frame,
        layout[2],
        cache.last_ingest,
        cache.ingest_flash,
        cache.pricing_missing.len(),
        help_modal.is_open(),
        dim_background,
    );

    if session_modal.is_open() {
        render_session_modal(
            frame,
            selected,
            messages,
            message_total,
            unattributed,
            turns_by_message,
            turn_total,
            &cache.modal_daily_totals,
            cache.modal_turn_totals.as_ref(),
            model_mix,
            tool_counts,
            session_modal,
        );
    }

    if missing_modal.is_open() {
        render_missing_prices_modal(frame, missing_modal, &cache.pricing_missing);
    }

    if help_modal.is_open() {
        render_help_modal(frame, view_mode);
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_overview(
    frame: &mut Frame,
    area: Rect,
    stats: &HeroStats,
    wrapped_data: Option<&WrappedStats>,
    wrapped_view: &WrappedViewState,
    dim: bool,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(6), Constraint::Min(0)])
        .split(area);

    render_hero_cards(frame, layout[0], stats, dim);

    let theme = ui_theme(dim);
    let wrapped_area = layout[1];
    match wrapped_data {
        Some(stats) if stats.has_activity() => {
            let wrapped_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(9),
                    Constraint::Min(0),
                ])
                .split(wrapped_area);
            render_wrapped_hero(frame, wrapped_layout[0], stats, &theme);
            render_wrapped_heatmap(frame, wrapped_layout[1], stats, &theme);
            render_wrapped_bottom(frame, wrapped_layout[2], stats, &theme);
        }
        Some(_) => {
            render_overview_placeholder(
                frame,
                wrapped_area,
                wrapped_view.year,
                format!("No Codex activity found for {}.", wrapped_view.year),
                &theme,
            );
        }
        None => {
            render_overview_placeholder(
                frame,
                wrapped_area,
                wrapped_view.year,
                "Loading annual stats…".to_string(),
                &theme,
            );
        }
    }
}

fn draw_sessions_view(
    frame: &mut Frame,
    area: Rect,
    sessions: &[SessionAggregate],
    total: usize,
    offset: usize,
    view: &mut SessionsViewState,
    dim: bool,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);
    let top_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(layout[0]);
    render_time_nav(frame, top_layout[0], &view.nav, dim);
    render_sort_nav(frame, top_layout[1], view.sort, dim);
    render_sessions_table(frame, layout[1], sessions, total, offset, view, dim);
}

fn draw_stats_view(
    frame: &mut Frame,
    area: Rect,
    stats: Option<&StatsRangeData>,
    view: &StatsViewState,
    dim: bool,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);
    render_time_nav(frame, layout[0], &view.nav, dim);
    render_stats_table(frame, layout[1], stats, dim);
}

fn render_wrapped_hero(frame: &mut Frame, area: Rect, stats: &WrappedStats, theme: &UiTheme) {
    let most_active = stats
        .most_active_day
        .map(|(date, tokens)| format!("{} ({})", date.format("%b %d"), format_tokens(tokens)))
        .unwrap_or_else(|| "—".to_string());
    let streak_value = if stats.current_streak > 0 {
        format!("{}d ({}d now)", stats.max_streak, stats.current_streak)
    } else {
        format!("{}d", stats.max_streak)
    };
    let started_value = format!(
        "{} ({}d ago)",
        stats.first_session_date.format("%b %d"),
        stats.days_since_first_session
    );

    let block = gray_block("Overview", theme);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner);
    let year_line = Paragraph::new(year_control_line(stats.year, theme));
    frame.render_widget(year_line, layout[0]);
    let spacer = Paragraph::new(Line::from(""));
    frame.render_widget(spacer, layout[1]);
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(48),
            Constraint::Length(2),
            Constraint::Percentage(48),
        ])
        .split(layout[2]);

    let usage_items = vec![
        ("Sessions".to_string(), format_tokens(stats.total_sessions)),
        ("Messages".to_string(), format_tokens(stats.total_messages)),
        (
            "Tokens".to_string(),
            format_tokens(stats.totals.total_tokens),
        ),
        ("Cost".to_string(), format_cost_short(stats.totals.cost_usd)),
    ];
    let context_items = vec![
        ("Projects".to_string(), format_tokens(stats.total_projects)),
        ("Streak".to_string(), streak_value),
        ("Most Active".to_string(), most_active),
        ("Started".to_string(), started_value),
    ];

    let usage_lines = overview_cluster_lines("Usage", &usage_items, theme);
    let context_lines = overview_cluster_lines("Context", &context_items, theme);

    let usage = Paragraph::new(usage_lines).style(Style::default().fg(theme.text_fg));
    let context = Paragraph::new(context_lines).style(Style::default().fg(theme.text_fg));
    frame.render_widget(usage, columns[0]);
    frame.render_widget(context, columns[2]);
}

fn overview_cluster_lines(
    title: &str,
    items: &[(String, String)],
    theme: &UiTheme,
) -> Vec<Line<'static>> {
    let mut lines = Vec::with_capacity(items.len().saturating_add(2));
    let header_style = Style::default()
        .fg(theme.header_fg)
        .add_modifier(Modifier::BOLD);
    lines.push(Line::from(vec![
        Span::raw(" "),
        Span::styled(title.to_string(), header_style),
    ]));
    lines.push(Line::from(""));

    let label_width = items
        .iter()
        .map(|(label, _)| label.chars().count())
        .max()
        .unwrap_or(0);
    for (label, value) in items {
        let label_pad = pad_right(label, label_width);
        lines.push(Line::from(vec![
            Span::raw(" "),
            Span::styled(label_pad, Style::default().fg(theme.label_fg)),
            Span::raw("  "),
            Span::styled(
                value.to_string(),
                Style::default()
                    .fg(theme.text_fg)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
    }
    lines
}

fn render_wrapped_heatmap(frame: &mut Frame, area: Rect, stats: &WrappedStats, theme: &UiTheme) {
    let weeks = heatmap_weeks_for_year(stats.year);
    let max_tokens = stats.daily_tokens.values().copied().max().unwrap_or(0);
    if weeks.is_empty() {
        let paragraph = Paragraph::new("No activity yet.")
            .block(gray_block("Token Activity", theme))
            .style(Style::default().fg(theme.text_fg));
        frame.render_widget(paragraph, area);
        return;
    }

    let weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    let mut lines = Vec::with_capacity(9);
    lines.push(heatmap_month_line(&weeks, theme, stats.year));
    let start_date = NaiveDate::from_ymd_opt(stats.year, 1, 1).unwrap_or(stats.end_date);
    let last_day = NaiveDate::from_ymd_opt(stats.year, 12, 31).unwrap_or(stats.end_date);
    let is_current_year = stats.end_date < last_day;
    for (row_idx, label) in weekday_labels.iter().enumerate() {
        let mut spans = vec![Span::styled(
            format!("{label} "),
            Style::default().fg(theme.label_fg),
        )];
        for (week_idx, week) in weeks.iter().enumerate() {
            if week_idx > 0 {
                spans.push(Span::raw(" "));
            }
            let date = week[row_idx];
            let count = date
                .and_then(|d| stats.daily_tokens.get(&d).copied())
                .unwrap_or(0);
            let intensity = heatmap_intensity(count, max_tokens);
            let color = if let Some(day) = date {
                if day < start_date {
                    HEATMAP_COLORS[0]
                } else if day > stats.end_date && is_current_year {
                    FUTURE_HEATMAP_COLOR
                } else {
                    let palette = if stats.max_streak_days.contains(&day) {
                        STREAK_COLORS
                    } else {
                        HEATMAP_COLORS
                    };
                    palette[intensity]
                }
            } else {
                HEATMAP_COLORS[0]
            };
            spans.push(Span::styled("  ".to_string(), Style::default().bg(color)));
        }
        lines.push(Line::from(spans));
    }

    let content_width = area.width.saturating_sub(2);
    lines.push(Line::from(""));
    lines.push(heatmap_legend_line(theme, content_width));

    let paragraph = Paragraph::new(lines)
        .block(gray_block("Token Activity (Mon–Sun)", theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn heatmap_month_line(weeks: &[[Option<NaiveDate>; 7]], theme: &UiTheme, year: i32) -> Line<'static> {
    if weeks.is_empty() {
        return Line::from("");
    }
    let width = weeks.len() * 2 + weeks.len().saturating_sub(1);
    let mut chars = vec![' '; width];
    for (week_idx, week) in weeks.iter().enumerate() {
        let month = week
            .iter()
            .flatten()
            .find(|date| date.year() == year && date.day() == 1)
            .map(|date| date.month());
        if let Some(month) = month {
            let label = month_abbrev(month);
            let start = week_idx * 3;
            for (offset, ch) in label.chars().enumerate() {
                if start + offset < chars.len() {
                    chars[start + offset] = ch;
                }
            }
        }
    }
    let month_line: String = chars.into_iter().collect();
    Line::from(vec![
        Span::styled("    ", Style::default().fg(theme.label_fg)),
        Span::styled(month_line, Style::default().fg(theme.label_fg)),
    ])
}

fn month_abbrev(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "",
    }
}

fn heatmap_legend_line(theme: &UiTheme, width: u16) -> Line<'static> {
    let mut spans = vec![Span::styled("Less ", Style::default().fg(theme.label_fg))];
    let mut left_len = "Less ".chars().count();
    for (idx, color) in HEATMAP_COLORS.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::raw(" "));
            left_len += 1;
        }
        spans.push(Span::styled("  ".to_string(), Style::default().bg(*color)));
        left_len += 2;
    }
    spans.push(Span::styled(" More", Style::default().fg(theme.label_fg)));
    left_len += " More".chars().count();

    let gap = 7usize; // 2 squares + 3 spaces between squares
    let total_width = width as usize;
    let right_label = "Streak ";
    let streak_squares_len = STREAK_COLORS.len() * 2 + STREAK_COLORS.len().saturating_sub(1);
    let min_right_len = right_label.chars().count() + streak_squares_len;
    if total_width <= left_len + gap + min_right_len {
        return Line::from(spans);
    }
    spans.push(Span::raw(" ".repeat(gap)));

    let mut right_tail = " max streak days".to_string();
    let available = total_width.saturating_sub(left_len + gap + min_right_len);
    if available < right_tail.chars().count() {
        right_tail = if available == 0 {
            String::new()
        } else {
            truncate_text(&right_tail, available)
        };
    }
    spans.push(Span::styled(
        right_label.to_string(),
        Style::default().fg(theme.label_fg),
    ));
    for (idx, color) in STREAK_COLORS.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::raw(" "));
        }
        spans.push(Span::styled("  ".to_string(), Style::default().bg(*color)));
    }
    spans.push(Span::styled(
        right_tail,
        Style::default().fg(theme.label_fg),
    ));
    Line::from(spans)
}

fn render_wrapped_bottom(frame: &mut Frame, area: Rect, stats: &WrappedStats, theme: &UiTheme) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_wrapped_top_models(frame, layout[0], stats, theme);
    render_wrapped_weekday(frame, layout[1], stats, theme);
}

fn render_wrapped_top_models(frame: &mut Frame, area: Rect, stats: &WrappedStats, theme: &UiTheme) {
    let mut lines = Vec::new();
    if stats.top_models.is_empty() {
        lines.push(Line::from("No model data."));
    } else {
        let total_width = area.width as usize;
        let prefix_len = 4usize; // " 1. "
        let sep = 2usize;
        let tokens_width = 7usize;
        let cost_width = 8usize;
        let pct_width = 6usize;
        let min_bar = 6usize;
        let mut model_width = 24usize;
        let fixed_tail = sep + tokens_width + sep + cost_width + sep + pct_width + sep;
        let available = total_width.saturating_sub(prefix_len + fixed_tail);
        let mut bar_width = 0usize;
        if available > 0 {
            if available > model_width + min_bar {
                bar_width = available - model_width;
            } else if available > min_bar {
                model_width = available - min_bar;
                bar_width = min_bar;
            } else {
                model_width = available;
            }
        }
        for (idx, model) in stats.top_models.iter().take(5).enumerate() {
            let pct = model.share * 100.0;
            let name = pad_right(&truncate_text(&model.model, model_width), model_width);
            let label = format!("{:>2}. {}", idx + 1, name);
            let tokens = align_right(format_tokens(model.tokens), tokens_width as u16);
            let cost = align_right(format_cost_short(Some(model.cost_usd)), cost_width as u16);
            let pct_label = align_right(format!("{pct:.1}%"), pct_width as u16);
            let bar = if bar_width > 0 {
                ratio_bar(model.share, bar_width)
            } else {
                String::new()
            };
            let line = Line::from(vec![
                Span::styled(label, Style::default().fg(theme.text_fg)),
                Span::raw("  "),
                Span::styled(tokens, Style::default().fg(theme.label_fg)),
                Span::raw("  "),
                Span::styled(cost, Style::default().fg(theme.label_fg)),
                Span::raw("  "),
                Span::styled(pct_label, Style::default().fg(theme.label_fg)),
                if bar_width > 0 {
                    Span::raw(" ")
                } else {
                    Span::raw("")
                },
                Span::styled(bar, Style::default().fg(theme.header_fg)),
            ]);
            lines.push(line);
        }
    }
    let paragraph = Paragraph::new(lines)
        .block(gray_block("Top Models (Spend)", theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_wrapped_weekday(frame: &mut Frame, area: Rect, stats: &WrappedStats, theme: &UiTheme) {
    let labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    let max = stats.weekday_tokens.iter().copied().max().unwrap_or(0);
    let label_width = 4u16;
    let content_width = area.width.saturating_sub(2);
    let preferred_value_width = 14u16;
    let min_value_width = 8u16;
    let max_value_width = content_width.saturating_sub(label_width + 1);
    let value_width = if max_value_width == 0 {
        0
    } else {
        preferred_value_width
            .min(max_value_width)
            .max(min_value_width.min(max_value_width))
    };
    let right_padding = if value_width > 0 { 1 } else { 0 };
    let bar_width = content_width
        .saturating_sub(label_width)
        .saturating_sub(value_width)
        .saturating_sub(1)
        .saturating_sub(right_padding) as usize;
    let bar_width = bar_width.max(1);
    let mut lines = Vec::new();
    for (idx, label) in labels.iter().enumerate() {
        let count = stats.weekday_tokens[idx];
        let ratio = if max > 0 {
            count as f64 / max as f64
        } else {
            0.0
        };
        let bar = ratio_bar(ratio, bar_width);
        let intensity = heatmap_intensity(count, max);
        let bar_color = HEATMAP_COLORS[intensity];
        let value_label = if value_width == 0 {
            String::new()
        } else {
            align_right(format_tokens(count), value_width)
        };
        let line = Line::from(vec![
            Span::styled(format!("{label} "), Style::default().fg(theme.label_fg)),
            Span::styled(bar, Style::default().fg(bar_color)),
            Span::raw(" "),
            Span::styled(value_label, Style::default().fg(theme.label_fg)),
            Span::raw(if right_padding > 0 { " " } else { "" }),
        ]);
        lines.push(line);
    }
    let paragraph = Paragraph::new(lines)
        .block(gray_block("Weekday Tokens (Mon–Sun)", theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_stats_table(frame: &mut Frame, area: Rect, stats: Option<&StatsRangeData>, dim: bool) {
    let theme = ui_theme(dim);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    if let Some(stats) = stats {
        render_stats_trend(frame, layout[0], stats, &theme);
        let mode = layout_mode(layout[1]);
        let label_width = if matches!(mode, LayoutMode::Wide) {
            12u16
        } else {
            10u16
        };
        let show_budget_column = stats.rows.iter().any(|row| row.budget_limit.is_some());
        let mut widths = vec![
            Constraint::Length(label_width),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ];
        if show_budget_column {
            widths.push(Constraint::Length(12));
        }

        let max_cost = stats.max_cost;
        let rows: Vec<Row> = stats
            .rows
            .iter()
            .rev()
            .map(|row| {
                let cost_label = format_cost_short(row.totals.cost_usd);
                let cost_label = align_right(cost_label, 10);
                let cost_style = cost_style(row.totals.cost_usd, max_cost);
                let is_zero_cost = matches!(row.totals.cost_usd, Some(cost) if cost <= 0.0);
                let cost_cell_style = if is_zero_cost {
                    Style::default().fg(Color::DarkGray)
                } else {
                    cost_style
                };
                let budget_cell = if row.over_budget {
                    if let (Some(limit), Some(cost)) = (row.budget_limit, row.totals.cost_usd) {
                        if let Some(bar) = budget_bar_with_percent(cost, limit, 12) {
                            Cell::from(Line::from(bar))
                        } else {
                            Cell::from("")
                        }
                    } else {
                        Cell::from("")
                    }
                } else {
                    Cell::from("")
                };
                let mut cells = vec![
                    Cell::from(row.label.clone()),
                    Cell::from(align_right(format_tokens(row.session_count), 9)),
                    Cell::from(cost_label).style(cost_cell_style),
                    Cell::from(align_right(format_tokens(row.totals.prompt_tokens), 10)),
                    Cell::from(align_right(
                        format_tokens(row.totals.cached_prompt_tokens),
                        10,
                    )),
                    Cell::from(align_right(format_tokens(row.totals.completion_tokens), 10)),
                    Cell::from(align_right(format_tokens(row.totals.reasoning_tokens), 10)),
                    Cell::from(align_right(format_tokens(row.totals.blended_total()), 10)),
                    Cell::from(align_right(format_tokens(row.totals.total_tokens), 10)),
                ];
                if show_budget_column {
                    cells.push(budget_cell);
                }
                let mut stats_row = Row::new(cells);
                if is_zero_cost {
                    stats_row = stats_row.style(Style::default().fg(Color::DarkGray));
                }
                stats_row
            })
            .collect();

        let mut header_labels = vec![
            "Period",
            "Sessions",
            "Cost",
            "Input",
            "Cached",
            "Output",
            "Reasoning",
            "Blended",
            "API",
        ];
        if show_budget_column {
            header_labels.push("Budget %");
        }
        let table = Table::new(rows, widths)
            .header(light_blue_header(header_labels, &theme))
            .block(gray_block(format!("Stats – {}", stats.label), &theme))
            .column_spacing(1)
            .style(Style::default().fg(theme.text_fg));

        frame.render_widget(table, layout[1]);
    } else {
        let paragraph = Paragraph::new("Loading stats…")
            .block(gray_block("Stats", &theme))
            .style(Style::default().fg(theme.text_fg));
        frame.render_widget(paragraph, area);
    }
}

fn render_stats_trend(frame: &mut Frame, area: Rect, stats: &StatsRangeData, theme: &UiTheme) {
    let spark = sparkline(&stats.trend_values);
    let mut parts = vec![
        format!("max: ${:.2}", stats.max_cost),
        format!("avg: ${:.2}", stats.avg_cost),
    ];
    if let Some(top) = stats.top_model.as_ref() {
        let pct = (top.share * 100.0).round() as u64;
        parts.push(format!("top: {} ({}%)", top.label, pct));
    }
    let mut line = parts.join("   ");
    if !spark.is_empty() {
        line.push_str("   ");
        line.push_str(&spark);
    }
    let paragraph = Paragraph::new(line)
        .block(gray_block("Trend", theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn draw_pricing_view(
    frame: &mut Frame,
    area: Rect,
    prices: &[PriceRow],
    missing: &[MissingPriceDetail],
    pricing_meta: Option<&PricingMeta>,
    view: &mut PricingViewState,
    dim: bool,
) {
    let theme = ui_theme(dim);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(0)])
        .split(area);

    render_missing_prices(frame, layout[0], missing, pricing_meta, &theme);

    let total = prices.len();
    let visible_rows = visible_rows_for_table(layout[1]);
    view.list.set_visible_rows(visible_rows, total);
    let (page, pages) = view.list.page_info(total);
    let header = light_blue_header(
        vec!["Model", "Prompt /1M", "Cached /1M", "Completion /1M"],
        &theme,
    );

    let rows: Vec<Row> = if prices.is_empty() {
        vec![Row::new(vec!["No prices available", "", "", ""])]
    } else {
        let start = view.list.scroll_offset;
        let end = (start + visible_rows).min(total);
        prices[start..end]
            .iter()
            .enumerate()
            .map(|(offset, price)| {
                let idx = start + offset;
                let mut row = Row::new(vec![
                    Cell::from(truncate_text(&price.model, 28)),
                    Cell::from(align_right(format_rate(price.prompt_per_1m), 14)),
                    Cell::from(align_right(
                        price
                            .cached_prompt_per_1m
                            .map(format_rate)
                            .unwrap_or_else(|| "—".to_string()),
                        14,
                    )),
                    Cell::from(align_right(format_rate(price.completion_per_1m), 14)),
                ]);
                if idx == view.list.selected_row {
                    row = row.style(
                        Style::default()
                            .fg(theme.highlight_fg)
                            .bg(theme.highlight_bg)
                            .add_modifier(Modifier::BOLD),
                    );
                }
                row
            })
            .collect()
    };

    let widths = [
        Constraint::Length(30),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(
            format!("Pricing — {total} models • page {page}/{pages} (R refresh; ↑/↓ PgUp/PgDn)"),
            &theme,
        ))
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));

    frame.render_widget(table, layout[1]);
}

fn format_pricing_meta_line(meta: &PricingMeta) -> String {
    let now = Utc::now();
    let age = now.signed_duration_since(meta.last_fetch_at);
    let age_label = if age.num_seconds() < 60 {
        format!("{}s ago", age.num_seconds().max(0))
    } else if age.num_minutes() < 60 {
        format!("{}m ago", age.num_minutes().max(0))
    } else if age.num_hours() < 48 {
        format!("{}h ago", age.num_hours().max(0))
    } else {
        let days = age.num_days().max(0);
        format!("{days}d ago")
    };
    let local = meta.last_fetch_at.with_timezone(&Local);
    let source = meta
        .source_url
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(meta.source_url.as_str());
    format!(
        "Last sync: {} ({}) • source: {}",
        local.format("%Y-%m-%d %H:%M"),
        age_label,
        truncate_text(source, 24),
    )
}

fn render_missing_prices(
    frame: &mut Frame,
    area: Rect,
    missing: &[MissingPriceDetail],
    pricing_meta: Option<&PricingMeta>,
    theme: &UiTheme,
) {
    let block = gray_block("Pricing Status (press ! for details)", theme);
    if missing.is_empty() {
        let status = pricing_meta
            .map(format_pricing_meta_line)
            .unwrap_or_else(|| "Last sync: —".to_string());
        let paragraph = Paragraph::new(format!("{}\nNo missing prices detected.", status))
            .block(block)
            .style(Style::default().fg(theme.text_fg));
        frame.render_widget(paragraph, area);
        return;
    }

    let mut lines = Vec::new();
    let status = pricing_meta
        .map(format_pricing_meta_line)
        .unwrap_or_else(|| "Last sync: —".to_string());
    lines.push(Line::from(status));
    let max_lines = area.height.saturating_sub(2) as usize;
    for entry in missing.iter().take(max_lines.max(1)) {
        let model = truncate_text(&entry.model, 24);
        let since = entry.first_seen.format("%Y-%m-%d").to_string();
        let last = entry.last_seen.format("%Y-%m-%d").to_string();
        lines.push(Line::from(format!("{model} since {since} (last {last})")));
    }
    let paragraph = Paragraph::new(lines)
        .wrap(Wrap { trim: true })
        .block(block)
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_missing_prices_modal(
    frame: &mut Frame,
    modal: &MissingPriceModalState,
    missing: &[MissingPriceDetail],
) {
    let area = centered_rect(70, 60, frame.size());
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Black))
        .title(Span::styled(
            " Missing Prices ",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut lines = Vec::new();
    if missing.is_empty() {
        lines.push(Line::from("No missing prices detected."));
    } else {
        for (idx, entry) in missing.iter().enumerate() {
            let model = truncate_text(&entry.model, 30);
            let since = entry.first_seen.format("%Y-%m-%d").to_string();
            let last = entry.last_seen.format("%Y-%m-%d").to_string();
            let line = Line::from(vec![
                Span::styled(
                    model,
                    if idx == modal.selected {
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    },
                ),
                Span::raw(format!("  since {since}  last {last}")),
            ]);
            lines.push(line);
        }
    }

    lines.push(Line::from(Span::styled(
        "Esc to close",
        Style::default().fg(Color::DarkGray),
    )));

    let paragraph = Paragraph::new(lines)
        .wrap(Wrap { trim: true })
        .style(Style::default().fg(Color::White))
        .block(Block::default().borders(Borders::NONE));
    frame.render_widget(paragraph, inner);
}

fn render_help_modal(frame: &mut Frame, view_mode: ViewMode) {
    let area = centered_rect(60, 50, frame.size());
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Black))
        .title(Span::styled(
            " Help ",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut lines = vec![
        Line::from("Navigation"),
        Line::from("  j/k or ↑/↓   move"),
        Line::from("  PgUp/PgDn    page"),
        Line::from("  1–4          switch views"),
        Line::from("  Tab          next view"),
        Line::from("  q            quit"),
        Line::from(""),
        Line::from("Actions"),
        Line::from("  Enter        open details / expand message"),
        Line::from("  y            copy (in modal)"),
        Line::from("  !            missing price details"),
        Line::from("  r            refresh pricing (pricing view)"),
        Line::from("  ?            toggle help"),
    ];

    if matches!(view_mode, ViewMode::Sessions | ViewMode::Stats) {
        lines.push(Line::from(""));
        lines.push(Line::from("Time Navigation"));
        lines.push(Line::from("  h/l or ←/→   prev/next period"));
        lines.push(Line::from("  d/w/m/y/a    day/week/month/year/all"));
    }

    if matches!(view_mode, ViewMode::Sessions) {
        lines.push(Line::from(""));
        lines.push(Line::from("Sorting"));
        lines.push(Line::from("  r            recent"));
        lines.push(Line::from("  c            cost"));
    }

    if matches!(view_mode, ViewMode::Overview) {
        lines.push(Line::from(""));
        lines.push(Line::from("Overview"));
        lines.push(Line::from("  h/l or ←/→   prev/next year"));
    }

    let paragraph = Paragraph::new(lines)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, inner);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn visible_rows_for_table(area: Rect) -> usize {
    area.height.saturating_sub(3) as usize
}

fn sessions_window_for(
    view: &SessionsViewState,
    total_rows: usize,
    max_limit: usize,
) -> (usize, usize) {
    if total_rows == 0 {
        return (0, 0);
    }

    let visible_rows = if view.list.visible_rows == 0 {
        20
    } else {
        view.list.visible_rows
    };
    let max_limit = max_limit.max(visible_rows).max(1);
    let mut limit = visible_rows.saturating_mul(3).max(visible_rows);
    if limit > max_limit {
        limit = max_limit;
    }

    let buffer = visible_rows;
    let mut offset = view.list.scroll_offset.saturating_sub(buffer);
    if offset >= total_rows {
        offset = total_rows.saturating_sub(1);
    }
    if offset + limit > total_rows {
        limit = total_rows.saturating_sub(offset);
    }
    (offset, limit)
}

struct UiTheme {
    header_fg: Color,
    border_fg: Color,
    label_fg: Color,
    text_fg: Color,
    highlight_fg: Color,
    highlight_bg: Color,
}

fn ui_theme(dim: bool) -> UiTheme {
    if dim {
        UiTheme {
            header_fg: Color::DarkGray,
            border_fg: Color::DarkGray,
            label_fg: Color::DarkGray,
            text_fg: Color::DarkGray,
            highlight_fg: Color::Gray,
            highlight_bg: Color::DarkGray,
        }
    } else {
        UiTheme {
            header_fg: Color::Cyan,
            border_fg: Color::DarkGray,
            label_fg: Color::Gray,
            text_fg: Color::Reset,
            highlight_fg: Color::White,
            highlight_bg: Color::Rgb(0, 90, 60),
        }
    }
}

#[derive(Copy, Clone)]
enum LayoutMode {
    Compact,
    Wide,
}

fn layout_mode(area: Rect) -> LayoutMode {
    if area.width < 150 {
        LayoutMode::Compact
    } else {
        LayoutMode::Wide
    }
}

fn session_list_widths(area: Rect, mode: LayoutMode) -> Vec<Constraint> {
    match mode {
        LayoutMode::Compact => {
            let spacing = 8u16;
            let total = area.width.saturating_sub(spacing) as i32;
            let fixed = [13, 9, 6, 28];
            let fixed_total: i32 = fixed.iter().sum();
            let mut title_width = total - fixed_total;
            if title_width < 20 {
                title_width = 20;
            }
            vec![
                Constraint::Length(fixed[0] as u16),
                Constraint::Length(fixed[1] as u16),
                Constraint::Length(fixed[2] as u16),
                Constraint::Length(fixed[3] as u16),
                Constraint::Length(title_width as u16),
            ]
        }
        LayoutMode::Wide => {
            let spacing = 10u16;
            let total = area.width.saturating_sub(spacing) as i32;
            let fixed = [13, 9, 6, 22, 21];
            let fixed_total: i32 = fixed.iter().sum();
            let mut title_width = total - fixed_total;
            if title_width < 20 {
                title_width = 20;
            }
            vec![
                Constraint::Length(fixed[0] as u16),
                Constraint::Length(fixed[1] as u16),
                Constraint::Length(fixed[2] as u16),
                Constraint::Length(fixed[3] as u16),
                Constraint::Length(fixed[4] as u16),
                Constraint::Length(title_width as u16),
            ]
        }
    }
}

fn format_rate(value: f64) -> String {
    format!("{:.4}", value)
}

fn loading_gradient_line(text: &str, theme: &UiTheme) -> Line<'static> {
    let palette: &[Color] = if theme.text_fg == Color::DarkGray {
        &[
            Color::DarkGray,
            Color::DarkGray,
            Color::Gray,
            Color::Gray,
            Color::DarkGray,
        ]
    } else {
        &[
            Color::DarkGray,
            Color::Gray,
            Color::White,
            Color::Gray,
            Color::DarkGray,
        ]
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let len = text.chars().count().max(1);
    let travel = len + palette.len();
    let phase = (now / 120) as usize % (travel * 2);
    let offset = if phase < travel {
        phase
    } else {
        (travel * 2).saturating_sub(phase)
    };

    let mut spans = Vec::with_capacity(len + 1);
    spans.push(Span::raw(" "));
    for (idx, ch) in text.chars().enumerate() {
        let color = palette[(idx + offset) % palette.len()];
        spans.push(Span::styled(
            ch.to_string(),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn sparkline(values: &[f64]) -> String {
    if values.is_empty() {
        return String::new();
    }
    let max = values.iter().cloned().fold(0.0_f64, f64::max).max(1.0);
    let levels = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let mut out = String::with_capacity(values.len());
    for value in values {
        let ratio = (value / max).clamp(0.0, 1.0);
        let idx = (ratio * (levels.len() - 1) as f64).round() as usize;
        out.push(levels[idx]);
    }
    out
}

fn render_navbar(frame: &mut Frame, area: Rect, view_mode: ViewMode, dim: bool) {
    let theme = ui_theme(dim);
    let tabs = [
        (ViewMode::Overview, "1 Overview"),
        (ViewMode::Sessions, "2 Sessions"),
        (ViewMode::Stats, "3 Stats"),
        (ViewMode::Pricing, "4 Pricing"),
    ];
    let mut spans = Vec::new();
    for (idx, (mode, label)) in tabs.iter().enumerate() {
        let label_upper = label.to_ascii_uppercase();
        let key_char = label_upper.chars().next().unwrap_or(' ');
        let padded = format!(" {} ", label_upper);
        let active = *mode == view_mode;
        let active_style = Style::default()
            .fg(theme.highlight_fg)
            .bg(theme.highlight_bg)
            .add_modifier(Modifier::BOLD);
        let inactive_style = Style::default().fg(theme.text_fg);
        for ch in padded.chars() {
            let mut style = if active { active_style } else { inactive_style };
            if ch == key_char {
                style = style.add_modifier(Modifier::UNDERLINED);
            }
            spans.push(Span::styled(ch.to_string(), style));
        }
        if idx < tabs.len() - 1 {
            spans.push(Span::raw(" "));
        }
    }
    let line = Line::from(spans);
    let block = Block::default()
        .title(" Codex Usage Tracker ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.border_fg));
    let paragraph = Paragraph::new(line)
        .block(block)
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_time_nav(frame: &mut Frame, area: Rect, nav: &TimeNavState, dim: bool) {
    let theme = ui_theme(dim);
    let now_local = Local::now();
    let period = period_for_range(nav.range, nav.anchor, now_local);
    let period_dim = nav.range == TimeRange::All;
    let period_style = if period_dim {
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(theme.text_fg)
    };
    let period_spans = period_label_spans(&period.label, 14, '~', &theme, period_dim);

    let mut spans = Vec::new();
    spans.push(Span::raw(" "));
    spans.push(Span::styled("◀ ", period_style));
    spans.extend(period_spans);
    spans.push(Span::styled(" ▶", period_style));
    spans.push(Span::raw("  "));

    let ranges = [
        (TimeRange::Day, "DAY", 'D'),
        (TimeRange::Week, "WEEK", 'W'),
        (TimeRange::Month, "MONTH", 'M'),
        (TimeRange::Year, "YEAR", 'Y'),
        (TimeRange::All, "ALL", 'A'),
    ];

    for (idx, (range, label, key)) in ranges.iter().enumerate() {
        let active = nav.range == *range;
        let (tab_spans, _) = range_tab_spans(label, *key, active, &theme);
        spans.extend(tab_spans);
        if idx < ranges.len() - 1 {
            spans.push(Span::raw(" "));
        }
    }

    let paragraph = Paragraph::new(Line::from(spans))
        .block(gray_block("Time Range", &theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_sort_nav(frame: &mut Frame, area: Rect, sort: SessionSort, dim: bool) {
    let theme = ui_theme(dim);
    let mut spans = vec![Span::raw(" ")];
    let (recent_spans, _) = range_tab_spans("RECENT", 'R', sort == SessionSort::Recent, &theme);
    let (cost_spans, _) = range_tab_spans("COST", 'C', sort == SessionSort::Cost, &theme);
    spans.extend(recent_spans);
    spans.push(Span::raw(" "));
    spans.extend(cost_spans);
    let paragraph = Paragraph::new(Line::from(spans))
        .block(gray_block("Sort", &theme))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn range_tab_spans(
    label: &str,
    key: char,
    active: bool,
    theme: &UiTheme,
) -> (Vec<Span<'static>>, usize) {
    let padded = format!(" {label} ");
    let mut spans = Vec::new();
    let active_style = Style::default()
        .fg(theme.highlight_fg)
        .bg(theme.highlight_bg)
        .add_modifier(Modifier::BOLD);
    let inactive_style = Style::default().fg(theme.text_fg);
    for ch in padded.chars() {
        let mut style = if active { active_style } else { inactive_style };
        if ch.to_ascii_uppercase() == key {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
        spans.push(Span::styled(ch.to_string(), style));
    }
    (spans, padded.chars().count())
}

fn render_status_bar(
    frame: &mut Frame,
    area: Rect,
    last_ingest: Option<DateTime<Utc>>,
    ingest_flash: Option<Instant>,
    missing_count: usize,
    help_open: bool,
    dim: bool,
) {
    let theme = ui_theme(dim);
    let now = Utc::now();
    let now_instant = Instant::now();
    let age_label = match last_ingest {
        Some(ts) => {
            let age = now.signed_duration_since(ts);
            let secs = age.num_seconds().max(0);
            if secs < 60 {
                format!("{secs}s ago")
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else {
                format!("{}h ago", secs / 3600)
            }
        }
        None => "—".to_string(),
    };
    let ingest_text = format!("last update: {age_label}");
    let ingest_color = ingest_flash
        .and_then(|instant| now_instant.checked_duration_since(instant))
        .map(|elapsed| {
            if elapsed < Duration::from_millis(500) {
                Color::Green
            } else if elapsed < Duration::from_millis(1100) {
                Color::LightGreen
            } else {
                Color::DarkGray
            }
        })
        .unwrap_or(Color::DarkGray);

    let missing_color = if missing_count > 0 {
        Color::Yellow
    } else {
        Color::DarkGray
    };
    let missing_text = format!("Missing prices: {}", missing_count);
    let help_text = if help_open {
        "? for help (open)"
    } else {
        "? for help"
    };
    let quit_text = "q to quit";
    let tab_text = "Tab to cycle screens";

    let mut spans = vec![
        Span::raw(" "),
        Span::styled("●", Style::default().fg(ingest_color)),
        Span::styled(" ", Style::default().fg(Color::DarkGray)),
        Span::styled(ingest_text, Style::default().fg(Color::DarkGray)),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(help_text, Style::default().fg(Color::DarkGray)),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(quit_text, Style::default().fg(Color::DarkGray)),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(tab_text, Style::default().fg(Color::DarkGray)),
    ];
    if missing_count > 0 {
        spans.push(Span::styled("  |  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            missing_text,
            Style::default().fg(missing_color),
        ));
        spans.push(Span::styled("  |  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            "! missing details",
            Style::default().fg(Color::DarkGray),
        ));
    }
    let line = Line::from(spans);

    let paragraph = Paragraph::new(line).style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_hero_cards(frame: &mut Frame, area: Rect, stats: &HeroStats, dim: bool) {
    let theme = ui_theme(dim);
    let block = gray_block("", &theme);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(60),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner);

    render_hero_card(frame, layout[0], "TODAY", &stats.today, &theme, true);
    render_vertical_divider(frame, layout[1], &theme);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(layout[2]);
    render_hero_card(frame, right[0], "THIS WEEK", &stats.week, &theme, false);
    render_hero_card(frame, right[1], "THIS MONTH", &stats.month, &theme, false);
}

fn render_hero_card(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    metric: &HeroMetric,
    theme: &UiTheme,
    primary: bool,
) {
    let mut content = area;
    if primary && area.width > 0 {
        let bar_area = Rect {
            x: area.x,
            y: area.y,
            width: 1,
            height: area.height,
        };
        let lines: Vec<Line> = (0..bar_area.height)
            .map(|_| Line::from(Span::styled("▌", Style::default().fg(theme.header_fg))))
            .collect();
        let paragraph = Paragraph::new(lines);
        frame.render_widget(paragraph, bar_area);
        let gap = 2;
        let consumed = 1 + gap;
        content.x = area.x.saturating_add(consumed);
        content.width = area.width.saturating_sub(consumed);
    } else if content.width > 0 {
        content.x = content.x.saturating_add(1);
        content.width = content.width.saturating_sub(1);
    }
    let cost_width = 7u16; // $NNN.NN
    let cost = if primary {
        format_cost_short(metric.total.cost_usd)
    } else {
        align_right(format_cost_short(metric.total.cost_usd), cost_width)
    };
    let cost_spans = if primary {
        let style = Style::default()
            .fg(Color::Black)
            .bg(theme.header_fg)
            .add_modifier(Modifier::BOLD);
        vec![
            Span::styled(" ", style),
            Span::styled(cost, style),
            Span::styled(" ", style),
        ]
    } else {
        vec![Span::styled(
            cost,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )]
    };
    let delta_width = cost_width + 1;
    let delta_span = match metric.delta {
        Some(delta) => {
            let (symbol, color) = if delta >= 0.0 {
                ("▲", Color::Red)
            } else {
                ("▼", Color::Green)
            };
            let value = format!("${:.2}", delta.abs());
            let text = align_right(format!("{symbol} {value}"), delta_width);
            Span::styled(text, Style::default().fg(color).add_modifier(Modifier::BOLD))
        }
        None => {
            let text = align_right("—".to_string(), delta_width);
            Span::styled(text, Style::default().fg(theme.label_fg))
        }
    };

    let mut metric_spans = Vec::new();
    metric_spans.extend(cost_spans);
    metric_spans.push(Span::raw("  "));
    metric_spans.push(delta_span);
    let session_label = align_right(format_count_short(metric.session_count), 3);
    let message_label = align_right(format_message_count_compact(metric.message_count), 4);
    metric_spans.push(Span::raw(" • "));
    let meta_color = if primary {
        theme.highlight_fg
    } else {
        theme.label_fg
    };
    let label_style = Style::default()
        .fg(theme.label_fg)
        .add_modifier(Modifier::DIM);
    let value_style = Style::default().fg(meta_color);
    metric_spans.push(Span::styled(session_label, value_style));
    let session_suffix = if metric.session_count == 1 {
        " session"
    } else {
        " sessions"
    };
    metric_spans.push(Span::styled(session_suffix, label_style));
    metric_spans.push(Span::raw(" • "));
    metric_spans.push(Span::styled(message_label, value_style));
    let message_suffix = if metric.message_count == 1 {
        " message"
    } else {
        " messages"
    };
    metric_spans.push(Span::styled(message_suffix, label_style));
    if let (Some(budget), Some(cost)) = (metric.budget, metric.total.cost_usd)
        && let Some(bar_spans) = budget_bar_with_percent(cost, budget, 10)
    {
        metric_spans.push(Span::raw("  "));
        metric_spans.extend(bar_spans);
    }

    let title_style = if primary {
        Style::default()
            .fg(theme.highlight_fg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
            .fg(theme.header_fg)
            .add_modifier(Modifier::BOLD)
    };
    let title_text = if primary {
        format!("{title} ")
    } else {
        format!(" {title} ")
    };
    let mut lines = vec![Line::from(Span::styled(title_text, title_style))];
    if content.height > 2 {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(metric_spans));

    let paragraph = Paragraph::new(lines)
        .style(Style::default().fg(theme.text_fg))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, content);
}

fn budget_bar_with_percent(cost: f64, budget: f64, width: usize) -> Option<Vec<Span<'static>>> {
    if budget <= 0.0 {
        return None;
    }
    if width == 0 {
        return None;
    }
    let ratio = (cost / budget).max(0.0);
    let filled = ((ratio.min(1.0)) * width as f64).round() as usize;
    let percent_value = (ratio * 100.0).round();
    let percent_text = if percent_value >= 1000.0 {
        "1k+%".to_string()
    } else {
        format!("{percent_value:.0}%")
    };
    let mut chars = vec![' '; width];
    let percent_len = percent_text.chars().count();
    if percent_len <= width {
        let start = (width - percent_len) / 2;
        for (idx, ch) in percent_text.chars().enumerate() {
            chars[start + idx] = ch;
        }
    }
    let color = if ratio >= 1.0 {
        Color::Indexed(160)
    } else if ratio >= 0.8 {
        Color::Indexed(172)
    } else {
        Color::Indexed(22)
    };
    let empty_bg = Color::DarkGray;
    let mut spans = Vec::with_capacity(width);
    for (idx, ch) in chars.into_iter().enumerate() {
        let bg = if idx < filled { color } else { empty_bg };
        let mut style = Style::default().bg(bg);
        if ch != ' ' {
            style = style.fg(Color::White).add_modifier(Modifier::BOLD);
        }
        spans.push(Span::styled(ch.to_string(), style));
    }
    Some(spans)
}

fn render_vertical_divider(frame: &mut Frame, area: Rect, theme: &UiTheme) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let lines: Vec<Line> = (0..area.height)
        .map(|_| Line::from(Span::styled("│", Style::default().fg(theme.border_fg))))
        .collect();
    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, area);
}

fn render_sessions_table(
    frame: &mut Frame,
    area: Rect,
    sessions: &[SessionAggregate],
    total_sessions: usize,
    window_offset: usize,
    view: &mut SessionsViewState,
    dim: bool,
) {
    let theme = ui_theme(dim);
    let total = total_sessions;
    let visible_rows = visible_rows_for_table(area);
    view.list.set_visible_rows(visible_rows, total);
    let (page, pages) = view.list.page_info(total);
    let title = format!(
        "Sessions – {total} total • page {page}/{pages} (↑/↓ PgUp/PgDn navigate, Enter details)"
    );
    let empty_state = if total > 0 && sessions.is_empty() {
        Some(EmptyState::Loading)
    } else {
        None
    };
    render_session_list_table(
        frame,
        area,
        sessions,
        &mut view.list,
        title,
        &theme,
        empty_state,
        total,
        window_offset,
    );
}

fn year_control_line(year: i32, theme: &UiTheme) -> Line<'static> {
    let label_style = Style::default()
        .fg(theme.highlight_fg)
        .bg(theme.highlight_bg)
        .add_modifier(Modifier::BOLD);
    let arrow_style = Style::default().fg(theme.text_fg);
    Line::from(vec![
        Span::raw(" "),
        Span::styled("◀ ", arrow_style),
        Span::styled(format!(" {year} "), label_style),
        Span::styled(" ▶", arrow_style),
    ])
}

fn render_overview_placeholder(
    frame: &mut Frame,
    area: Rect,
    year: i32,
    message: String,
    theme: &UiTheme,
) {
    let block = gray_block("Overview", theme);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(inner);
    let year_line = Paragraph::new(year_control_line(year, theme));
    frame.render_widget(year_line, layout[0]);
    let spacer = Paragraph::new(Line::from(""));
    frame.render_widget(spacer, layout[1]);

    let paragraph = Paragraph::new(message).style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, layout[2]);
}

#[allow(clippy::too_many_arguments)]
fn render_session_list_table(
    frame: &mut Frame,
    area: Rect,
    sessions: &[SessionAggregate],
    list: &mut ListState,
    title: String,
    theme: &UiTheme,
    empty_state: Option<EmptyState>,
    total_rows: usize,
    window_offset: usize,
) {
    let visible_rows = list.visible_rows;
    let mode = layout_mode(area);
    let cost_width = 9u16;
    let msg_width = 6u16;
    let header_labels = match mode {
        LayoutMode::Compact => vec!["Time", "Cost", "Msgs", "Context", "Title"],
        LayoutMode::Wide => vec!["Time", "Cost", "Msgs", "Project", "Branch", "Title"],
    };
    let header_labels_len = header_labels.len();
    let header = light_blue_header(header_labels, theme);

    let max_cost = sessions
        .iter()
        .filter_map(|row| row.cost_usd)
        .fold(0.0_f64, f64::max);

    let rows: Vec<Row> = if total_rows == 0 || sessions.is_empty() {
        match empty_state {
            Some(EmptyState::Loading) => {
                let mut cells = Vec::with_capacity(header_labels_len);
                cells.push(Cell::from("–"));
                for _ in 1..header_labels_len {
                    cells.push(Cell::from(""));
                }
                cells[1] = Cell::from(loading_gradient_line("Loading...", theme));
                vec![Row::new(cells)]
            }
            None => {
                let cells: Vec<Cell> = (0..header_labels_len).map(|_| Cell::from("-")).collect();
                vec![Row::new(cells).style(Style::default().fg(Color::DarkGray))]
            }
        }
    } else {
        let start = list.scroll_offset;
        let end = (start + visible_rows).min(total_rows);
        if start < window_offset || end > window_offset + sessions.len() {
            let mut cells = Vec::with_capacity(header_labels_len);
            cells.push(Cell::from("–"));
            for _ in 1..header_labels_len {
                cells.push(Cell::from(""));
            }
            cells[1] = Cell::from(loading_gradient_line("Loading...", theme));
            vec![Row::new(cells)]
        } else {
            let local_start = start.saturating_sub(window_offset);
            let local_end = (end.saturating_sub(window_offset)).min(sessions.len());
            let mut rows = Vec::new();
            for (offset, aggregate) in sessions[local_start..local_end].iter().enumerate() {
                let idx = window_offset + local_start + offset;
                let local_time = aggregate.last_activity.with_timezone(&Local);
                let time_label = local_time.format("%b %d %H:%M").to_string();
                let title = aggregate
                    .title
                    .as_ref()
                    .map(|value| truncate_text(value, LIST_TITLE_MAX_CHARS))
                    .unwrap_or_else(|| "—".to_string());
                let cost_style = cost_style(aggregate.cost_usd, max_cost);
                let cost_label = align_right(format_cost_short(aggregate.cost_usd), cost_width);
                let is_zero_cost = matches!(aggregate.cost_usd, Some(cost) if cost <= 0.0);
                let cost_cell_style = if is_zero_cost {
                    Style::default().fg(Color::DarkGray)
                } else {
                    cost_style
                };

                let msg_label = align_right(format_count_short(aggregate.user_messages), msg_width);
                let mut row = match mode {
                    LayoutMode::Compact => Row::new(vec![
                        Cell::from(time_label),
                        Cell::from(cost_label).style(cost_cell_style),
                        Cell::from(msg_label),
                        Cell::from(truncate_text(
                            &format_context_label(
                                aggregate.repo_url.as_deref(),
                                aggregate.repo_branch.as_deref(),
                                aggregate.cwd.as_deref(),
                                mode,
                            ),
                            28,
                        )),
                        Cell::from(title),
                    ]),
                    LayoutMode::Wide => Row::new(vec![
                        Cell::from(time_label),
                        Cell::from(cost_label).style(cost_cell_style),
                        Cell::from(msg_label),
                        Cell::from(truncate_text(
                            &format_project_label(
                                aggregate.repo_url.as_deref(),
                                aggregate.cwd.as_deref(),
                            ),
                            CWD_MAX_CHARS,
                        )),
                        Cell::from(truncate_text(
                            &format_branch_label(aggregate.repo_branch.as_deref()),
                            BRANCH_MAX_CHARS,
                        )),
                        Cell::from(title),
                    ]),
                };
                if idx == list.selected_row {
                    row = row.style(
                        Style::default()
                            .fg(theme.highlight_fg)
                            .bg(theme.highlight_bg)
                            .add_modifier(Modifier::BOLD),
                    );
                } else if is_zero_cost {
                    row = row.style(Style::default().fg(Color::DarkGray));
                }
                rows.push(row);
                if rows.len() >= visible_rows {
                    break;
                }
            }
            rows
        }
    };

    let widths = session_list_widths(area, mode);

    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(title, theme))
        .column_spacing(2)
        .style(Style::default().fg(theme.text_fg));

    frame.render_widget(table, area);
}

enum EmptyState {
    Loading,
}

fn render_session_metadata(
    frame: &mut Frame,
    area: Rect,
    rows: Vec<Row<'static>>,
    theme: &UiTheme,
    title: String,
) {
    let detail_block = gray_block(title, theme);
    let table = Table::new(rows, [Constraint::Length(18), Constraint::Min(0)])
        .block(detail_block)
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(table, area);
}

#[allow(clippy::too_many_arguments)]
fn render_session_modal(
    frame: &mut Frame,
    selected: Option<&SessionAggregate>,
    messages: &[SessionMessage],
    message_total: usize,
    unattributed: Option<&SessionMessage>,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    total_turns: usize,
    daily_totals: &HashMap<NaiveDate, AggregateTotals>,
    summary_totals: Option<&AggregateTotals>,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
    modal: &mut SessionModalState,
) {
    let Some(selected) = selected else {
        return;
    };

    let theme = ui_theme(false);
    let area = centered_rect(92, 90, frame.size());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Black));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let detail_rows = session_detail_rows(selected, &theme, model_mix, tool_counts);
    let detail_height = (detail_rows.len().saturating_add(2)) as u16;

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(detail_height), Constraint::Min(0)])
        .split(inner);

    let title = format!(
        "Session {} (Esc to close)",
        full_session_label(selected.session_id.as_str())
    );
    render_session_metadata(frame, layout[0], detail_rows, &theme, title);

    let show_summary = daily_totals.len() > 1;
    let summary = if show_summary { summary_totals } else { None };
    let cache_key = modal_row_cache_key(
        messages,
        unattributed,
        modal.expanded_key(),
        show_summary,
        turns_by_message,
    );
    if modal.row_cache().is_empty() || modal.row_cache_key() != Some(cache_key) {
        let rows = build_session_modal_row_descriptors(
            messages,
            unattributed,
            modal.expanded_key(),
            turns_by_message,
            show_summary,
        );
        modal.set_row_cache(rows, cache_key);
    }
    let visible_rows = visible_rows_for_table(layout[1]);
    modal.set_visible_rows(visible_rows, modal.row_cache().len());
    modal.ensure_selectable(true);

    render_session_messages_table(
        frame,
        layout[1],
        modal.row_cache(),
        messages,
        unattributed,
        turns_by_message,
        message_total,
        total_turns,
        modal,
        &theme,
        daily_totals,
        summary,
    );
}

fn build_session_modal_row_descriptors(
    messages: &[SessionMessage],
    unattributed: Option<&SessionMessage>,
    expanded: Option<MessageGroupKey>,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    show_summary: bool,
) -> Vec<ModalRowDescriptor> {
    let mut rows = Vec::new();
    if show_summary {
        rows.push(ModalRowDescriptor::Summary);
    }

    let mut last_date: Option<NaiveDate> = None;
    let push_message_rows = |message: &SessionMessage,
                             message_index: Option<usize>,
                             key: MessageGroupKey,
                             rows: &mut Vec<ModalRowDescriptor>,
                             last_date: &mut Option<NaiveDate>| {
        let date = message.timestamp.with_timezone(&Local).date_naive();
        if last_date.is_none() || *last_date != Some(date) {
            rows.push(ModalRowDescriptor::Day { date });
            *last_date = Some(date);
        }

        let is_expanded = expanded == Some(key);
        rows.push(ModalRowDescriptor::Message {
            key,
            message_index,
            expanded: is_expanded,
        });

        if is_expanded {
            if let Some(turns) = turns_by_message.get(&key) {
                if turns.is_empty() {
                    rows.push(ModalRowDescriptor::Placeholder { label: "No turns" });
                } else {
                    for idx in 0..turns.len() {
                        rows.push(ModalRowDescriptor::Turn {
                            key,
                            turn_index: idx,
                        });
                    }
                }
            } else if message.turn_count == 0 {
                rows.push(ModalRowDescriptor::Placeholder { label: "No turns" });
            } else {
                rows.push(ModalRowDescriptor::Placeholder {
                    label: "Loading…"
                });
            }
        }
    };

    for (idx, message) in messages.iter().enumerate() {
        push_message_rows(
            message,
            Some(idx),
            MessageGroupKey::Message(message.id),
            &mut rows,
            &mut last_date,
        );
    }
    if let Some(message) = unattributed {
        push_message_rows(
            message,
            None,
            MessageGroupKey::Unattributed,
            &mut rows,
            &mut last_date,
        );
    }

    rows
}

#[allow(clippy::too_many_arguments)]
fn render_session_messages_table(
    frame: &mut Frame,
    area: Rect,
    rows: &[ModalRowDescriptor],
    messages: &[SessionMessage],
    unattributed: Option<&SessionMessage>,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    message_total: usize,
    total_turns: usize,
    modal: &SessionModalState,
    theme: &UiTheme,
    daily_totals: &HashMap<NaiveDate, AggregateTotals>,
    summary: Option<&AggregateTotals>,
) {
    let note_max = {
        let spacing = 10u16;
        let fixed_total = MODAL_TIME_COL_WIDTH
            + MODAL_INFO_COL_WIDTH
            + MODAL_COST_COL_WIDTH
            + MODAL_TOKEN_COL_WIDTH * 5
            + MODAL_REASON_COL_WIDTH
            + MODAL_CONTEXT_COL_WIDTH;
        let total = area.width.saturating_sub(spacing);
        let available = total.saturating_sub(fixed_total) as usize;
        let available = available.saturating_sub(1);
        available.max(24)
    };
    let info_max = (MODAL_INFO_COL_WIDTH as usize).saturating_sub(1);

    let header = light_blue_header(
        vec![
            "Time",
            " Turns",
            " Message",
            "Cost",
            "Input",
            "Cached",
            "Blended",
            "Output",
            "API",
            "Reasoning",
            "Context",
        ],
        theme,
    );

    let visible_rows = visible_rows_for_table(area);
    let (page, pages) = modal.page_info(rows.len());
    let selected_style = Style::default()
        .fg(theme.highlight_fg)
        .bg(theme.highlight_bg)
        .add_modifier(Modifier::BOLD);

    let start = modal.scroll_offset().min(rows.len());
    let end = (start + visible_rows).min(rows.len());
    let rows: Vec<Row> = if rows.is_empty() {
        vec![Row::new(vec![
            "–",
            "",
            " No messages",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ])]
    } else {
        let mut rendered = Vec::new();
        for (offset, descriptor) in rows[start..end].iter().enumerate() {
            let absolute_idx = start + offset;
            let mut row = match *descriptor {
                ModalRowDescriptor::Summary => summary
                    .map(session_summary_row)
                    .unwrap_or_else(|| session_placeholder_row("No totals")),
                ModalRowDescriptor::Day { date } => {
                    let totals = daily_totals.get(&date);
                    session_day_totals_row(date, totals)
                }
                ModalRowDescriptor::Message {
                    message_index,
                    expanded,
                    ..
                } => {
                    let message = match message_index {
                        Some(idx) => messages.get(idx),
                        None => unattributed,
                    };
                    if let Some(message) = message {
                        session_message_row(message, expanded, note_max, info_max)
                    } else {
                        session_placeholder_row("Missing message")
                    }
                }
                ModalRowDescriptor::Turn { key, turn_index } => turns_by_message
                    .get(&key)
                    .and_then(|turns| turns.get(turn_index))
                    .map(|turn| session_turn_row(turn, note_max, info_max))
                    .unwrap_or_else(|| session_placeholder_row("Missing turn")),
                ModalRowDescriptor::Placeholder { label } => session_placeholder_row(label),
            };

            if absolute_idx == modal.selected_row() && is_modal_row_selectable(&rows[absolute_idx])
            {
                row = row.style(selected_style);
            }
            rendered.push(row);
            if rendered.len() >= visible_rows {
                break;
            }
        }
        rendered
    };

    let widths = [
        Constraint::Length(MODAL_TIME_COL_WIDTH),
        Constraint::Length(MODAL_INFO_COL_WIDTH),
        Constraint::Min(24),
        Constraint::Length(MODAL_COST_COL_WIDTH),
        Constraint::Length(MODAL_TOKEN_COL_WIDTH),
        Constraint::Length(MODAL_TOKEN_COL_WIDTH),
        Constraint::Length(MODAL_TOKEN_COL_WIDTH),
        Constraint::Length(MODAL_TOKEN_COL_WIDTH),
        Constraint::Length(MODAL_TOKEN_COL_WIDTH),
        Constraint::Length(MODAL_REASON_COL_WIDTH),
        Constraint::Length(MODAL_CONTEXT_COL_WIDTH),
    ];

    let loaded_messages = messages.len() + unattributed.map(|_| 1).unwrap_or(0);
    let message_label = if message_total > loaded_messages {
        format!("{loaded_messages}/{message_total} msgs")
    } else {
        format!("{loaded_messages} msgs")
    };
    let turn_label = format!("{total_turns} turns");
    let title = format!(
        "Session Messages – page {page}/{pages} • {message_label} • {turn_label} (↑/↓ PgUp/PgDn, Enter expand)"
    );
    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(title, theme))
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(table, area);
}

fn session_message_row(
    message: &SessionMessage,
    expanded: bool,
    note_max: usize,
    info_max: usize,
) -> Row<'static> {
    let local_time = message.timestamp.with_timezone(&Local);
    let arrow = if expanded { "▼" } else { "▶" };
    let time_label = format!("{arrow} {}", local_time.format("%H:%M"));
    let info_label = format!(
        " {}",
        truncate_text(&format_count_short(message.turn_count), info_max)
    );
    let snippet = format!(" {}", truncate_text(&message.snippet, note_max));
    let included = message.turn_count > 0;

    Row::new(vec![
        Cell::from(time_label),
        Cell::from(info_label),
        Cell::from(snippet),
        Cell::from(align_right(
            format_turn_cost(included, message.cost_usd),
            10,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.prompt_tokens),
            7,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.cached_prompt_tokens),
            7,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.blended_total()),
            7,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.completion_tokens),
            7,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.total_tokens),
            7,
        )),
        Cell::from(align_right(
            format_turn_tokens(included, message.reasoning_tokens),
            9,
        )),
        Cell::from(""),
    ])
}

fn session_turn_row(turn: &SessionTurn, note_max: usize, info_max: usize) -> Row<'static> {
    let local_time = turn.timestamp.with_timezone(&Local);
    let model = format!(
        " {}",
        format_model_effort(&turn.model, turn.reasoning_effort.as_deref(), info_max)
    );
    let note = turn
        .note
        .as_ref()
        .map(|value| truncate_text(value, note_max))
        .unwrap_or_else(|| "—".to_string());
    let dim_style = Style::default().fg(Color::Rgb(140, 140, 140));

    Row::new(vec![
        Cell::from(format!("  {}", local_time.format("%H:%M:%S"))).style(dim_style),
        Cell::from(model).style(dim_style),
        Cell::from(format!(" {}", note)).style(dim_style),
        Cell::from(align_right(
            format_turn_cost(turn.usage_included, turn.cost_usd),
            10,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.prompt_tokens),
            7,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.cached_prompt_tokens),
            7,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.blended_total()),
            7,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.completion_tokens),
            7,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.total_tokens),
            7,
        ))
        .style(dim_style),
        Cell::from(align_right(
            format_turn_tokens(turn.usage_included, turn.reasoning_tokens),
            9,
        ))
        .style(dim_style),
        Cell::from(Line::from(ctx_gauge_spans(
            turn.total_tokens,
            turn.context_window,
        ))),
    ])
}

fn session_placeholder_row(label: &str) -> Row<'static> {
    Row::new(vec![
        Cell::from(""),
        Cell::from(""),
        Cell::from(format!(" {label}")),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
    ])
    .style(Style::default().fg(Color::DarkGray))
}

fn session_day_totals_row(date: NaiveDate, totals: Option<&AggregateTotals>) -> Row<'static> {
    let label = format!("─ {} ──", date.format("%b %d"));
    let mut cells = vec![
        Cell::from(label),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
        Cell::from(""),
    ];
    if let Some(totals) = totals {
        cells[3] = Cell::from(align_right(format_cost(totals.cost_usd), 10));
        cells[4] = Cell::from(align_right(format_tokens(totals.prompt_tokens), 7));
        cells[5] = Cell::from(align_right(format_tokens(totals.cached_prompt_tokens), 7));
        cells[6] = Cell::from(align_right(format_tokens(totals.blended_total()), 7));
        cells[7] = Cell::from(align_right(format_tokens(totals.completion_tokens), 7));
        cells[8] = Cell::from(align_right(format_tokens(totals.total_tokens), 7));
        cells[9] = Cell::from(align_right(format_tokens(totals.reasoning_tokens), 9));
    }
    Row::new(cells).style(
        Style::default()
            .fg(Color::LightGreen)
            .add_modifier(Modifier::BOLD),
    )
}

fn session_summary_row(totals: &AggregateTotals) -> Row<'static> {
    let cells = vec![
        Cell::from("Total"),
        Cell::from(""),
        Cell::from(""),
        Cell::from(align_right(format_cost(totals.cost_usd), 10)),
        Cell::from(align_right(format_tokens(totals.prompt_tokens), 7)),
        Cell::from(align_right(format_tokens(totals.cached_prompt_tokens), 7)),
        Cell::from(align_right(format_tokens(totals.blended_total()), 7)),
        Cell::from(align_right(format_tokens(totals.completion_tokens), 7)),
        Cell::from(align_right(format_tokens(totals.total_tokens), 7)),
        Cell::from(align_right(format_tokens(totals.reasoning_tokens), 9)),
        Cell::from(""),
    ];
    Row::new(cells).style(
        Style::default()
            .fg(Color::LightGreen)
            .add_modifier(Modifier::BOLD),
    )
}

fn format_tokens(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_count_short(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_message_count_compact(value: u64) -> String {
    if value >= 10_000 {
        "10K+".to_string()
    } else if value >= 1_000 {
        "1K+".to_string()
    } else {
        value.to_string()
    }
}

fn format_cost(cost: Option<f64>) -> String {
    match cost {
        Some(value) => format!("${:.4}", value),
        None => "unknown".to_string(),
    }
}

fn format_cost_short(cost: Option<f64>) -> String {
    match cost {
        Some(value) => format!("${:.2}", value),
        None => "?".to_string(),
    }
}

fn align_right(value: String, width: u16) -> String {
    let width = width as usize;
    let len = value.chars().count();
    if len >= width {
        value
    } else {
        format!("{:>width$}", value, width = width)
    }
}

fn period_label_spans(
    value: &str,
    width: usize,
    _filler: char,
    theme: &UiTheme,
    dim: bool,
) -> Vec<Span<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let base_style = if dim {
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(theme.text_fg)
    };
    let highlight_style = if dim {
        base_style
    } else {
        Style::default()
            .fg(theme.highlight_fg)
            .bg(theme.highlight_bg)
            .add_modifier(Modifier::BOLD)
    };

    let label = if value.chars().count() > width {
        truncate_text(value, width)
    } else {
        value.to_string()
    };
    let len = label.chars().count();
    if len >= width {
        return vec![Span::styled(label, highlight_style)];
    }

    let padding = width - len;
    let left_pad = padding / 2;
    let right_pad = padding - left_pad;
    let mut spans = Vec::new();
    let highlight_span = format!("{}{}{}", " ".repeat(left_pad), label, " ".repeat(right_pad));
    spans.push(Span::styled(highlight_span, highlight_style));
    spans
}

fn format_turn_tokens(included: bool, value: u64) -> String {
    if included {
        format_tokens(value)
    } else {
        "n/a".to_string()
    }
}

fn format_turn_cost(included: bool, cost: Option<f64>) -> String {
    if included {
        format_cost(cost)
    } else {
        "n/a".to_string()
    }
}

fn format_top_model(entry: &TopModelShare) -> Option<String> {
    let model = entry.model.trim();
    if model.is_empty() {
        return None;
    }
    let effort = entry
        .reasoning_effort
        .as_deref()
        .unwrap_or("default")
        .trim();
    if effort.is_empty() {
        Some(model.to_string())
    } else {
        Some(format!("{model} / {effort}"))
    }
}

fn ratio_bar(ratio: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let ratio = ratio.clamp(0.0, 1.0);
    let filled = (ratio * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width.saturating_sub(filled);
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

fn cost_style(cost: Option<f64>, max: f64) -> Style {
    let Some(cost) = cost else {
        return Style::default().fg(Color::DarkGray);
    };
    if max <= 0.0 {
        return Style::default().fg(Color::Gray);
    }
    let ratio = (cost / max).clamp(0.0, 1.0);
    if ratio < 0.25 {
        Style::default().fg(Color::Gray)
    } else if ratio < 0.5 {
        Style::default().fg(Color::Yellow)
    } else if ratio < 0.75 {
        Style::default().fg(Color::LightRed)
    } else {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    }
}

fn ctx_gauge_spans(total_tokens: u64, context_window: Option<u64>) -> Vec<Span<'static>> {
    let Some(window) = context_window else {
        return vec![Span::styled(
            "—".to_string(),
            Style::default().fg(Color::DarkGray),
        )];
    };
    if window == 0 {
        return vec![Span::styled(
            "—".to_string(),
            Style::default().fg(Color::DarkGray),
        )];
    }

    let ratio = total_tokens as f64 / window as f64;
    let width = 6usize;
    let filled = (ratio.clamp(0.0, 1.0) * width as f64).round() as usize;
    let pct = (ratio * 100.0).clamp(0.0, 999.0).round() as i64;
    let pct_label = format!("{:>3}%", pct);
    let color = ctx_gauge_color(ratio);
    let empty_bg = Color::DarkGray;
    let fill_bg = color;
    let mut spans = Vec::with_capacity(width + 2);
    for idx in 0..width {
        let bg = if idx < filled { fill_bg } else { empty_bg };
        spans.push(Span::styled(" ".to_string(), Style::default().bg(bg)));
    }
    spans.push(Span::raw(" "));
    spans.push(Span::styled(pct_label, Style::default().fg(color)));
    spans
}

fn ctx_gauge_color(ratio: f64) -> Color {
    if ratio < 0.5 {
        Color::Green
    } else if ratio < 0.7 {
        Color::Yellow
    } else {
        Color::LightRed
    }
}

fn format_effort_short(effort: &str) -> String {
    let trimmed = effort.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "low" => "low".to_string(),
        "medium" => "med".to_string(),
        "high" => "high".to_string(),
        _ => truncate_text(trimmed, 6),
    }
}

fn format_turn_effort(effort: Option<&str>) -> String {
    effort
        .map(format_effort_short)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "—".to_string())
}

fn format_model_effort(model: &str, effort: Option<&str>, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    let effort_label = format_turn_effort(effort);
    let effort_len = effort_label.chars().count();
    if effort_len >= max_width {
        return truncate_text(&effort_label, max_width);
    }
    let sep = " / ";
    let sep_len = sep.chars().count();
    if effort_len + sep_len >= max_width {
        return truncate_text(&effort_label, max_width);
    }
    let model_budget = max_width.saturating_sub(effort_len + sep_len);
    if model_budget == 0 {
        return effort_label;
    }
    let model_trimmed = model.trim();
    if model_trimmed.is_empty() {
        return effort_label;
    }
    let model_label = truncate_text(model_trimmed, model_budget);
    format!("{model_label}{sep}{effort_label}")
}

#[allow(clippy::too_many_arguments)]
fn handle_key_event(
    key: KeyEvent,
    view_mode: &mut ViewMode,
    sessions_view: &mut SessionsViewState,
    stats_view: &mut StatsViewState,
    pricing_view: &mut PricingViewState,
    wrapped_view: &mut WrappedViewState,
    session_modal: &mut SessionModalState,
    missing_modal: &mut MissingPriceModalState,
    help_modal: &mut HelpModalState,
    sessions_rows: &[SessionAggregate],
    sessions_total: usize,
    sessions_offset: usize,
    modal_messages: &[SessionMessage],
    modal_unattributed: Option<&SessionMessage>,
    modal_turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    modal_daily_totals: &HashMap<NaiveDate, AggregateTotals>,
    modal_turn_totals: Option<&AggregateTotals>,
    session_turns_total: usize,
    modal_model_mix: &[ModelUsageRow],
    modal_tool_counts: &[ToolCountRow],
    pricing_rows: &[PriceRow],
    pricing_missing: &[MissingPriceDetail],
    runtime: &Handle,
    storage: &Storage,
    today: NaiveDate,
    pricing_config: &PricingConfig,
) -> bool {
    if key.code == KeyCode::Char('q')
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
    {
        return true;
    }

    if help_modal.is_open() {
        match key.code {
            KeyCode::Esc | KeyCode::Char('?') => help_modal.toggle(),
            _ => {}
        }
        return false;
    }

    if missing_modal.is_open() {
        handle_missing_modal_input(missing_modal, key, pricing_missing);
        return false;
    }

    if session_modal.is_open() {
        let selected_session = match *view_mode {
            ViewMode::Sessions => sessions_view.selected(sessions_rows, sessions_offset),
            _ => None,
        };
        if handle_session_modal_input(
            session_modal,
            key,
            modal_messages,
            modal_unattributed,
            modal_turns_by_message,
            modal_daily_totals,
            modal_turn_totals,
            session_turns_total,
            selected_session,
            modal_model_mix,
            modal_tool_counts,
        ) {
            return false;
        }
    }

    if *view_mode == ViewMode::Pricing && handle_pricing_keys(key, runtime, storage, pricing_config)
    {
        return false;
    }

    match key.code {
        KeyCode::Char('?') => {
            help_modal.toggle();
        }
        KeyCode::Char('!') => {
            if missing_modal.is_open() {
                missing_modal.close();
            } else {
                missing_modal.open();
            }
        }
        KeyCode::Char('1') => {
            *view_mode = ViewMode::Overview;
        }
        KeyCode::Char('2') => {
            *view_mode = ViewMode::Sessions;
        }
        KeyCode::Char('3') => {
            *view_mode = ViewMode::Stats;
        }
        KeyCode::Char('4') => {
            *view_mode = ViewMode::Pricing;
        }
        KeyCode::Tab => {
            *view_mode = view_mode.next();
        }
        KeyCode::Esc => {
            *view_mode = ViewMode::Overview;
        }
        KeyCode::Left | KeyCode::Char('h') => match *view_mode {
            ViewMode::Overview => {
                wrapped_view.prev_year();
            }
            ViewMode::Sessions => {
                sessions_view.nav.move_prev();
                sessions_view.reset();
            }
            ViewMode::Stats => {
                stats_view.nav.move_prev();
            }
            _ => {}
        },
        KeyCode::Right | KeyCode::Char('l') => match *view_mode {
            ViewMode::Overview => {
                wrapped_view.next_year();
            }
            ViewMode::Sessions => {
                sessions_view.nav.move_next();
                sessions_view.reset();
            }
            ViewMode::Stats => {
                stats_view.nav.move_next();
            }
            _ => {}
        },
        KeyCode::Up | KeyCode::Char('k') => match *view_mode {
            ViewMode::Sessions => {
                sessions_view.move_selection_up(sessions_total);
            }
            ViewMode::Pricing => {
                pricing_view.move_selection_up(pricing_rows.len());
            }
            _ => {}
        },
        KeyCode::Down | KeyCode::Char('j') => match *view_mode {
            ViewMode::Sessions => {
                sessions_view.move_selection_down(sessions_total);
            }
            ViewMode::Pricing => {
                pricing_view.move_selection_down(pricing_rows.len());
            }
            _ => {}
        },
        KeyCode::PageUp => match *view_mode {
            ViewMode::Sessions => {
                sessions_view.page_up(sessions_total);
            }
            ViewMode::Pricing => {
                pricing_view.page_up(pricing_rows.len());
            }
            _ => {}
        },
        KeyCode::PageDown => match *view_mode {
            ViewMode::Sessions => {
                sessions_view.page_down(sessions_total);
            }
            ViewMode::Pricing => {
                pricing_view.page_down(pricing_rows.len());
            }
            _ => {}
        },
        KeyCode::Enter => match *view_mode {
            ViewMode::Sessions => {
                if let Some(selected) = sessions_view.selected(sessions_rows, sessions_offset) {
                    session_modal.open_for(session_key(selected));
                }
            }
            _ => {}
        },
        KeyCode::Char(ch) => {
            if matches!(view_mode, ViewMode::Sessions | ViewMode::Stats)
                && let Some(range) = TimeRange::from_key(ch)
            {
                if matches!(view_mode, ViewMode::Sessions) {
                    sessions_view.nav.set_range(range, today);
                    sessions_view.reset();
                } else {
                    stats_view.nav.set_range(range, today);
                }
            }
            if matches!(view_mode, ViewMode::Sessions)
                && let Some(sort) = SessionSort::from_key(ch)
            {
                sessions_view.set_sort(sort);
            }
        }
        _ => {}
    }

    false
}

fn handle_missing_modal_input(
    modal: &mut MissingPriceModalState,
    key: KeyEvent,
    missing: &[MissingPriceDetail],
) {
    match key.code {
        KeyCode::Esc => {
            modal.close();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            modal.move_up(missing.len());
        }
        KeyCode::Down | KeyCode::Char('j') => {
            modal.move_down(missing.len());
        }
        KeyCode::Enter => {
            modal.close();
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_session_modal_input(
    modal: &mut SessionModalState,
    key: KeyEvent,
    messages: &[SessionMessage],
    unattributed: Option<&SessionMessage>,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
    daily_totals: &HashMap<NaiveDate, AggregateTotals>,
    summary_totals: Option<&AggregateTotals>,
    total_turns: usize,
    selected: Option<&SessionAggregate>,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> bool {
    let show_summary = daily_totals.len() > 1 && summary_totals.is_some();
    let cache_key = modal_row_cache_key(
        messages,
        unattributed,
        modal.expanded_key(),
        show_summary,
        turns_by_message,
    );
    if modal.row_cache().is_empty() || modal.row_cache_key() != Some(cache_key) {
        let rows = build_session_modal_row_descriptors(
            messages,
            unattributed,
            modal.expanded_key(),
            turns_by_message,
            show_summary,
        );
        modal.set_row_cache(rows, cache_key);
    }
    modal.ensure_selectable(true);

    match key.code {
        KeyCode::Esc => {
            modal.close();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            modal.move_selection_up();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            modal.move_selection_down();
        }
        KeyCode::PageUp => {
            modal.page_up();
        }
        KeyCode::PageDown => {
            modal.page_down();
        }
        KeyCode::Enter => {
            let current_row = modal.selected_row();
            if let Some(ModalRowDescriptor::Message { key, .. }) =
                modal.row_cache().get(current_row)
            {
                let key = *key;
                modal.toggle_expand(key);
                let updated_rows = build_session_modal_row_descriptors(
                    messages,
                    unattributed,
                    modal.expanded_key(),
                    turns_by_message,
                    show_summary,
                );
                let cache_key = modal_row_cache_key(
                    messages,
                    unattributed,
                    modal.expanded_key(),
                    show_summary,
                    turns_by_message,
                );
                modal.set_row_cache(updated_rows, cache_key);
                if let Some(idx) = find_modal_message_row(modal.row_cache(), key) {
                    modal.select_row(idx);
                }
            }
        }
        KeyCode::Char('y') | KeyCode::Char('Y')
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
        {
            if let Some(aggregate) = selected {
                let fallback_turns: u64 = messages
                    .iter()
                    .map(|message| message.turn_count)
                    .sum::<u64>()
                    + unattributed.map(|message| message.turn_count).unwrap_or(0);
                let resolved_turns = if total_turns == 0 {
                    fallback_turns as usize
                } else {
                    total_turns
                };
                if let Err(err) =
                    copy_session_details_osc52(aggregate, resolved_turns, model_mix, tool_counts)
                {
                    tracing::warn!(error = %err, "failed to copy session details");
                }
            }
        }
        _ => {}
    }
    true
}

fn handle_pricing_keys(
    key: KeyEvent,
    runtime: &Handle,
    storage: &Storage,
    pricing_config: &PricingConfig,
) -> bool {
    match key.code {
        KeyCode::Char('r') | KeyCode::Char('R') => {
            let pricing_config = pricing_config.clone();
            let storage = storage.clone();
            runtime.spawn(async move {
                if let Err(err) = pricing_remote::force_sync(&pricing_config, &storage).await {
                    tracing::warn!(error = %err, "failed to refresh pricing");
                }
            });
            true
        }
        _ => false,
    }
}

struct AlertSettings {
    daily_budget_usd: Option<f64>,
    monthly_budget_usd: Option<f64>,
}

impl AlertSettings {
    fn from_config(config: &crate::config::AlertConfig) -> Self {
        Self {
            daily_budget_usd: config.daily_budget_usd,
            monthly_budget_usd: config.monthly_budget_usd,
        }
    }
}

#[derive(Clone)]
struct Period {
    label: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

#[derive(Clone)]
struct HeroMetric {
    total: AggregateTotals,
    delta: Option<f64>,
    budget: Option<f64>,
    session_count: u64,
    message_count: u64,
}

struct HeroStats {
    today: HeroMetric,
    week: HeroMetric,
    month: HeroMetric,
}

impl Default for HeroStats {
    fn default() -> Self {
        Self {
            today: HeroMetric {
                total: AggregateTotals::default(),
                delta: None,
                budget: None,
                session_count: 0,
                message_count: 0,
            },
            week: HeroMetric {
                total: AggregateTotals::default(),
                delta: None,
                budget: None,
                session_count: 0,
                message_count: 0,
            },
            month: HeroMetric {
                total: AggregateTotals::default(),
                delta: None,
                budget: None,
                session_count: 0,
                message_count: 0,
            },
        }
    }
}

impl HeroStats {
    async fn gather(storage: &Storage, today: NaiveDate, alerts: &AlertSettings) -> Result<Self> {
        let now_local = Local::now();
        let today_period = period_for_range(TimeRange::Day, today, now_local);
        let week_period = period_for_range(TimeRange::Week, today, now_local);
        let month_period = period_for_range(TimeRange::Month, today, now_local);

        let today_prev = period_for_range(
            TimeRange::Day,
            today
                .checked_sub_signed(ChronoDuration::days(1))
                .unwrap_or(today),
            now_local,
        );
        let week_prev =
            period_for_range(TimeRange::Week, week_period_anchor_prev(today), now_local);
        let month_prev =
            period_for_range(TimeRange::Month, month_period_anchor_prev(today), now_local);

        let today_totals = storage
            .totals_between_timestamps(today_period.start, today_period.end)
            .await?;
        let week_totals = storage
            .totals_between_timestamps(week_period.start, week_period.end)
            .await?;
        let month_totals = storage
            .totals_between_timestamps(month_period.start, month_period.end)
            .await?;

        let today_counts = storage
            .counts_between_timestamps(today_period.start, today_period.end)
            .await?;
        let week_counts = storage
            .counts_between_timestamps(week_period.start, week_period.end)
            .await?;
        let month_counts = storage
            .counts_between_timestamps(month_period.start, month_period.end)
            .await?;

        let today_prev_totals = storage
            .totals_between_timestamps(today_prev.start, today_prev.end)
            .await?;
        let week_prev_totals = storage
            .totals_between_timestamps(week_prev.start, week_prev.end)
            .await?;
        let month_prev_totals = storage
            .totals_between_timestamps(month_prev.start, month_prev.end)
            .await?;

        Ok(Self {
            today: HeroMetric {
                total: today_totals.clone(),
                delta: cost_delta(today_totals.cost_usd, today_prev_totals.cost_usd),
                budget: alerts.daily_budget_usd,
                session_count: today_counts.session_count,
                message_count: today_counts.message_count,
            },
            week: HeroMetric {
                total: week_totals.clone(),
                delta: cost_delta(week_totals.cost_usd, week_prev_totals.cost_usd),
                budget: None,
                session_count: week_counts.session_count,
                message_count: week_counts.message_count,
            },
            month: HeroMetric {
                total: month_totals.clone(),
                delta: cost_delta(month_totals.cost_usd, month_prev_totals.cost_usd),
                budget: alerts.monthly_budget_usd,
                session_count: month_counts.session_count,
                message_count: month_counts.message_count,
            },
        })
    }
}

struct StatsRangeData {
    label: String,
    rows: Vec<StatRow>,
    trend_values: Vec<f64>,
    avg_cost: f64,
    max_cost: f64,
    top_model: Option<TopModelStat>,
}

impl StatsRangeData {
    async fn gather(
        storage: &Storage,
        period: &Period,
        range: TimeRange,
        alerts: &AlertSettings,
    ) -> Result<Self> {
        let (bucket_expr, buckets) = buckets_for_range(range, period);
        let rows = storage
            .aggregates_by_bucket(period.start, period.end, bucket_expr)
            .await?;
        let top_model_share = storage.top_model_share(period.start, period.end).await?;
        let mut map = HashMap::new();
        for entry in rows {
            map.insert(entry.bucket, (entry.totals, entry.session_count));
        }

        let mut result_rows = Vec::with_capacity(buckets.len());
        let mut trend = Vec::with_capacity(buckets.len());
        let mut max_cost = 0.0;
        let mut sum_cost = 0.0;
        let mut count_cost = 0u64;

        for bucket in buckets {
            let (totals, session_count) = map
                .remove(&bucket.key)
                .unwrap_or((AggregateTotals::default(), 0));
            let cost = totals.cost_usd.unwrap_or(0.0);
            if cost > 0.0 {
                sum_cost += cost;
                count_cost += 1;
            }
            if cost > max_cost {
                max_cost = cost;
            }
            trend.push(cost);
            let budget_limit = match bucket.granularity {
                BucketGranularity::Day => alerts.daily_budget_usd,
                BucketGranularity::Month => alerts.monthly_budget_usd,
                _ => None,
            };
            let over_budget = budget_limit.map(|limit| cost > limit).unwrap_or(false);
            result_rows.push(StatRow {
                label: bucket.label,
                totals,
                over_budget,
                session_count,
                budget_limit,
            });
        }

        let avg_cost = if count_cost > 0 {
            sum_cost / count_cost as f64
        } else {
            0.0
        };
        let top_model = top_model_share
            .as_ref()
            .and_then(|entry| format_top_model(entry).map(|label| (label, entry.share)))
            .map(|(label, share)| TopModelStat { label, share });

        Ok(Self {
            label: period.label.clone(),
            rows: result_rows,
            trend_values: trend,
            avg_cost,
            max_cost,
            top_model,
        })
    }
}

struct WrappedModelStat {
    model: String,
    tokens: u64,
    cost_usd: f64,
    share: f64,
}

struct WrappedStats {
    year: i32,
    end_date: NaiveDate,
    first_session_date: NaiveDate,
    days_since_first_session: i64,
    total_sessions: u64,
    total_messages: u64,
    total_projects: u64,
    totals: AggregateTotals,
    top_models: Vec<WrappedModelStat>,
    daily_tokens: HashMap<NaiveDate, u64>,
    most_active_day: Option<(NaiveDate, u64)>,
    weekday_tokens: [u64; 7],
    max_streak: u64,
    current_streak: u64,
    max_streak_days: HashSet<NaiveDate>,
}

impl WrappedStats {
    async fn gather(storage: &Storage, year: i32, now_local: DateTime<Local>) -> Result<Self> {
        let (start_date, end_date, start, end) = wrapped_year_bounds(year, now_local);
        let totals = storage.totals_between_timestamps(start, end).await?;
        let counts = storage.counts_between_timestamps(start, end).await?;
        let daily_rows = storage.token_totals_by_day(start, end).await?;
        let project_count = storage.project_count_between(start, end).await?;
        let model_totals = storage.model_usage_by_cost_between(start, end, 5).await?;
        let first_session = storage
            .first_session_timestamp()
            .await?
            .or(storage.first_turn_timestamp().await?);

        let first_session_date = first_session
            .map(|ts| ts.with_timezone(&Local).date_naive())
            .unwrap_or(start_date);
        let days_since_first_session = now_local
            .date_naive()
            .signed_duration_since(first_session_date)
            .num_days()
            .max(0);

        let mut daily_tokens = HashMap::new();
        let mut most_active_day: Option<(NaiveDate, u64)> = None;
        let mut weekday_tokens = [0u64; 7];
        for DailyTokenTotal { date, total_tokens } in daily_rows {
            if total_tokens == 0 {
                continue;
            }
            daily_tokens.insert(date, total_tokens);
            let idx = date.weekday().num_days_from_monday() as usize;
            weekday_tokens[idx] += total_tokens;
            if most_active_day
                .as_ref()
                .map(|(_, max)| total_tokens > *max)
                .unwrap_or(true)
            {
                most_active_day = Some((date, total_tokens));
            }
        }

        let (max_streak, current_streak, max_streak_days) =
            streak_stats_for_year(&daily_tokens, end_date);

        let total_cost = totals.cost_usd.unwrap_or(0.0);
        let model_total_cost: f64 = model_totals.iter().map(|row| row.cost_usd).sum();
        let denominator = if total_cost > 0.0 {
            total_cost
        } else {
            model_total_cost
        };
        let top_models = model_totals
            .into_iter()
            .map(|row| WrappedModelStat {
                model: row.model,
                tokens: row.total_tokens,
                cost_usd: row.cost_usd,
                share: if denominator > 0.0 {
                    row.cost_usd / denominator
                } else {
                    0.0
                },
            })
            .collect();

        Ok(Self {
            year,
            end_date,
            first_session_date,
            days_since_first_session,
            total_sessions: counts.session_count,
            total_messages: counts.message_count,
            total_projects: project_count,
            totals,
            top_models,
            daily_tokens,
            most_active_day,
            weekday_tokens,
            max_streak,
            current_streak,
            max_streak_days,
        })
    }

    fn has_activity(&self) -> bool {
        self.total_sessions > 0 || self.total_messages > 0 || self.totals.total_tokens > 0
    }
}

struct TopModelStat {
    label: String,
    share: f64,
}

struct StatRow {
    label: String,
    totals: AggregateTotals,
    over_budget: bool,
    session_count: u64,
    budget_limit: Option<f64>,
}

#[derive(Clone, Copy)]
enum BucketGranularity {
    Hour,
    Day,
    Month,
    Year,
}

struct BucketSpec {
    key: String,
    label: String,
    granularity: BucketGranularity,
}

fn buckets_for_range(range: TimeRange, period: &Period) -> (&'static str, Vec<BucketSpec>) {
    let start_local = period.start.with_timezone(&Local).date_naive();
    let end_local = period.end.with_timezone(&Local).date_naive();

    match range {
        TimeRange::Day => {
            let mut buckets = Vec::with_capacity(24);
            for hour in 0..24 {
                buckets.push(BucketSpec {
                    key: format!("{:02}", hour),
                    label: format!("{:02}:00", hour),
                    granularity: BucketGranularity::Hour,
                });
            }
            ("strftime('%H', timestamp, 'localtime')", buckets)
        }
        TimeRange::Week | TimeRange::Month => {
            let mut buckets = Vec::new();
            let mut cursor = start_local;
            while cursor < end_local {
                buckets.push(BucketSpec {
                    key: cursor.to_string(),
                    label: cursor.to_string(),
                    granularity: BucketGranularity::Day,
                });
                cursor = cursor
                    .checked_add_signed(ChronoDuration::days(1))
                    .unwrap_or(cursor);
                if cursor == start_local {
                    break;
                }
            }
            ("strftime('%Y-%m-%d', timestamp, 'localtime')", buckets)
        }
        TimeRange::Year => {
            let year = start_local.year();
            let mut buckets = Vec::with_capacity(12);
            for month in 1..=12 {
                buckets.push(BucketSpec {
                    key: format!("{:04}-{:02}", year, month),
                    label: format!("{:04}-{:02}", year, month),
                    granularity: BucketGranularity::Month,
                });
            }
            ("strftime('%Y-%m', timestamp, 'localtime')", buckets)
        }
        TimeRange::All => {
            let start_year = start_local.year().max(1970);
            let end_year = end_local.year().max(start_year);
            let mut buckets = Vec::new();
            for year in start_year..=end_year {
                buckets.push(BucketSpec {
                    key: format!("{year}"),
                    label: format!("{year}"),
                    granularity: BucketGranularity::Year,
                });
            }
            ("strftime('%Y', timestamp, 'localtime')", buckets)
        }
    }
}

fn cost_delta(current: Option<f64>, previous: Option<f64>) -> Option<f64> {
    match (current, previous) {
        (Some(cur), Some(prev)) => Some(cur - prev),
        _ => None,
    }
}

fn wrapped_year_bounds(
    year: i32,
    now_local: DateTime<Local>,
) -> (NaiveDate, NaiveDate, DateTime<Utc>, DateTime<Utc>) {
    let start_date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap_or(now_local.date_naive());
    let last_day = NaiveDate::from_ymd_opt(year, 12, 31).unwrap_or(start_date);
    let end_date = if year == now_local.year() {
        now_local.date_naive().min(last_day)
    } else {
        last_day
    };
    let start = local_start_of_day(start_date);
    let end_exclusive = end_date
        .checked_add_signed(ChronoDuration::days(1))
        .unwrap_or(end_date);
    let end = local_start_of_day(end_exclusive);
    (start_date, end_date, start, end)
}

fn streak_stats_for_year(
    daily_tokens: &HashMap<NaiveDate, u64>,
    end_date: NaiveDate,
) -> (u64, u64, HashSet<NaiveDate>) {
    if daily_tokens.is_empty() {
        return (0, 0, HashSet::new());
    }
    let mut days: Vec<NaiveDate> = daily_tokens.keys().copied().collect();
    days.sort();

    let mut max_streak = 1u64;
    let mut max_start = 0usize;
    let mut max_end = 0usize;
    let mut temp_start = 0usize;
    let mut temp_len = 1u64;

    for i in 1..days.len() {
        let prev = days[i - 1];
        let current = days[i];
        if current == prev + ChronoDuration::days(1) {
            temp_len += 1;
            if temp_len > max_streak {
                max_streak = temp_len;
                max_start = temp_start;
                max_end = i;
            }
        } else {
            temp_start = i;
            temp_len = 1;
        }
    }

    let max_streak_days: HashSet<NaiveDate> = days[max_start..=max_end].iter().copied().collect();

    let active_days: HashSet<NaiveDate> = daily_tokens.keys().copied().collect();
    let today = end_date;
    let yesterday = today - ChronoDuration::days(1);
    let current_anchor = if active_days.contains(&today) {
        Some(today)
    } else if active_days.contains(&yesterday) {
        Some(yesterday)
    } else {
        None
    };

    let current_streak = if let Some(mut cursor) = current_anchor {
        let mut streak = 0u64;
        loop {
            if active_days.contains(&cursor) {
                streak += 1;
                cursor = cursor - ChronoDuration::days(1);
            } else {
                break;
            }
        }
        streak
    } else {
        0
    };

    (max_streak, current_streak, max_streak_days)
}

fn heatmap_weeks_for_year(year: i32) -> Vec<[Option<NaiveDate>; 7]> {
    let start_date = NaiveDate::from_ymd_opt(year, 1, 1)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let end_date = NaiveDate::from_ymd_opt(year, 12, 31).unwrap_or(start_date);
    let start_offset = start_date.weekday().num_days_from_monday() as i64;
    let mut cursor = start_date
        .checked_sub_signed(ChronoDuration::days(start_offset))
        .unwrap_or(start_date);
    let end_offset = 6 - end_date.weekday().num_days_from_monday() as i64;
    let end_cursor = end_date
        .checked_add_signed(ChronoDuration::days(end_offset))
        .unwrap_or(end_date);
    let mut weeks = Vec::new();
    let mut week = [None; 7];

    loop {
        let weekday_idx = cursor.weekday().num_days_from_monday() as usize;
        week[weekday_idx] = Some(cursor);
        if weekday_idx == 6 {
            weeks.push(week);
            week = [None; 7];
            if cursor >= end_cursor {
                break;
            }
        }
        cursor = cursor
            .checked_add_signed(ChronoDuration::days(1))
            .unwrap_or(cursor);
    }

    weeks
}

fn heatmap_intensity(count: u64, max: u64) -> usize {
    if count == 0 || max == 0 {
        return 0;
    }
    let ratio = count as f64 / max as f64;
    if ratio <= 0.1 {
        1
    } else if ratio <= 0.25 {
        2
    } else if ratio <= 0.4 {
        3
    } else if ratio <= 0.6 {
        4
    } else if ratio <= 0.8 {
        5
    } else {
        6
    }
}

fn period_for_range(range: TimeRange, anchor: NaiveDate, now_local: DateTime<Local>) -> Period {
    match range {
        TimeRange::Day => {
            let start = local_start_of_day(anchor);
            let end = local_start_of_day(
                anchor
                    .checked_add_signed(ChronoDuration::days(1))
                    .unwrap_or(anchor),
            );
            Period {
                label: anchor.format("%b %d, %Y").to_string(),
                start,
                end,
            }
        }
        TimeRange::Week => {
            let start_date = start_of_week_local(anchor);
            let end_date = start_date
                .checked_add_signed(ChronoDuration::days(7))
                .unwrap_or(start_date);
            let iso = start_date.iso_week();
            Period {
                label: format!("{}-W{:02}", iso.year(), iso.week()),
                start: local_start_of_day(start_date),
                end: local_start_of_day(end_date),
            }
        }
        TimeRange::Month => {
            let start_date = first_day_of_month(anchor);
            let end_date = start_date
                .checked_add_months(Months::new(1))
                .unwrap_or(start_date);
            Period {
                label: start_date.format("%b %Y").to_string(),
                start: local_start_of_day(start_date),
                end: local_start_of_day(end_date),
            }
        }
        TimeRange::Year => {
            let start_date = NaiveDate::from_ymd_opt(anchor.year(), 1, 1).unwrap_or(anchor);
            let end_date = NaiveDate::from_ymd_opt(anchor.year() + 1, 1, 1).unwrap_or(start_date);
            Period {
                label: format!("{}", start_date.year()),
                start: local_start_of_day(start_date),
                end: local_start_of_day(end_date),
            }
        }
        TimeRange::All => {
            let start_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Period {
                label: "All Time".to_string(),
                start: local_start_of_day(start_date),
                end: now_local.with_timezone(&Utc),
            }
        }
    }
}

fn week_period_anchor_prev(today: NaiveDate) -> NaiveDate {
    let start = start_of_week_local(today);
    start
        .checked_sub_signed(ChronoDuration::days(7))
        .unwrap_or(start)
}

fn month_period_anchor_prev(today: NaiveDate) -> NaiveDate {
    let start = first_day_of_month(today);
    start.checked_sub_months(Months::new(1)).unwrap_or(start)
}

fn start_of_week_local(date: NaiveDate) -> NaiveDate {
    let days_from_monday = date.weekday().num_days_from_monday() as i64;
    date.checked_sub_signed(ChronoDuration::days(days_from_monday))
        .unwrap_or(date)
}

fn first_day_of_month(date: NaiveDate) -> NaiveDate {
    NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap_or(date)
}

fn local_start_of_day(date: NaiveDate) -> DateTime<Utc> {
    let naive = date.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    });
    match Local.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt.with_timezone(&Utc),
        LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
        LocalResult::None => Local
            .from_local_datetime(&(naive + ChronoDuration::hours(1)))
            .earliest()
            .unwrap_or_else(Local::now)
            .with_timezone(&Utc),
    }
}

struct ListState {
    selected_row: usize,
    scroll_offset: usize,
    visible_rows: usize,
}

impl ListState {
    fn new() -> Self {
        Self {
            selected_row: 0,
            scroll_offset: 0,
            visible_rows: 0,
        }
    }

    fn reset(&mut self) {
        self.selected_row = 0;
        self.scroll_offset = 0;
    }

    fn set_visible_rows(&mut self, visible_rows: usize, total_rows: usize) {
        self.visible_rows = visible_rows;
        self.clamp(total_rows);
    }

    fn clamp(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row >= total_rows {
            self.selected_row = total_rows - 1;
        }
        if self.scroll_offset > self.selected_row {
            self.scroll_offset = self.selected_row;
        }
        if self.visible_rows == 0 {
            return;
        }
        let max_scroll = total_rows.saturating_sub(self.visible_rows);
        if self.scroll_offset > max_scroll {
            self.scroll_offset = max_scroll;
        }
        if self.selected_row >= self.scroll_offset + self.visible_rows {
            self.scroll_offset = self.selected_row + 1 - self.visible_rows;
        }
    }

    fn move_selection_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row > 0 {
            self.selected_row -= 1;
        }
        self.clamp(total_rows);
    }

    fn move_selection_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row + 1 < total_rows {
            self.selected_row += 1;
        }
        self.clamp(total_rows);
    }

    fn page_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        let step = self.visible_rows.max(1);
        if self.selected_row >= step {
            self.selected_row -= step;
        } else {
            self.selected_row = 0;
        }
        self.clamp(total_rows);
    }

    fn page_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        let step = self.visible_rows.max(1);
        self.selected_row = (self.selected_row + step).min(total_rows - 1);
        self.clamp(total_rows);
    }

    fn page_info(&self, total_rows: usize) -> (usize, usize) {
        if total_rows == 0 || self.visible_rows == 0 {
            return (1, 1);
        }
        let pages = total_rows.div_ceil(self.visible_rows);
        let page = if self.scroll_offset + self.visible_rows >= total_rows {
            pages
        } else {
            (self.scroll_offset / self.visible_rows) + 1
        };
        (page, pages.max(1))
    }
}

struct SessionsViewState {
    nav: TimeNavState,
    list: ListState,
    sort: SessionSort,
    initialized: bool,
}

impl SessionsViewState {
    fn new() -> Self {
        let today = Local::now().date_naive();
        Self {
            nav: TimeNavState::new(TimeRange::Day, today),
            list: ListState::new(),
            sort: SessionSort::Recent,
            initialized: false,
        }
    }

    fn reset(&mut self) {
        self.list.reset();
        self.initialized = false;
    }

    fn set_sort(&mut self, sort: SessionSort) {
        if self.sort != sort {
            self.sort = sort;
            self.reset();
        }
    }

    fn sync_with(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.list.reset();
            self.initialized = false;
            return;
        }
        if !self.initialized {
            self.list.reset();
            self.initialized = true;
        } else {
            self.list.clamp(total_rows);
        }
    }

    fn move_selection_up(&mut self, total_rows: usize) {
        self.list.move_selection_up(total_rows);
    }

    fn move_selection_down(&mut self, total_rows: usize) {
        self.list.move_selection_down(total_rows);
    }

    fn page_up(&mut self, total_rows: usize) {
        self.list.page_up(total_rows);
    }

    fn page_down(&mut self, total_rows: usize) {
        self.list.page_down(total_rows);
    }

    fn selected<'a>(
        &self,
        sessions: &'a [SessionAggregate],
        offset: usize,
    ) -> Option<&'a SessionAggregate> {
        let idx = self.list.selected_row.checked_sub(offset)?;
        sessions.get(idx)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum MessageGroupKey {
    Message(i64),
    Unattributed,
}

enum ModalRowDescriptor {
    Summary,
    Day {
        date: NaiveDate,
    },
    Message {
        key: MessageGroupKey,
        message_index: Option<usize>,
        expanded: bool,
    },
    Turn {
        key: MessageGroupKey,
        turn_index: usize,
    },
    Placeholder {
        label: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModalRowCacheKey {
    message_count: usize,
    has_unattributed: bool,
    expanded: Option<MessageGroupKey>,
    expanded_turns_len: Option<usize>,
    show_summary: bool,
}

fn modal_row_cache_key(
    messages: &[SessionMessage],
    unattributed: Option<&SessionMessage>,
    expanded: Option<MessageGroupKey>,
    show_summary: bool,
    turns_by_message: &HashMap<MessageGroupKey, Vec<SessionTurn>>,
) -> ModalRowCacheKey {
    let expanded_turns_len =
        expanded.and_then(|key| turns_by_message.get(&key).map(|turns| turns.len()));
    ModalRowCacheKey {
        message_count: messages.len(),
        has_unattributed: unattributed.is_some(),
        expanded,
        expanded_turns_len,
        show_summary,
    }
}

fn is_modal_row_selectable(row: &ModalRowDescriptor) -> bool {
    matches!(
        row,
        ModalRowDescriptor::Message { .. } | ModalRowDescriptor::Turn { .. }
    )
}

fn find_modal_selectable(
    rows: &[ModalRowDescriptor],
    start: usize,
    direction: i32,
) -> Option<usize> {
    if rows.is_empty() {
        return None;
    }
    let dir = if direction >= 0 { 1 } else { -1 };
    if dir > 0 {
        for (idx, row) in rows.iter().enumerate().skip(start) {
            if is_modal_row_selectable(row) {
                return Some(idx);
            }
        }
    } else {
        let mut idx = start.min(rows.len().saturating_sub(1));
        loop {
            if is_modal_row_selectable(&rows[idx]) {
                return Some(idx);
            }
            if idx == 0 {
                break;
            }
            idx -= 1;
        }
    }
    None
}

fn find_modal_message_row(rows: &[ModalRowDescriptor], key: MessageGroupKey) -> Option<usize> {
    rows.iter().enumerate().find_map(|(idx, row)| match row {
        ModalRowDescriptor::Message { key: row_key, .. } if *row_key == key => Some(idx),
        _ => None,
    })
}

struct SessionModalState {
    open: bool,
    list: ListState,
    active_key: Option<String>,
    expanded: Option<MessageGroupKey>,
    row_cache: Vec<ModalRowDescriptor>,
    row_cache_key: Option<ModalRowCacheKey>,
}

impl SessionModalState {
    fn new() -> Self {
        Self {
            open: false,
            list: ListState::new(),
            active_key: None,
            expanded: None,
            row_cache: Vec::new(),
            row_cache_key: None,
        }
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn open_for(&mut self, key: String) {
        self.list.reset();
        self.active_key = Some(key);
        self.expanded = None;
        self.row_cache.clear();
        self.row_cache_key = None;
        self.open = true;
    }

    fn close(&mut self) {
        self.open = false;
        self.active_key = None;
        self.expanded = None;
        self.row_cache.clear();
        self.row_cache_key = None;
    }

    fn set_visible_rows(&mut self, visible_rows: usize, total_rows: usize) {
        self.list.set_visible_rows(visible_rows, total_rows);
    }

    fn set_row_cache(&mut self, rows: Vec<ModalRowDescriptor>, key: ModalRowCacheKey) {
        self.row_cache = rows;
        self.row_cache_key = Some(key);
        self.list.clamp(self.row_cache.len());
        self.ensure_selectable(true);
    }

    fn row_cache(&self) -> &[ModalRowDescriptor] {
        &self.row_cache
    }

    fn row_cache_key(&self) -> Option<ModalRowCacheKey> {
        self.row_cache_key
    }

    fn select_row(&mut self, idx: usize) {
        self.list.selected_row = idx;
        self.list.clamp(self.row_cache.len());
    }

    fn ensure_selectable(&mut self, prefer_forward: bool) {
        if self.row_cache.is_empty() {
            self.list.reset();
            return;
        }
        let idx = self
            .list
            .selected_row
            .min(self.row_cache.len().saturating_sub(1));
        if is_modal_row_selectable(&self.row_cache[idx]) {
            self.list.clamp(self.row_cache.len());
            return;
        }
        let forward = find_modal_selectable(&self.row_cache, idx + 1, 1);
        let backward = if idx > 0 {
            find_modal_selectable(&self.row_cache, idx - 1, -1)
        } else {
            None
        };
        let chosen = if prefer_forward {
            forward.or(backward)
        } else {
            backward.or(forward)
        };
        if let Some(next) = chosen {
            self.list.selected_row = next;
        }
        self.list.clamp(self.row_cache.len());
    }

    fn move_selection_up(&mut self) {
        let Some(idx) = find_modal_selectable(
            &self.row_cache,
            self.list.selected_row.saturating_sub(1),
            -1,
        ) else {
            return;
        };
        self.list.selected_row = idx;
        self.list.clamp(self.row_cache.len());
    }

    fn move_selection_down(&mut self) {
        let Some(idx) = find_modal_selectable(&self.row_cache, self.list.selected_row + 1, 1)
        else {
            return;
        };
        self.list.selected_row = idx;
        self.list.clamp(self.row_cache.len());
    }

    fn page_up(&mut self) {
        let total = self.row_cache.len();
        if total == 0 {
            self.list.reset();
            return;
        }
        let step = self.list.visible_rows.max(1);
        let target = self.list.selected_row.saturating_sub(step);
        self.move_selection_to(target, -1);
    }

    fn page_down(&mut self) {
        let total = self.row_cache.len();
        if total == 0 {
            self.list.reset();
            return;
        }
        let step = self.list.visible_rows.max(1);
        let target = (self.list.selected_row + step).min(total.saturating_sub(1));
        self.move_selection_to(target, 1);
    }

    fn move_selection_to(&mut self, target: usize, direction: i32) {
        let total = self.row_cache.len();
        if total == 0 {
            self.list.reset();
            return;
        }
        let dir = if direction >= 0 { 1 } else { -1 };
        let forward = find_modal_selectable(&self.row_cache, target, dir);
        let backward_start = target.saturating_sub(1);
        let backward = find_modal_selectable(&self.row_cache, backward_start, -dir);
        if let Some(idx) = forward.or(backward) {
            self.list.selected_row = idx;
        }
        self.list.clamp(self.row_cache.len());
    }

    fn page_info(&self, total_rows: usize) -> (usize, usize) {
        self.list.page_info(total_rows)
    }

    fn scroll_offset(&self) -> usize {
        self.list.scroll_offset
    }

    fn selected_row(&self) -> usize {
        self.list.selected_row
    }

    fn expanded_key(&self) -> Option<MessageGroupKey> {
        self.expanded
    }

    fn toggle_expand(&mut self, key: MessageGroupKey) {
        if self.expanded == Some(key) {
            self.expanded = None;
        } else {
            self.expanded = Some(key);
        }
    }
}

struct StatsViewState {
    nav: TimeNavState,
}

impl StatsViewState {
    fn new() -> Self {
        let today = Local::now().date_naive();
        Self {
            nav: TimeNavState::new(TimeRange::Month, today),
        }
    }
}

struct WrappedViewState {
    year: i32,
}

impl WrappedViewState {
    fn new(today: NaiveDate) -> Self {
        Self { year: today.year() }
    }

    fn prev_year(&mut self) {
        self.year = self.year.saturating_sub(1);
    }

    fn next_year(&mut self) {
        self.year = self.year.saturating_add(1);
    }
}

struct MissingPriceModalState {
    open: bool,
    selected: usize,
}

impl MissingPriceModalState {
    fn new() -> Self {
        Self {
            open: false,
            selected: 0,
        }
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn open(&mut self) {
        self.open = true;
        self.selected = 0;
    }

    fn close(&mut self) {
        self.open = false;
    }

    fn move_up(&mut self, total: usize) {
        if total == 0 {
            self.selected = 0;
            return;
        }
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    fn move_down(&mut self, total: usize) {
        if total == 0 {
            self.selected = 0;
            return;
        }
        if self.selected + 1 < total {
            self.selected += 1;
        }
    }
}

struct HelpModalState {
    open: bool,
}

impl HelpModalState {
    fn new() -> Self {
        Self { open: false }
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn toggle(&mut self) {
        self.open = !self.open;
    }
}

struct PricingViewState {
    list: ListState,
}

impl PricingViewState {
    fn new() -> Self {
        Self {
            list: ListState::new(),
        }
    }

    fn sync(&mut self, rows: usize) {
        self.list.clamp(rows);
    }

    fn move_selection_up(&mut self, rows: usize) {
        self.list.move_selection_up(rows);
    }

    fn move_selection_down(&mut self, rows: usize) {
        self.list.move_selection_down(rows);
    }

    fn page_up(&mut self, rows: usize) {
        self.list.page_up(rows);
    }

    fn page_down(&mut self, rows: usize) {
        self.list.page_down(rows);
    }
}

fn format_cwd_label(cwd: Option<&str>) -> String {
    let value = cwd.unwrap_or("").trim();
    if value.is_empty() {
        return "—".to_string();
    }
    Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_repo_label(repo_url: Option<&str>) -> String {
    let value = repo_url.unwrap_or("").trim();
    if value.is_empty() {
        return "—".to_string();
    }
    let trimmed = value.trim_end_matches(".git");
    let parts: Vec<&str> = trimmed.split('/').filter(|part| !part.is_empty()).collect();
    if let Some(last) = parts.last() {
        last.to_string()
    } else {
        trimmed
            .rsplit_once(':')
            .map(|(_, tail)| tail.to_string())
            .unwrap_or_else(|| trimmed.to_string())
    }
}

fn format_branch_label(branch: Option<&str>) -> String {
    let value = branch.unwrap_or("").trim();
    if value.is_empty() {
        "—".to_string()
    } else {
        value.to_string()
    }
}

fn format_context_label(
    repo_url: Option<&str>,
    branch: Option<&str>,
    cwd: Option<&str>,
    mode: LayoutMode,
) -> String {
    match mode {
        LayoutMode::Compact => {
            let project = format_project_label(repo_url, cwd);
            let branch = format_branch_label(branch);
            let mut parts = Vec::new();
            if project != "—" {
                parts.push(project);
            }
            if branch != "—" {
                parts.push(branch);
            }
            if parts.is_empty() {
                "—".to_string()
            } else {
                parts.join(" • ")
            }
        }
        LayoutMode::Wide => format_project_label(repo_url, cwd),
    }
}

fn format_project_label(repo_url: Option<&str>, cwd: Option<&str>) -> String {
    let folder = format_cwd_label(cwd);
    if folder != "—" {
        return folder;
    }
    format_repo_label(repo_url)
}

fn session_key(aggregate: &SessionAggregate) -> String {
    aggregate.session_id.clone()
}

fn full_session_label(id: &str) -> String {
    let raw = id.trim();
    if raw.is_empty() {
        "(no session id)".to_string()
    } else {
        raw.to_string()
    }
}

fn session_detail_rows(
    aggregate: &SessionAggregate,
    theme: &UiTheme,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> Vec<Row<'static>> {
    let cwd_spans = format_cwd_spans(aggregate.cwd.as_ref(), theme);
    let repo_spans = format_repo_branch_spans(
        aggregate.repo_url.as_ref(),
        aggregate.repo_branch.as_ref(),
        theme,
    );
    let mut rows = vec![
        detail_row(
            "First Prompt",
            format_detail_snippet(aggregate.title.as_ref()),
            theme,
        ),
        detail_row(
            "Last Result",
            format_detail_snippet(aggregate.last_summary.as_ref()),
            theme,
        ),
        detail_row_spans("CWD", cwd_spans, theme),
        detail_row_spans("Repo", repo_spans, theme),
        detail_row(
            "Subagent",
            format_detail_snippet(aggregate.subagent.as_ref()),
            theme,
        ),
    ];
    rows.extend(format_model_detail_rows(model_mix, theme));
    rows.extend(format_tool_detail_rows(tool_counts, theme));
    rows
}

fn format_model_detail_rows(model_mix: &[ModelUsageRow], theme: &UiTheme) -> Vec<Row<'static>> {
    if model_mix.is_empty() {
        return vec![detail_row("Models", "—".to_string(), theme)];
    }
    if model_mix.len() == 1 {
        let row = &model_mix[0];
        let spans = format_model_effort_spans(&row.model, row.reasoning_effort.as_deref());
        return vec![detail_row_spans("Models", spans, theme)];
    }

    let mut groups = group_models(model_mix);
    groups.sort_by(|a, b| b.total_tokens.cmp(&a.total_tokens));
    let total = total_tokens(model_mix);
    let max_model_len = groups
        .iter()
        .map(|group| group.model.chars().count())
        .max()
        .unwrap_or(0);
    let mut rows = Vec::new();
    for (idx, group) in groups.iter().enumerate() {
        let spans = format_single_model_effort_spans(group, total, theme, max_model_len);
        let label = if idx == 0 { "Models" } else { "" };
        rows.push(detail_row_spans(label, spans, theme));
    }
    rows
}

fn format_single_model_effort_spans(
    group: &ModelGroup,
    total_tokens: u64,
    theme: &UiTheme,
    model_width: usize,
) -> Vec<Span<'static>> {
    if total_tokens == 0 {
        return vec![Span::raw(format!("{} —", group.model))];
    }
    let mut efforts = group.efforts.clone();
    efforts.sort_by(|a, b| {
        let rank_a = effort_rank(&a.label);
        let rank_b = effort_rank(&b.label);
        rank_a.cmp(&rank_b).then_with(|| b.tokens.cmp(&a.tokens))
    });
    let mut spans = Vec::new();
    let padded_model = pad_right(&group.model, model_width);
    spans.push(Span::raw(format!("{padded_model} ")));
    for (idx, effort) in efforts.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::raw("  "));
        }
        let pct = percent_of_total(effort.tokens, total_tokens);
        spans.extend(percent_bar_spans(pct, 9, theme));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            effort.label.clone(),
            Style::default().fg(Color::Green),
        ));
    }
    spans
}

fn format_model_effort_spans(model: &str, effort: Option<&str>) -> Vec<Span<'static>> {
    let effort = effort
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let mut spans = Vec::new();
    spans.push(Span::raw(model.to_string()));
    if let Some(value) = effort {
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            value.to_string(),
            Style::default().fg(Color::Green),
        ));
    }
    spans
}

fn format_effort_label(effort: Option<&str>) -> String {
    effort
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string())
}

fn percent_of_total(value: u64, total: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    ((value as f64 / total as f64) * 100.0).round() as u64
}

fn pad_right(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        return value.to_string();
    }
    let mut out = String::with_capacity(width);
    out.push_str(value);
    out.extend(std::iter::repeat_n(' ', width - len));
    out
}

fn percent_bar_spans(percent: u64, width: usize, theme: &UiTheme) -> Vec<Span<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let ratio = (percent as f64 / 100.0).clamp(0.0, 1.0);
    let filled = (ratio * width as f64).round() as usize;
    let percent_text = format!("{percent}%");
    let mut chars = vec![' '; width];
    if percent_text.chars().count() <= width {
        let start = (width - percent_text.chars().count()) / 2;
        for (idx, ch) in percent_text.chars().enumerate() {
            chars[start + idx] = ch;
        }
    }
    let empty_bg = Color::DarkGray;
    let fill_bg = theme.highlight_bg;
    let mut spans = Vec::with_capacity(width);
    for (idx, ch) in chars.into_iter().enumerate() {
        let bg = if idx < filled { fill_bg } else { empty_bg };
        let mut style = Style::default().bg(bg);
        if ch != ' ' {
            style = style.fg(Color::White).add_modifier(Modifier::BOLD);
        }
        spans.push(Span::styled(ch.to_string(), style));
    }
    spans
}

fn effort_rank(effort: &str) -> u8 {
    match effort.to_ascii_lowercase().as_str() {
        "xhigh" | "extra_high" | "extra-high" | "highest" => 0,
        "high" => 1,
        "medium" | "med" => 2,
        "low" => 3,
        "default" => 4,
        _ => 5,
    }
}

fn total_tokens(model_mix: &[ModelUsageRow]) -> u64 {
    model_mix.iter().map(|row| row.total_tokens).sum()
}

fn format_tool_detail_rows(tools: &[ToolCountRow], theme: &UiTheme) -> Vec<Row<'static>> {
    if tools.is_empty() {
        return vec![detail_row("Tools", "—".to_string(), theme)];
    }
    let total_calls: u64 = tools.iter().map(|row| row.count).sum();
    let name_width = 14usize;
    let bar_width = 8usize;
    let mut rows = Vec::new();
    for (row_idx, chunk) in tools.chunks(6).enumerate() {
        let mut spans: Vec<Span<'static>> = Vec::new();
        for (idx, tool) in chunk.iter().enumerate() {
            if idx > 0 {
                spans.push(Span::raw("  "));
            }
            let (bar_spans, overlaid) =
                tool_share_bar_spans(tool.count, total_calls, bar_width, theme);
            spans.extend(bar_spans);
            if !overlaid {
                spans.push(Span::raw(format!(" {}", tool.count)));
            }
            spans.push(Span::raw(" "));
            let name = truncate_text(&tool.tool, name_width);
            spans.push(Span::styled(
                format!("{name:<name_width$}"),
                Style::default().fg(Color::Green),
            ));
        }
        let label = if row_idx == 0 { "Tools" } else { "" };
        rows.push(detail_row_spans(label, spans, theme));
    }
    rows
}

fn tool_share_bar_spans(
    count: u64,
    total: u64,
    width: usize,
    theme: &UiTheme,
) -> (Vec<Span<'static>>, bool) {
    if total == 0 || width == 0 {
        return (
            vec![Span::styled(
                "—".to_string(),
                Style::default().fg(Color::DarkGray),
            )],
            true,
        );
    }
    if count == 0 {
        return (
            vec![Span::styled(
                " ".repeat(width.max(1)),
                Style::default().bg(Color::DarkGray),
            )],
            true,
        );
    }
    let ratio = (count as f64 / total as f64).clamp(0.0, 1.0);
    let filled = ((ratio.min(1.0)) * width as f64).round() as usize;
    let empty_bg = Color::DarkGray;
    let fill_bg = theme.highlight_bg;
    let mut spans = Vec::with_capacity(width);
    for idx in 0..width {
        let bg = if idx < filled { fill_bg } else { empty_bg };
        spans.push(Span::styled(" ".to_string(), Style::default().bg(bg)));
    }

    let count_label = count.to_string();
    let label_len = count_label.chars().count();
    if label_len > width {
        return (spans, false);
    }
    let start = (width - label_len) / 2;
    for (offset, ch) in count_label.chars().enumerate() {
        let idx = start + offset;
        if idx < spans.len() {
            spans[idx] = Span::styled(
                ch.to_string(),
                Style::default()
                    .fg(Color::White)
                    .bg(if idx < filled { fill_bg } else { empty_bg })
                    .add_modifier(Modifier::BOLD),
            );
        }
    }

    (spans, true)
}

#[derive(Clone)]
struct ModelGroup {
    model: String,
    total_tokens: u64,
    efforts: Vec<EffortShare>,
}

#[derive(Clone)]
struct EffortShare {
    label: String,
    tokens: u64,
}

fn group_models(model_mix: &[ModelUsageRow]) -> Vec<ModelGroup> {
    let mut map: HashMap<String, Vec<EffortShare>> = HashMap::new();
    for row in model_mix {
        map.entry(row.model.clone()).or_default().push(EffortShare {
            label: format_effort_label(row.reasoning_effort.as_deref()),
            tokens: row.total_tokens,
        });
    }
    let mut groups = Vec::with_capacity(map.len());
    for (model, mut efforts) in map {
        efforts.sort_by(|a, b| b.tokens.cmp(&a.tokens));
        let total = efforts.iter().map(|effort| effort.tokens).sum();
        groups.push(ModelGroup {
            model,
            total_tokens: total,
            efforts,
        });
    }
    groups
}

fn format_cwd_spans(cwd: Option<&String>, _theme: &UiTheme) -> Vec<Span<'static>> {
    let value = format_detail_snippet(cwd);
    if value == "—" {
        return vec![Span::raw("—")];
    }
    highlight_tail_spans(&value, '/', Style::default().fg(Color::Green))
}

fn format_repo_branch_spans(
    repo: Option<&String>,
    branch: Option<&String>,
    _theme: &UiTheme,
) -> Vec<Span<'static>> {
    let repo_value = format_detail_snippet(repo);
    let branch_value = format_detail_snippet(branch);
    if repo_value == "—" && branch_value == "—" {
        return vec![Span::raw("—")];
    }
    let mut spans = Vec::new();
    if repo_value != "—" {
        spans.extend(highlight_tail_spans(
            &repo_value,
            '/',
            Style::default().fg(Color::Green),
        ));
    }
    if branch_value != "—" {
        if repo_value != "—" {
            spans.push(Span::styled(
                " @ ".to_string(),
                Style::default().fg(Color::DarkGray),
            ));
        }
        spans.push(Span::styled(
            branch_value,
            Style::default().fg(Color::Green),
        ));
    }
    spans
}

fn highlight_tail_spans(value: &str, sep: char, tail_style: Style) -> Vec<Span<'static>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return vec![Span::raw("—")];
    }
    let (head, tail) = match trimmed.rfind(sep) {
        Some(idx) if idx + 1 < trimmed.len() => (&trimmed[..=idx], &trimmed[idx + 1..]),
        Some(_) => ("", trimmed),
        None => ("", trimmed),
    };
    let mut spans = Vec::new();
    if !head.is_empty() {
        spans.push(Span::raw(head.to_string()));
    }
    if !tail.is_empty() {
        spans.push(Span::styled(tail.to_string(), tail_style));
    }
    spans
}

fn session_token_summary(aggregate: &SessionAggregate) -> String {
    format!(
        "in {} | out {} | cached {} | blended {} | api {} | reasoning {}",
        format_tokens(aggregate.prompt_tokens),
        format_tokens(aggregate.completion_tokens),
        format_tokens(aggregate.cached_prompt_tokens),
        format_tokens(aggregate.blended_total()),
        format_tokens(aggregate.total_tokens),
        format_tokens(aggregate.reasoning_tokens),
    )
}

fn build_session_share_text(
    aggregate: &SessionAggregate,
    turn_count: usize,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Cost: {}", format_cost(aggregate.cost_usd)));
    lines.push(format!("Messages: {}", aggregate.user_messages));
    lines.push(format!("Turns: {turn_count}"));
    lines.push(format!("Tokens: {}", session_token_summary(aggregate)));
    lines.push(format!("Models: {}", format_model_mix_plain(model_mix)));
    lines.push(format!("Tools: {}", format_tool_counts(tool_counts)));
    lines.join("\n")
}

fn copy_session_details_osc52(
    aggregate: &SessionAggregate,
    turn_count: usize,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> io::Result<()> {
    let text = build_session_share_text(aggregate, turn_count, model_mix, tool_counts);
    let encoded = general_purpose::STANDARD.encode(text.as_bytes());
    let mut stdout = io::stdout();
    write!(stdout, "\x1b]52;c;{}\x07", encoded)?;
    stdout.flush()
}

fn detail_row(label: &'static str, value: String, theme: &UiTheme) -> Row<'static> {
    Row::new(vec![
        Cell::from(label).style(
            Style::default()
                .fg(theme.header_fg)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from(value),
    ])
}

fn detail_row_spans(
    label: &'static str,
    spans: Vec<Span<'static>>,
    theme: &UiTheme,
) -> Row<'static> {
    Row::new(vec![
        Cell::from(label).style(
            Style::default()
                .fg(theme.header_fg)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from(Line::from(spans)),
    ])
}

fn format_detail_snippet(text: Option<&String>) -> String {
    text.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
    .unwrap_or_else(|| "—".to_string())
}

fn format_model_mix_plain(models: &[ModelUsageRow]) -> String {
    format_model_mix_with_bars(models, false)
}

fn format_model_mix_with_bars(models: &[ModelUsageRow], with_bars: bool) -> String {
    if models.is_empty() {
        return "—".to_string();
    }
    let total: u64 = models.iter().map(|row| row.total_tokens).sum();
    if total == 0 {
        return "—".to_string();
    }

    let mut parts = Vec::new();
    let mut remaining = 0usize;
    for (idx, row) in models.iter().enumerate() {
        if idx >= 4 {
            remaining = models.len().saturating_sub(idx);
            break;
        }
        let pct = (row.total_tokens as f64 / total as f64) * 100.0;
        let label = match row.reasoning_effort.as_deref() {
            Some(effort) => format!("{} {}", row.model, effort),
            None => row.model.clone(),
        };
        if with_bars {
            let bar = ratio_bar(pct / 100.0, 6);
            parts.push(format!("{label} {bar} {}%", pct.round() as u64));
        } else {
            parts.push(format!("{label} {}%", pct.round() as u64));
        }
    }
    if remaining > 0 {
        parts.push(format!("+{remaining} more"));
    }
    parts.join(" | ")
}

fn format_tool_counts(tools: &[ToolCountRow]) -> String {
    if tools.is_empty() {
        return "—".to_string();
    }
    let mut parts = Vec::new();
    let mut remaining = 0usize;
    for (idx, row) in tools.iter().enumerate() {
        if idx >= 5 {
            remaining = tools.len().saturating_sub(idx);
            break;
        }
        parts.push(format!("{} {}", truncate_text(&row.tool, 16), row.count));
    }
    if remaining > 0 {
        parts.push(format!("+{remaining} more"));
    }
    parts.join(" | ")
}

fn truncate_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let mut truncated = String::new();
    for ch in input.chars().take(max_chars.saturating_sub(1)) {
        truncated.push(ch);
    }
    truncated.push('…');
    truncated
}

fn light_blue_header(labels: Vec<&'static str>, theme: &UiTheme) -> Row<'static> {
    Row::new(labels).style(
        Style::default()
            .fg(theme.header_fg)
            .add_modifier(Modifier::BOLD),
    )
}

fn gray_block(title: impl Into<String>, theme: &UiTheme) -> Block<'static> {
    Block::default()
        .title(title.into())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.border_fg))
}

#[cfg(test)]
mod tests {
    // No TUI-specific tests at the moment.
}
