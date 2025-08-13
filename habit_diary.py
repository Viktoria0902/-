#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DB_PATH = Path("/workspace/habit_diary.db")

BASE_POINTS = 10
DIFFICULTY_MULTIPLIER = {
    1: 1.0,   # легко
    2: 1.2,   # средне
    3: 1.5,   # сложно
}
MAX_STREAK_BONUS_MULTIPLIER = 0.5  # +50% максимум
STREAK_STEP_BONUS = 0.05  # +5% за каждый день серии после первого
WEEKLY_CONSISTENCY_BONUS = 20


@dataclass
class Habit:
    id: int
    name: str
    cue: str
    intention: str
    min_action: str
    difficulty: int
    frequency_per_week: int
    start_date: str
    is_active: int
    created_at: str


class HabitDiary:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self._ensure_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_db(self) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS habits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    cue TEXT NOT NULL,
                    intention TEXT NOT NULL,
                    min_action TEXT NOT NULL,
                    difficulty INTEGER NOT NULL CHECK(difficulty IN (1,2,3)),
                    frequency_per_week INTEGER NOT NULL CHECK(frequency_per_week BETWEEN 1 AND 7),
                    start_date TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    habit_id INTEGER NOT NULL,
                    log_date TEXT NOT NULL,
                    completed INTEGER NOT NULL DEFAULT 1,
                    notes TEXT,
                    points INTEGER NOT NULL DEFAULT 0,
                    kind TEXT NOT NULL DEFAULT 'COMPLETION', -- COMPLETION | BONUS | BADGE
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(habit_id) REFERENCES habits(id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weekly_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    week_start_date TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS awards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    habit_id INTEGER,
                    award_date TEXT NOT NULL,
                    points INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    period_start TEXT,
                    period_end TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(habit_id) REFERENCES habits(id)
                )
                """
            )
            conn.commit()

    # ------------------ Habit operations ------------------
    def add_habit(
        self,
        name: str,
        cue: str,
        intention: str,
        min_action: str,
        difficulty: int,
        frequency_per_week: int,
        start_date_value: Optional[date] = None,
    ) -> int:
        today_str = (start_date_value or date.today()).isoformat()
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO habits (name, cue, intention, min_action, difficulty, frequency_per_week, start_date, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (name, cue, intention, min_action, difficulty, frequency_per_week, today_str, created_at),
            )
            conn.commit()
            return cur.lastrowid

    def list_habits(self, include_inactive: bool = False) -> List[Habit]:
        query = "SELECT * FROM habits" + ("" if include_inactive else " WHERE is_active = 1") + " ORDER BY id ASC"
        with self._connect() as conn:
            rows = conn.execute(query).fetchall()
            return [Habit(**dict(row)) for row in rows]

    def deactivate_habit(self, habit_id: int) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE habits SET is_active = 0 WHERE id = ?", (habit_id,))
            conn.commit()

    def _get_habit_by_id_or_name(self, habit_ref: str) -> Habit:
        with self._connect() as conn:
            if habit_ref.isdigit():
                row = conn.execute("SELECT * FROM habits WHERE id = ?", (int(habit_ref),)).fetchone()
            else:
                row = conn.execute("SELECT * FROM habits WHERE name = ?", (habit_ref,)).fetchone()
            if row is None:
                raise ValueError("Привычка не найдена по идентификатору или имени: " + habit_ref)
            return Habit(**dict(row))

    # ------------------ Logging & points ------------------
    def log_completion(
        self,
        habit_ref: str,
        log_date_value: Optional[date] = None,
        notes: Optional[str] = None,
    ) -> Tuple[int, int]:
        habit = self._get_habit_by_id_or_name(habit_ref)
        log_day = (log_date_value or date.today())
        log_day_str = log_day.isoformat()

        # Prevent multiple point-earning completions per day
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT COUNT(1) AS c FROM logs WHERE habit_id = ? AND log_date = ? AND kind = 'COMPLETION'",
                (habit.id, log_day_str),
            ).fetchone()[0]
            if existing:
                # Allow note-only append but no extra points
                created_at = datetime.now().isoformat(timespec="seconds")
                conn.execute(
                    """
                    INSERT INTO logs (habit_id, log_date, completed, notes, points, kind, created_at)
                    VALUES (?, ?, 0, ?, 0, 'COMPLETION', ?)
                    """,
                    (habit.id, log_day_str, (notes or "Повторная отметка без начисления очков"), created_at),
                )
                conn.commit()
                return (0, self._get_month_total_points(log_day.year, log_day.month))

        streak_length = self._calculate_streak_length(habit.id, log_day)
        points = self._calculate_points_for_completion(habit.difficulty, streak_length)

        created_at = datetime.now().isoformat(timespec="seconds")
        with self._connect() as conn:
            # Insert completion log with calculated points
            conn.execute(
                """
                INSERT INTO logs (habit_id, log_date, completed, notes, points, kind, created_at)
                VALUES (?, ?, 1, ?, ?, 'COMPLETION', ?)
                """,
                (habit.id, log_day_str, notes, points, created_at),
            )
            conn.commit()

        # Check weekly consistency bonus and badges
        bonus_points = self._award_weekly_consistency_bonus_if_eligible(habit, log_day)
        badges_points = self._check_and_award_badges(habit, log_day)

        total_points_now = self._get_month_total_points(log_day.year, log_day.month)
        return (points + bonus_points + badges_points, total_points_now)

    def _calculate_streak_length(self, habit_id: int, upto_day: date) -> int:
        # Count consecutive days including upto_day with at least one completion per day
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT log_date FROM logs
                WHERE habit_id = ? AND kind = 'COMPLETION' AND completed = 1 AND DATE(log_date) <= DATE(?)
                ORDER BY log_date DESC
                """,
                (habit_id, upto_day.isoformat()),
            ).fetchall()
        if not rows:
            return 1  # first day counts as streak length 1
        streak = 0
        cursor_day = upto_day
        dates_set = {row["log_date"] for row in rows}
        # iterate backwards from upto_day until a gap is found
        while True:
            day_str = cursor_day.isoformat()
            if day_str in dates_set:
                streak += 1
                cursor_day = cursor_day - timedelta(days=1)
            else:
                # If the very first completion ever is today and no previous day, streak is 1
                if streak == 0:
                    streak = 1
                break
        return streak

    def _calculate_points_for_completion(self, difficulty: int, streak_length: int) -> int:
        diff_mult = DIFFICULTY_MULTIPLIER.get(difficulty, 1.0)
        streak_bonus = min(MAX_STREAK_BONUS_MULTIPLIER, STREAK_STEP_BONUS * max(0, streak_length - 1))
        total = BASE_POINTS * diff_mult * (1.0 + streak_bonus)
        return int(round(total))

    def _award_weekly_consistency_bonus_if_eligible(self, habit: Habit, log_day: date) -> int:
        # Determine ISO week (Mon-Sun) containing log_day
        iso_year, iso_week, _ = log_day.isocalendar()
        week_start = iso_to_monday(iso_year, iso_week)
        week_end = week_start + timedelta(days=6)

        with self._connect() as conn:
            # Count completions for this habit within the week
            cnt = conn.execute(
                """
                SELECT COUNT(*) FROM logs
                WHERE habit_id = ? AND kind = 'COMPLETION' AND completed = 1
                AND DATE(log_date) BETWEEN DATE(?) AND DATE(?)
                """,
                (habit.id, week_start.isoformat(), week_end.isoformat()),
            ).fetchone()[0]
            # Has bonus for this week already been awarded?
            already_awarded = conn.execute(
                """
                SELECT COUNT(*) FROM awards
                WHERE name = 'WEEKLY_CONSISTENCY' AND habit_id = ? AND DATE(award_date) BETWEEN DATE(?) AND DATE(?)
                """,
                (habit.id, week_start.isoformat(), week_end.isoformat()),
            ).fetchone()[0]

        if cnt >= habit.frequency_per_week and not already_awarded:
            created_at = datetime.now().isoformat(timespec="seconds")
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO awards (name, habit_id, award_date, points, reason, period_start, period_end, created_at)
                    VALUES ('WEEKLY_CONSISTENCY', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        habit.id,
                        log_day.isoformat(),
                        WEEKLY_CONSISTENCY_BONUS,
                        f"Выполнен недельный план: {cnt}/{habit.frequency_per_week}",
                        week_start.isoformat(),
                        week_end.isoformat(),
                        created_at,
                    ),
                )
                # Also log in logs table for transparency
                conn.execute(
                    """
                    INSERT INTO logs (habit_id, log_date, completed, notes, points, kind, created_at)
                    VALUES (?, ?, 0, ?, ?, 'BONUS', ?)
                    """,
                    (
                        habit.id,
                        log_day.isoformat(),
                        f"Бонус за недельную консистентность ({cnt}/{habit.frequency_per_week})",
                        WEEKLY_CONSISTENCY_BONUS,
                        created_at,
                    ),
                )
                conn.commit()
            return WEEKLY_CONSISTENCY_BONUS
        return 0

    def _check_and_award_badges(self, habit: Habit, log_day: date) -> int:
        # Badges do not grant points directly (we keep points = 0), but are logged for motivation.
        badges_awarded_points = 0

        # Badge 1: 7-day streak
        streak = self._calculate_streak_length(habit.id, log_day)
        if streak == 7:
            self._award_badge(
                habit.id,
                log_day,
                badge_name="STREAK_7",
                reason=f"Серия 7 дней для '{habit.name}'",
            )
        if streak == 30:
            self._award_badge(
                habit.id,
                log_day,
                badge_name="STREAK_30",
                reason=f"Серия 30 дней для '{habit.name}'",
            )

        # Badge 2: 30 total completions
        total_completions = self._get_total_completions(habit.id)
        if total_completions == 30:
            self._award_badge(
                habit.id,
                log_day,
                badge_name="COMPLETE_30",
                reason=f"30 выполнений привычки '{habit.name}'",
            )
        if total_completions == 100:
            self._award_badge(
                habit.id,
                log_day,
                badge_name="COMPLETE_100",
                reason=f"100 выполнений привычки '{habit.name}'",
            )

        return badges_awarded_points

    def _award_badge(self, habit_id: int, when: date, badge_name: str, reason: str) -> None:
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._connect() as conn:
            # Avoid duplicate same-day inserts for same badge
            exists = conn.execute(
                """
                SELECT COUNT(*) FROM awards
                WHERE name = ? AND habit_id = ? AND DATE(award_date) = DATE(?)
                """,
                (badge_name, habit_id, when.isoformat()),
            ).fetchone()[0]
            if exists:
                return
            conn.execute(
                """
                INSERT INTO awards (name, habit_id, award_date, points, reason, period_start, period_end, created_at)
                VALUES (?, ?, ?, 0, ?, NULL, NULL, ?)
                """,
                (badge_name, habit_id, when.isoformat(), reason, created_at),
            )
            conn.execute(
                """
                INSERT INTO logs (habit_id, log_date, completed, notes, points, kind, created_at)
                VALUES (?, ?, 0, ?, 0, 'BADGE', ?)
                """,
                (habit_id, when.isoformat(), f"Награда: {badge_name} — {reason}", created_at),
            )
            conn.commit()

    def _get_total_completions(self, habit_id: int) -> int:
        with self._connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM logs WHERE habit_id = ? AND kind = 'COMPLETION' AND completed = 1",
                (habit_id,),
            ).fetchone()[0]

    # ------------------ Stats & summaries ------------------
    def _get_month_total_points(self, year: int, month: int) -> int:
        start_day, end_day = month_range(year, month)
        with self._connect() as conn:
            p_logs = conn.execute(
                """
                SELECT COALESCE(SUM(points), 0) FROM logs
                WHERE DATE(log_date) BETWEEN DATE(?) AND DATE(?)
                """,
                (start_day.isoformat(), end_day.isoformat()),
            ).fetchone()[0]
            p_awards = conn.execute(
                """
                SELECT COALESCE(SUM(points), 0) FROM awards
                WHERE DATE(award_date) BETWEEN DATE(?) AND DATE(?)
                """,
                (start_day.isoformat(), end_day.isoformat()),
            ).fetchone()[0]
        return int(p_logs)  # points from awards are also echoed into logs as BONUS

    def month_summary(self, year: int, month: int) -> Dict:
        start_day, end_day = month_range(year, month)
        with self._connect() as conn:
            # Points per habit
            rows = conn.execute(
                """
                SELECT h.id, h.name, COALESCE(SUM(l.points), 0) AS pts
                FROM habits h
                LEFT JOIN logs l ON l.habit_id = h.id AND DATE(l.log_date) BETWEEN DATE(?) AND DATE(?)
                GROUP BY h.id, h.name
                ORDER BY pts DESC
                """,
                (start_day.isoformat(), end_day.isoformat()),
            ).fetchall()
            per_habit = [(row["id"], row["name"], int(row["pts"])) for row in rows]
            # Total points
            total = sum(p for (_, _, p) in per_habit)

            # Badges and awards in period
            awards_rows = conn.execute(
                """
                SELECT name, award_date, points, reason FROM awards
                WHERE DATE(award_date) BETWEEN DATE(?) AND DATE(?)
                ORDER BY DATE(award_date) ASC
                """,
                (start_day.isoformat(), end_day.isoformat()),
            ).fetchall()

        top_habit_name = per_habit[0][1] if per_habit else None
        reward_suggestions = suggest_rewards_for_points(total, top_habit_name)
        return {
            "period": f"{year}-{month:02d}",
            "per_habit": per_habit,
            "total_points": total,
            "awards": [dict(r) for r in awards_rows],
            "suggested_rewards": reward_suggestions,
        }

    # ------------------ Weekly review ------------------
    def add_weekly_review(self, text: str, when: Optional[date] = None) -> None:
        when = when or date.today()
        iso_year, iso_week, _ = when.isocalendar()
        week_start = iso_to_monday(iso_year, iso_week)
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO weekly_reviews (week_start_date, text, created_at)
                VALUES (?, ?, ?)
                """,
                (week_start.isoformat(), text, created_at),
            )
            conn.commit()


# ------------------ Helpers ------------------
def month_range(year: int, month: int) -> Tuple[date, date]:
    first = date(year, month, 1)
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last = next_month - timedelta(days=1)
    return first, last


def iso_to_monday(iso_year: int, iso_week: int) -> date:
    # ISO weeks: Monday is day 1
    # The following gives us the Monday of the requested ISO week
    fourth_jan = date(iso_year, 1, 4)
    delta = timedelta(days=fourth_jan.isoweekday() - 1)
    year_start_monday = fourth_jan - delta
    return year_start_monday + timedelta(weeks=iso_week - 1)


def suggest_rewards_for_points(total_points: int, top_habit_name: Optional[str]) -> List[str]:
    habit_suffix = f" (особенно за '{top_habit_name}')" if top_habit_name else ""
    if total_points < 200:
        return [
            "Небольшая пауза и подведение итогов месяца: отметьте прогресс и уточните план на следующий месяц" + habit_suffix,
            "Качественный вечер отдыха без гаджетов",
        ]
    if total_points < 400:
        return [
            "Маленькая награда: любимый кофе/десерт в хорошем месте",
            "Новая цифровая обоина/иконка или мелкая покупка до 10$" + habit_suffix,
        ]
    if total_points < 700:
        return [
            "Средняя награда: новая книга/курс по теме развития",
            "Кино/театр или хобби-покупка до 30$" + habit_suffix,
        ]
    if total_points < 1000:
        return [
            "Крупная награда: день-поездка, СПА или ужин в новом месте",
            "Апгрейд инвентаря для привычки до 70$" + habit_suffix,
        ]
    return [
        "Большая награда: уик-энд-поездка или полноценный мастер‑класс",
        "Существенный апгрейд снаряжения для ключевой привычки" + habit_suffix,
    ]


# ------------------ CLI ------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Дневник привычек с геймификацией и научными техниками: \n"
            "- фокус на мини-действие (Tiny Habits),\n"
            "- намерения формата 'Если‑то' (Implementation Intentions),\n"
            "- очки, серии, недельные бонусы и бейджи,\n"
            "- месячный отчёт с мотивирующими наградами."
        )
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # add-habit
    p_add = sub.add_parser("add-habit", help="Добавить новую привычку")
    p_add.add_argument("name", type=str, help="Короткое название привычки")
    p_add.add_argument("cue", type=str, help="Триггер/подсказка: когда/где/после чего? (‘Если…’) ")
    p_add.add_argument("intention", type=str, help="Намерение: что именно будете делать? (‘…то я…’) ")
    p_add.add_argument("min_action", type=str, help="Минимально жизнеспособное действие (<=2 минуты)")
    p_add.add_argument("difficulty", type=int, choices=[1, 2, 3], help="Сложность: 1=легко, 2=средне, 3=сложно")
    p_add.add_argument(
        "frequency_per_week", type=int, choices=range(1, 8), help="Цель выполнений в неделю (1..7)"
    )

    # list-habits
    p_list = sub.add_parser("list-habits", help="Список привычек")
    p_list.add_argument("--all", action="store_true", help="Показывать и деактивированные")

    # deactivate
    p_deact = sub.add_parser("deactivate", help="Деактивировать привычку")
    p_deact.add_argument("habit", type=str, help="ID или имя привычки")

    # log
    p_log = sub.add_parser("log", help="Отметить выполнение привычки за день и начислить очки")
    p_log.add_argument("habit", type=str, help="ID или имя привычки")
    p_log.add_argument("--date", type=str, help="Дата в формате YYYY-MM-DD (по умолчанию сегодня)")
    p_log.add_argument("--notes", type=str, help="Заметка")

    # month-summary
    p_sum = sub.add_parser("month-summary", help="Месячный отчёт: очки, бейджи, награды")
    p_sum.add_argument("--year", type=int, default=date.today().year)
    p_sum.add_argument("--month", type=int, default=date.today().month)

    # review-week
    p_rev = sub.add_parser("review-week", help="Добавить короткий недельный обзор/рефлексию")
    p_rev.add_argument("--text", type=str, required=True, help="Текст обзора")
    p_rev.add_argument("--date", type=str, help="Любой день той недели, которую хотите отметить")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    diary = HabitDiary()

    if args.command == "add-habit":
        habit_id = diary.add_habit(
            name=args.name.strip(),
            cue=args.cue.strip(),
            intention=args.intention.strip(),
            min_action=args.min_action.strip(),
            difficulty=args.difficulty,
            frequency_per_week=args.frequency_per_week,
        )
        print(f"OK: добавлена привычка #{habit_id} — {args.name}")
        print("Совет: сформулируйте как 'Если [триггер], то я [микродействие] в течение [N минут]'.")
        return 0

    if args.command == "list-habits":
        habits = diary.list_habits(include_inactive=args.all)
        if not habits:
            print("Привычек пока нет. Добавьте первую: add-habit …")
            return 0
        for h in habits:
            status = "активна" if h.is_active else "выключена"
            print(
                f"#{h.id} [{status}] {h.name}\n"
                f"  Если: {h.cue}\n"
                f"  То: {h.intention}\n"
                f"  Мини-действие: {h.min_action}\n"
                f"  Сложность: {h.difficulty} | Цель/нед: {h.frequency_per_week}\n"
                f"  С {h.start_date}"
            )
        return 0

    if args.command == "deactivate":
        diary.deactivate_habit(int(args.habit) if args.habit.isdigit() else diary._get_habit_by_id_or_name(args.habit).id)
        print("OK: привычка деактивирована")
        return 0

    if args.command == "log":
        when = None
        if args.date:
            try:
                when = datetime.strptime(args.date, "%Y-%m-%d").date()
            except ValueError:
                print("Ошибка: дата должна быть в формате YYYY-MM-DD", file=sys.stderr)
                return 2
        gained, month_total = diary.log_completion(args.habit, when, args.notes)
        if gained == 0:
            print("Повторная отметка. Очки за сегодня уже начислены.")
        else:
            print(f"Начислено очков: {gained}")
        print(f"Сумма очков за месяц: {month_total}")
        return 0

    if args.command == "month-summary":
        if not (1 <= args.month <= 12):
            print("Ошибка: месяц 1..12", file=sys.stderr)
            return 2
        summary = diary.month_summary(args.year, args.month)
        print(f"Отчёт за {summary['period']}")
        print("— Очки по привычкам:")
        for _, name, pts in summary["per_habit"]:
            print(f"   · {name}: {pts}")
        print(f"— Итого очков: {summary['total_points']}")
        if summary["awards"]:
            print("— Награды/бейджи:")
            for a in summary["awards"]:
                pts = f" (+{a['points']})" if a["points"] else ""
                print(f"   · {a['name']} {a['award_date']}{pts}: {a['reason']}")
        print("— Предложение наград:")
        for s in summary["suggested_rewards"]:
            print(f"   · {s}")
        return 0

    if args.command == "review-week":
        when = None
        if args.date:
            try:
                when = datetime.strptime(args.date, "%Y-%m-%d").date()
            except ValueError:
                print("Ошибка: дата должна быть в формате YYYY-MM-DD", file=sys.stderr)
                return 2
        diary.add_weekly_review(args.text.strip(), when)
        print("OK: недельный обзор добавлен. Маленькая победа — тоже победа!")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())