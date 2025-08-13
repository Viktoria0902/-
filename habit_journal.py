#!/usr/bin/env python3

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date as date_class, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

__version__ = "1.0.0"


@dataclass
class Badge:
    code: str
    title: str
    description: str
    awarded_at: str
    habit_id: Optional[int]
    points_at_award: int


class HabitJournal:
    def __init__(self, db_path: str) -> None:
        expanded = os.path.expanduser(db_path)
        parent_dir = os.path.dirname(expanded)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        self.connection = sqlite3.connect(expanded)
        self.connection.row_factory = sqlite3.Row
        self._enable_foreign_keys()
        self.ensure_schema()

    def _enable_foreign_keys(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        self.connection.commit()

    def ensure_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS habits (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                description TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY,
                habit_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 1,
                points_awarded INTEGER NOT NULL DEFAULT 0,
                streak_count INTEGER NOT NULL DEFAULT 1,
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(habit_id, date),
                FOREIGN KEY(habit_id) REFERENCES habits(id) ON DELETE CASCADE
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS badges (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                awarded_at TEXT NOT NULL,
                habit_id INTEGER,
                points_at_award INTEGER NOT NULL,
                UNIQUE(code, habit_id),
                FOREIGN KEY(habit_id) REFERENCES habits(id) ON DELETE SET NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        self.connection.commit()

    def add_habit(self, name: str, description: str = "") -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO habits (name, description) VALUES (?, ?)",
            (name.strip(), description.strip()),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def list_habits(self, active_only: bool = True) -> List[sqlite3.Row]:
        cursor = self.connection.cursor()
        if active_only:
            cursor.execute(
                "SELECT id, name, description, created_at, is_active FROM habits WHERE is_active = 1 ORDER BY name"
            )
        else:
            cursor.execute(
                "SELECT id, name, description, created_at, is_active FROM habits ORDER BY name"
            )
        return list(cursor.fetchall())

    def get_habit_by_name(self, name: str) -> Optional[sqlite3.Row]:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT id, name, description, created_at, is_active FROM habits WHERE name = ?",
            (name.strip(),),
        )
        row = cursor.fetchone()
        return row

    def _get_total_points(self) -> int:
        cursor = self.connection.cursor()
        cursor.execute("SELECT COALESCE(SUM(points_awarded), 0) AS total FROM logs;")
        row = cursor.fetchone()
        return int(row["total"] or 0)

    def _get_total_points_until(self, inclusive_date: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COALESCE(SUM(points_awarded), 0) AS total FROM logs WHERE date <= ?;",
            (inclusive_date,),
        )
        row = cursor.fetchone()
        return int(row["total"] or 0)

    def _count_logs_on_date(self, iso_date: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM logs WHERE date = ?;", (iso_date,))
        return int(cursor.fetchone()["cnt"])

    def _get_previous_day_streak(self, habit_id: int, iso_date: str) -> int:
        prev = (parse_date(iso_date) - timedelta(days=1)).isoformat()
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT streak_count FROM logs WHERE habit_id = ? AND date = ?",
            (habit_id, prev),
        )
        row = cursor.fetchone()
        if row is None:
            return 0
        return int(row["streak_count"])  # streak ending yesterday

    def _award_badge(self, code: str, title: str, description: str, habit_id: Optional[int]) -> Optional[Badge]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "INSERT INTO badges (code, title, description, awarded_at, habit_id, points_at_award) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    code,
                    title,
                    description,
                    datetime.utcnow().isoformat(timespec="seconds"),
                    habit_id,
                    self._get_total_points(),
                ),
            )
            self.connection.commit()
            return Badge(
                code=code,
                title=title,
                description=description,
                awarded_at=datetime.utcnow().isoformat(timespec="seconds"),
                habit_id=habit_id,
                points_at_award=self._get_total_points(),
            )
        except sqlite3.IntegrityError:
            return None

    def _maybe_award_points_threshold_badges(self) -> List[Badge]:
        thresholds = [100, 500, 1000, 2000, 5000]
        awarded: List[Badge] = []
        total_points = self._get_total_points()
        cursor = self.connection.cursor()
        for threshold in thresholds:
            code = f"POINTS_{threshold}"
            cursor.execute("SELECT 1 FROM badges WHERE code = ? AND habit_id IS NULL", (code,))
            exists = cursor.fetchone() is not None
            if not exists and total_points >= threshold:
                badge = self._award_badge(
                    code=code,
                    title=f"Достигнут порог {threshold} очков",
                    description=f"Вы заработали как минимум {threshold} очков. Держите темп!",
                    habit_id=None,
                )
                if badge:
                    awarded.append(badge)
        return awarded

    def _maybe_award_first_step_badge(self) -> Optional[Badge]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM logs;")
        count_logs = int(cursor.fetchone()["cnt"])
        if count_logs == 1:
            return self._award_badge(
                code="FIRST_STEP",
                title="Первый шаг",
                description="Первое выполнение привычки — начало пути!",
                habit_id=None,
            )
        return None

    def _maybe_award_streak_badge(self, habit_id: int, habit_name: str, streak_count: int) -> Optional[Badge]:
        streak_thresholds = [3, 7, 14, 30, 60, 100]
        if streak_count in streak_thresholds:
            code = f"STREAK_{streak_count}"
            title = f"Серия {streak_count}"
            description = f"{habit_name}: серия из {streak_count} дней подряд"
            return self._award_badge(code, title, description, habit_id)
        return None

    def log_completion(self, habit_name: str, iso_date: str, note: str = "") -> Tuple[int, int, List[Badge]]:
        habit = self.get_habit_by_name(habit_name)
        if habit is None:
            raise ValueError(f"Привычка '{habit_name}' не найдена. Добавьте её командой add-habit.")

        habit_id = int(habit["id"])
        if not is_valid_iso_date(iso_date):
            raise ValueError("Неверный формат даты, используйте YYYY-MM-DD")

        cursor = self.connection.cursor()
        try:
            prev_streak = self._get_previous_day_streak(habit_id, iso_date)
            current_streak = prev_streak + 1 if prev_streak > 0 else (1 if self._has_log_on_date(habit_id, (parse_date(iso_date) - timedelta(days=1)).isoformat()) else 1)

            base_points = 10
            streak_bonus = min(max(current_streak - 1, 0) * 2, 20)
            prior_completions_today = self._count_logs_on_date(iso_date)
            combo_bonus = 5 if prior_completions_today >= 2 else 0
            awarded_points = base_points + streak_bonus + combo_bonus

            cursor.execute(
                "INSERT INTO logs (habit_id, date, completed, points_awarded, streak_count, note) VALUES (?, ?, 1, ?, ?, ?)",
                (habit_id, iso_date, awarded_points, current_streak, note.strip()),
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            raise ValueError("Запись на эту дату уже существует для данной привычки")

        earned: List[Badge] = []
        first_step = self._maybe_award_first_step_badge()
        if first_step:
            earned.append(first_step)
        streak_badge = self._maybe_award_streak_badge(habit_id, habit["name"], current_streak)
        if streak_badge:
            earned.append(streak_badge)
        earned.extend(self._maybe_award_points_threshold_badges())

        return awarded_points, current_streak, earned

    def _has_log_on_date(self, habit_id: int, iso_date: str) -> bool:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT 1 FROM logs WHERE habit_id = ? AND date = ?",
            (habit_id, iso_date),
        )
        return cursor.fetchone() is not None

    def list_badges(self) -> List[Badge]:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT code, title, description, awarded_at, habit_id, points_at_award FROM badges ORDER BY datetime(awarded_at) ASC"
        )
        result: List[Badge] = []
        for row in cursor.fetchall():
            result.append(
                Badge(
                    code=row["code"],
                    title=row["title"],
                    description=row["description"],
                    awarded_at=row["awarded_at"],
                    habit_id=row["habit_id"],
                    points_at_award=row["points_at_award"],
                )
            )
        return result

    def get_day_status(self, iso_date: str) -> List[Dict[str, object]]:
        habits = self.list_habits(active_only=True)
        status_rows: List[Dict[str, object]] = []
        for habit in habits:
            habit_id = int(habit["id"])
            done = self._has_log_on_date(habit_id, iso_date)
            streak = self._compute_current_streak_up_to(habit_id, iso_date)
            status_rows.append(
                {
                    "habit_id": habit_id,
                    "name": habit["name"],
                    "done": done,
                    "current_streak": streak,
                }
            )
        return status_rows

    def _compute_current_streak_up_to(self, habit_id: int, iso_date: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT date FROM logs WHERE habit_id = ? AND date <= ? ORDER BY date DESC",
            (habit_id, iso_date),
        )
        rows = [r["date"] for r in cursor.fetchall()]
        if not rows:
            return 0
        target = parse_date(iso_date)
        streak = 0
        current_day = target
        dates_set = set(rows)
        while current_day.isoformat() in dates_set:
            streak += 1
            current_day = current_day - timedelta(days=1)
        return streak

    def month_report(self, year: int, month: int) -> Dict[str, object]:
        start = date_class(year, month, 1)
        if month == 12:
            end = date_class(year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date_class(year, month + 1, 1) - timedelta(days=1)
        start_iso = start.isoformat()
        end_iso = end.isoformat()

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT COALESCE(SUM(points_awarded), 0) AS points,
                   COUNT(*) AS completions
            FROM logs WHERE date BETWEEN ? AND ?;
            """,
            (start_iso, end_iso),
        )
        row = cursor.fetchone()
        points = int(row["points"] or 0)
        completions = int(row["completions"] or 0)

        cursor.execute(
            """
            SELECT h.name AS habit_name, COUNT(l.id) AS cnt, COALESCE(SUM(l.points_awarded), 0) AS pts
            FROM logs l
            JOIN habits h ON h.id = l.habit_id
            WHERE l.date BETWEEN ? AND ?
            GROUP BY h.id
            ORDER BY cnt DESC, pts DESC
            LIMIT 5;
            """,
            (start_iso, end_iso),
        )
        top_habits = [
            {"habit": r["habit_name"], "completions": int(r["cnt"]), "points": int(r["pts"]) }
            for r in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT MAX(streak_count) AS best_streak
            FROM logs
            WHERE date BETWEEN ? AND ?;
            """,
            (start_iso, end_iso),
        )
        best_streak = int((cursor.fetchone()["best_streak"] or 0))

        cursor.execute(
            "SELECT COUNT(DISTINCT habit_id) AS uniq FROM logs WHERE date BETWEEN ? AND ?;",
            (start_iso, end_iso),
        )
        unique_habits = int(cursor.fetchone()["uniq"] or 0)

        reward_tier, reward_text = self._monthly_reward(points)

        return {
            "period": f"{year:04d}-{month:02d}",
            "points": points,
            "completions": completions,
            "unique_habits": unique_habits,
            "best_streak": best_streak,
            "top_habits": top_habits,
            "reward_tier": reward_tier,
            "reward_text": reward_text,
        }

    def _monthly_reward(self, points: int) -> Tuple[str, str]:
        if points >= 1000:
            return (
                "Платина",
                "Крупная награда: устроить путешествие/долгую прогулку, реализовать давнюю мечту",
            )
        if points >= 600:
            return (
                "Золото",
                "Существенная награда: поход в кафе/ресторан, обновка, впечатление",
            )
        if points >= 300:
            return (
                "Серебро",
                "Мини-награда: фильм, книга, расслабляющий вечер без дел",
            )
        if points >= 100:
            return (
                "Бронза",
                "Маленькая радость: любимый кофе/чай, сладость, мини-покупка",
            )
        return (
            "Старт",
            "Продолжайте: поставьте маленькую цель и отметьте прогресс небольшой паузой",
        )


def parse_date(s: Optional[str]) -> date_class:
    if s is None or s.strip() == "":
        return date_class.today()
    try:
        parts = [int(p) for p in s.split("-")]
        if len(parts) != 3:
            raise ValueError
        return date_class(parts[0], parts[1], parts[2])
    except Exception as exc:  # noqa: BLE001
        raise ValueError("Ожидается дата в формате YYYY-MM-DD") from exc


def is_valid_iso_date(s: str) -> bool:
    try:
        parse_date(s)
        return True
    except Exception:  # noqa: BLE001
        return False


def print_habits(habits: Iterable[sqlite3.Row]) -> None:
    print("Список привычек:")
    for h in habits:
        mark = "✅" if h["is_active"] else "⏸"
        print(f" - {mark} {h['name']} — {h['description']}")


def print_day_status(status_rows: List[Dict[str, object]], iso_date: str) -> None:
    print(f"Статус на {iso_date}:")
    for row in status_rows:
        mark = "✅" if row["done"] else "▫️"
        streak = int(row["current_streak"])
        streak_str = f" (серия {streak})" if streak > 0 else ""
        print(f" - {mark} {row['name']}{streak_str}")


def print_badges(badges: List[Badge]) -> None:
    if not badges:
        print("Бейджи пока не получены. Всё впереди!")
        return
    print("Заработанные бейджи:")
    for b in badges:
        scope = "(общий)" if b.habit_id is None else f"(привычка #{b.habit_id})"
        print(f" - {b.title} {scope}: {b.description} — {b.awarded_at}")


def print_month_report(rep: Dict[str, object]) -> None:
    print(f"Отчёт за {rep['period']}:")
    print(f" - Очки: {rep['points']}")
    print(f" - Выполнений: {rep['completions']}")
    print(f" - Уникальных привычек: {rep['unique_habits']}")
    print(f" - Лучшая серия: {rep['best_streak']}")
    if rep["top_habits"]:
        print(" - Топ привычек:")
        for item in rep["top_habits"]:
            print(f"    • {item['habit']}: {item['completions']} вып., {item['points']} очков")
    print(f" - Награда: {rep['reward_tier']} — {rep['reward_text']}")


def build_parser() -> argparse.ArgumentParser:
    default_db = os.path.expanduser("~/.habit_journal.db")
    parser = argparse.ArgumentParser(
        prog="habit_journal",
        description=(
            "Консольный дневник привычек с геймификацией: очки, серии, бонусы, бейджи и месячные отчёты."
        ),
    )
    parser.add_argument(
        "--db",
        default=default_db,
        help=f"Путь к базе данных SQLite (по умолчанию {default_db})",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"habit_journal {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    sp_init = subparsers.add_parser("init", help="Инициализировать хранилище")
    sp_init.set_defaults(func=cmd_init)

    sp_add = subparsers.add_parser("add-habit", help="Добавить новую привычку")
    sp_add.add_argument("name", help="Название привычки")
    sp_add.add_argument("-d", "--description", default="", help="Описание")
    sp_add.set_defaults(func=cmd_add_habit)

    sp_list = subparsers.add_parser("list-habits", help="Список активных привычек")
    sp_list.set_defaults(func=cmd_list_habits)

    sp_log = subparsers.add_parser("log", help="Отметить выполнение привычки")
    sp_log.add_argument("name", help="Название привычки")
    sp_log.add_argument("--date", default=None, help="Дата выполнения YYYY-MM-DD (по умолчанию сегодня)")
    sp_log.add_argument("--note", default="", help="Комментарий к выполнению")
    sp_log.set_defaults(func=cmd_log)

    sp_status = subparsers.add_parser("status", help="Статус на указанную дату (по умолчанию сегодня)")
    sp_status.add_argument("--date", default=None, help="Дата YYYY-MM-DD")
    sp_status.set_defaults(func=cmd_status)

    sp_badges = subparsers.add_parser("badges", help="Показать заработанные бейджи")
    sp_badges.set_defaults(func=cmd_badges)

    sp_report = subparsers.add_parser("report-month", help="Месячный отчёт и награда")
    sp_report.add_argument("--month", default=None, help="Месяц в формате YYYY-MM (по умолчанию текущий)")
    sp_report.set_defaults(func=cmd_report_month)

    return parser


def cmd_init(args: argparse.Namespace) -> None:
    HabitJournal(args.db)
    print(f"База данных готова: {os.path.expanduser(args.db)}")


def cmd_add_habit(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    habit_id = hj.add_habit(args.name, args.description)
    print(f"Добавлена привычка #{habit_id}: {args.name}")


def cmd_list_habits(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    habits = hj.list_habits(active_only=True)
    print_habits(habits)


def cmd_log(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    iso_date = parse_date(args.date).isoformat()
    try:
        points, streak, badges = hj.log_completion(args.name, iso_date, note=args.note)
    except ValueError as e:
        print(f"Ошибка: {e}")
        sys.exit(2)
    print(f"Готово: +{points} очков, серия теперь {streak}")
    if badges:
        print("Новые бейджи:")
        for b in badges:
            print(f" - {b.title}: {b.description}")


def cmd_status(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    iso_date = parse_date(args.date).isoformat()
    rows = hj.get_day_status(iso_date)
    print_day_status(rows, iso_date)


def cmd_badges(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    badges = hj.list_badges()
    print_badges(badges)


def cmd_report_month(args: argparse.Namespace) -> None:
    hj = HabitJournal(args.db)
    if args.month:
        try:
            year, month = [int(p) for p in args.month.split("-")]
        except Exception as exc:  # noqa: BLE001
            raise SystemExit("Ожидается формат YYYY-MM") from exc
    else:
        today = date_class.today()
        year, month = today.year, today.month
    rep = hj.month_report(year, month)
    print_month_report(rep)


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except KeyboardInterrupt:
        print("Прервано пользователем")
        return 130


if __name__ == "__main__":
    sys.exit(main())