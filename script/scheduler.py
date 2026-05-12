"""
Планировщик ETL. Запускается как постоянный сервис (docker-compose up -d).
Ежедневно в 02:00 по московскому времени запускает metrika_loader.py.

Мониторинг лога во время прогона:
  - [CRITICAL] → немедленная остановка загрузчика
  - [WARNING]  → счётчик; при превышении MAX_WARNINGS → остановка

Для разового ручного запуска используйте:
    docker-compose run --rm loader
"""
import re
import subprocess
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

LOG_FILE    = "/app/data/scheduler.log"
MAX_ERRORS  = 5  # порог [ERROR] строк; [WARNING] — операционный шум (rate limits и т.п.)

_CRITICAL_RE = re.compile(r'\[CRITICAL\]')
_ERROR_RE    = re.compile(r'\[ERROR\]')


def _log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [SCHEDULER] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def run_loader() -> None:
    _log(f"Запуск ETL. Порог ERROR: {MAX_ERRORS} (WARNING игнорируется — операционный шум).")

    proc = subprocess.Popen(
        [sys.executable, "/app/metrika_loader.py"],
        cwd="/app",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    error_count  = 0
    abort_reason = None

    for line in proc.stdout:
        print(line, end="", flush=True)

        if _CRITICAL_RE.search(line):
            abort_reason = "обнаружена CRITICAL-ошибка"
            break

        if _ERROR_RE.search(line):
            error_count += 1
            if error_count >= MAX_ERRORS:
                abort_reason = f"превышен порог ошибок ({MAX_ERRORS})"
                break

    if abort_reason:
        _log(f"ABORT: {abort_reason} — останавливаем загрузчик.")
        proc.terminate()
        try:
            proc.communicate(timeout=15)  # дренируем pipe + ждём выхода
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
    else:
        proc.wait()

    if abort_reason:
        _log(f"Загрузчик прерван. Errors: {error_count}. Exit code: {proc.returncode}.")
    elif proc.returncode == 0:
        _log(f"ETL завершён успешно. Errors: {error_count}.")
    else:
        _log(f"ETL завершился с ошибкой. Exit code: {proc.returncode}. Errors: {error_count}.")


if __name__ == "__main__":
    _log("Планировщик запущен. Расписание: ежедневно в 02:00 МСК.")

    scheduler = BlockingScheduler(timezone="Europe/Moscow")
    scheduler.add_job(run_loader, "cron", hour=2, minute=0)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        _log("Планировщик остановлен.")
