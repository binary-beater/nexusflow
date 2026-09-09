from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    """Abstract time authority for deterministic testing."""
    def now_utc(self) -> datetime:
        """Returns timezone-aware UTC datetime. Raises ValueError if naive."""
        ...

class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(UTC)

class FakeClock:
    def __init__(self, initial_time: datetime | None = None) -> None:
        if initial_time is not None:
            if initial_time.tzinfo is None:
                raise ValueError("Initial time must be timezone-aware.")
            self._current_time = initial_time
        else:
            self._current_time = datetime.now(UTC)

    def now_utc(self) -> datetime:
        return self._current_time

    def set_time(self, new_time: datetime) -> None:
        if new_time.tzinfo is None:
            raise ValueError("New time must be timezone-aware.")
        self._current_time = new_time

    def advance(self, seconds: float) -> None:
        from datetime import timedelta
        self._current_time += timedelta(seconds=seconds)
