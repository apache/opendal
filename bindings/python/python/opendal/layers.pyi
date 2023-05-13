class ConcurrentLimitLayer:
    def __init__(self, permits: int) -> None: ...

class ImmutableIndexLayer:
    def insert(self, key: str) -> None: ...

class RetryLayer:
    def __init__(
        self,
        max_times: int | None = None,
        factor: float | None = None,
        jitter: bool = False,
        max_delay: float | None = None,
        min_delay: float | None = None,
    ) -> None: ...
