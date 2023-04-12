class SQLSchedulerNoDevSchema(Exception):
    """
    Raised when no Dev Schema is provided and the stage is set to dev.
    """


class SQLSchedulerInvalidInsertDirectory(Exception):
    """
    Raised when the given Insert Directory does not exist.
    """


class SQLSchedulerInvalidDDLDirectory(Exception):
    """
    Raised when the given DDL Directory does not exist.
    """


class SQLSchedulerInvalidDependency(Exception):
    """
    Raised when a task has an invalid dependency.
    """


class SQLSchedulerCycleFound(Exception):
    """
    Raised when a cycle found between tasks.
    """


class SQLSchedulerTargetNotFound(Exception):
    """
    Raised when a specified target does not exist.
    """


class SQLSchedulerNoDSN(Exception):
    """
    Raised when no DSN is provided
    """


class SQLSchedulerInvalidStage(Exception):
    """
    Raised when an invalid stage is provided.
    """


class SQLSchedulerInvalidCacheDuration(Exception):
    """
    Raised when an invalid cache duration is provided
    """


class SQLSchedulerInvalidConcurrency(Exception):
    """
    Raised when an invalid concurrency is provided
    """


class SQLSchedulerInvalidIntervalDuration(Exception):
    """
    Raised when an invalid interval duration is given.
    """
