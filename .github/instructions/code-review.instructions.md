---
applyTo: "**"
excludeAgent: "coding-agent"
---

## Code Quality Rules

* **Flag any assert in non-test code** - Assert statements should only be used in test code. In production code, they can be disabled with the `-O` flag, which can lead to unexpected behavior if relied upon for critical checks.
* **Flag any time.time() used for measuring durations** - Using `time.time()` for measuring durations can lead to inaccurate results due to system clock adjustments. Use `time.perf_counter()` or `time.monotonic()` instead.
* **Flag any from or import statement inside a function or method body** - Import statements should be at the top of the file to improve readability and maintainability. Importing inside functions can lead to unexpected behavior and make it harder to track dependencies. The only exceptions to this rule are (1) circular import avoidance, (2) `TYPE_CHECKING` imports for type hints.
* **Flag any @lru_cache(maxsize=None)** - Using `@lru_cache(maxsize=None)` can lead to unbounded memory usage if the cache grows indefinitely. It's important to set a reasonable `maxsize` to prevent potential memory issues.

## Testing Requirements

* **Require tests for any new code** - All new code should be accompanied by appropriate tests to ensure its correctness and maintainability. This includes unit tests, integration tests, and any other relevant testing methodologies.
* **Flag any mock.Mock() or mock.MagicMock() without spec or autospec** - Using `mock.Mock()` or `mock.MagicMock()` without specifying `spec` or `autospec` can lead to tests that pass even when the code being tested is incorrect. Specifying `spec` or `autospec` helps ensure that the mock objects behave more like the real objects they are replacing, leading to more accurate and reliable tests.
* **Flag any issue number in test docstrings (e.g., """Fix for #12345""")** - Test docstrings should describe the purpose of the test and the expected behavior, rather than referencing specific issue numbers. This helps maintain clarity and relevance of the test documentation over time, as issue numbers may become outdated or irrelevant.
