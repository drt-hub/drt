"""Chainable post-processing for events emitted by dagster-drt."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, TypeAlias, TypeVar, cast

from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    MaterializeResult,
    OpExecutionContext,
)
from dagster._annotations import public
from dagster._core.definitions.metadata.metadata_set import TableMetadataSet

DrtEventType: TypeAlias = AssetMaterialization | MaterializeResult[None]
T = TypeVar("T", bound=DrtEventType)


class DrtEventIterator(Iterator[T]):
    """Wrap drt's Dagster events with chainable post-processing methods."""

    def __init__(
        self,
        events: Iterator[T],
        context: AssetExecutionContext | OpExecutionContext,
        row_count_fetcher: Callable[[T], int],
    ) -> None:
        self._inner_iterator = events
        self._context = context
        self._row_count_fetcher = row_count_fetcher

    def __next__(self) -> T:
        return next(self._inner_iterator)

    def __iter__(self) -> DrtEventIterator[T]:
        return self

    @public
    def fetch_row_count(self) -> DrtEventIterator[T]:
        """Fetch and attach the current source-side row count for each sync.

        This independently re-runs the resolved drt model against its source.
        It does not derive the value from ``rows_extracted`` or ``rows_synced``
        metadata produced by the sync run.
        """

        def _events_with_row_count() -> Iterator[T]:
            for event in self:
                try:
                    row_count = self._row_count_fetcher(event)
                    row_count_metadata = TableMetadataSet(row_count=row_count)
                except Exception as exc:  # noqa: BLE001 - optional post-processing
                    self._context.log.error(
                        "An error occurred while fetching the source row count; "
                        "row count metadata will not be included in the event.\n\n"
                        f"Exception: {exc}"
                    )
                    row_count_metadata = TableMetadataSet(row_count=None)

                metadata: dict[str, Any] = {
                    **row_count_metadata,
                    **(event.metadata or {}),
                }
                yield cast(
                    T,
                    event._replace(metadata=metadata),
                )

        return DrtEventIterator(
            _events_with_row_count(),
            context=self._context,
            row_count_fetcher=self._row_count_fetcher,
        )
