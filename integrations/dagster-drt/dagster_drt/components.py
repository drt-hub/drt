"""Dagster Components integration for declarative drt sync assets."""

import hashlib
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Annotated

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    Component,
    ComponentLoadContext,
    Resolvable,
    Resolver,
    Scaffolder,
    ScaffoldRequest,
    scaffold_component,
)
from dagster._annotations import public
from dagster.components.resolved.context import ResolutionContext
from dagster.components.scaffold.scaffold import scaffold_with
from pydantic import BaseModel

from dagster_drt.assets import drt_assets
from dagster_drt.event_iterator import DrtEventType
from dagster_drt.resource import DagsterDrtResource
from dagster_drt.translator import DagsterDrtTranslator, DrtTranslatorData

_INVALID_OP_NAME_CHARS = re.compile(r"[^A-Za-z0-9_]")


def _resolve_project_dir(context: ResolutionContext, project_dir: str) -> str:
    """Resolve component paths relative to the declaring ``defs.yaml``."""
    return str(context.resolve_source_relative_path(project_dir))


@dataclass
class DrtSyncTranslation(Resolvable):
    """Declarative AssetSpec overrides supported by ``DrtSyncComponent``."""

    group_name: str | None = None


class DrtSyncScaffolderParams(BaseModel):
    project_dir: str = "."
    sync_names: list[str] | None = None
    group_name: str | None = None


class DrtSyncComponentScaffolder(Scaffolder[DrtSyncScaffolderParams]):
    """Scaffold a ready-to-edit ``DrtSyncComponent`` ``defs.yaml``."""

    @classmethod
    def get_scaffold_params(cls) -> type[DrtSyncScaffolderParams]:
        return DrtSyncScaffolderParams

    def scaffold(self, request: ScaffoldRequest[DrtSyncScaffolderParams]) -> None:
        scaffold_dir = request.target_path.parent if request.append else request.target_path
        scaffold_dir.mkdir(parents=True, exist_ok=True)
        attributes: dict[str, object] = {
            "project_dir": request.params.project_dir,
        }
        if request.params.sync_names is not None:
            attributes["sync_names"] = request.params.sync_names
        if request.params.group_name is not None:
            attributes["translation"] = {"group_name": request.params.group_name}
        scaffold_component(request=request, yaml_attributes=attributes)


@public
@scaffold_with(DrtSyncComponentScaffolder)
@dataclass
class DrtSyncComponent(Component, Resolvable):
    """Expose a drt project as assets through a declarative ``defs.yaml``.

    Example::

        type: dagster_drt.DrtSyncComponent
        attributes:
          project_dir: path/to/drt-project
          sync_names: [customers_to_crm]
          translation:
            group_name: reverse_etl
    """

    project_dir: Annotated[
        str,
        Resolver(
            _resolve_project_dir,
            model_field_type=str,
            description="Path to the drt project, relative to this defs.yaml file.",
        ),
    ]
    sync_names: Sequence[str] | None = None
    translation: DrtSyncTranslation | None = None

    @cached_property
    def drt_resource(self) -> DagsterDrtResource:
        return DagsterDrtResource(project_dir=self.project_dir)

    @cached_property
    def _base_translator(self) -> DagsterDrtTranslator:
        return DagsterDrtTranslator()

    @public
    def get_asset_spec(self, data: DrtTranslatorData) -> AssetSpec:
        """Build an AssetSpec for a sync; subclasses may override this hook."""
        spec = self._base_translator.get_asset_spec(data)
        if self.translation and self.translation.group_name is not None:
            spec = spec.replace_attributes(group_name=self.translation.group_name)
        return spec

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        translator = _DrtSyncComponentTranslator(self)
        component_key = context.component_path.get_relative_key(context.defs_module_path)
        component_slug = _INVALID_OP_NAME_CHARS.sub("_", component_key).strip("_")
        component_digest = hashlib.sha256(component_key.encode()).hexdigest()[:12]
        op_name = f"drt_sync_assets_{component_slug or 'root'}_{component_digest}"

        @drt_assets(
            project_dir=self.project_dir,
            sync_names=list(self.sync_names) if self.sync_names is not None else None,
            dagster_drt_translator=translator,
            name=op_name,
        )
        def drt_sync_assets(context: AssetExecutionContext) -> Iterator[DrtEventType]:
            yield from self.execute(context, self.drt_resource)

        return dg.Definitions(assets=[drt_sync_assets])

    @public
    def execute(
        self,
        context: AssetExecutionContext,
        drt_resource: DagsterDrtResource,
    ) -> Iterator[DrtEventType]:
        """Execute selected drt assets; subclasses may override this hook."""
        yield from drt_resource.run(context=context)


class _DrtSyncComponentTranslator(DagsterDrtTranslator):
    def __init__(self, component: DrtSyncComponent) -> None:
        self._component = component

    def get_asset_spec(self, data: DrtTranslatorData) -> AssetSpec:
        return self._component.get_asset_spec(data)
