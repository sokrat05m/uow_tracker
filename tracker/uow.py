from copy import copy

from sqlalchemy.ext.asyncio import AsyncConnection

from tracker.mappers import Registry
from tracker.models import DomainEntity


class EntityChangeTracker:
    def __init__(self):
        self.new_entities = {}
        self.modified_entities = {}
        self.deleted_entities = {}
        self.entity_snapshots = {}

    def register_new(self, entity: DomainEntity) -> None:
        self.new_entities.setdefault(type(entity), []).append(entity)

    def take_snapshot(self, entity: DomainEntity) -> None:
        self.entity_snapshots[(type(entity), entity.id)] = (
            entity,
            copy(entity.__dict__),
        )
        for value in entity.__dict__.values():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, DomainEntity):
                        self.take_snapshot(item)
            elif isinstance(value, DomainEntity):
                self.take_snapshot(value)

    def collect_changes(self, entity: DomainEntity) -> None:
        _, orig_state = self.entity_snapshots[(type(entity), entity.id)]
        curr_state = copy(entity.__dict__)

        for attr_name, curr_value in curr_state.items():
            orig_value = orig_state[attr_name]
            self.compare_values(orig_value, curr_value, entity)

    def compare_values(self, orig, curr, parent: DomainEntity) -> None:
        if isinstance(curr, DomainEntity):
            self.collect_changes(curr)

        elif isinstance(curr, list):
            for o, c in zip(orig, curr):
                if isinstance(o, DomainEntity):
                    self.collect_changes(c)
                else:
                    if orig != curr and parent not in self.modified_entities.get(
                        type(parent), []
                    ):
                        self.modified_entities.setdefault(type(parent), []).append(
                            parent
                        )

        else:
            if orig != curr and parent not in self.modified_entities.get(
                type(parent), []
            ):
                self.modified_entities.setdefault(type(parent), []).append(parent)


class UnitOfWork:
    def __init__(self, connection: AsyncConnection, mapper_registry: Registry):
        self._change_tracker = EntityChangeTracker()
        self._connection = connection
        self._mapper_registry = mapper_registry

    def register_new(self, entity: DomainEntity) -> None:
        self._change_tracker.register_new(entity)

    def register_existing(self, entity: DomainEntity) -> None:
        self._change_tracker.take_snapshot(entity)

    def register_deleted(self, entity):
        self._change_tracker.deleted_entities.setdefault(type(entity), []).append(
            entity
        )

    async def commit(self):
        for entity, _ in self._change_tracker.entity_snapshots.values():
            self._change_tracker.collect_changes(entity)

        for entity_type, entities in self._change_tracker.new_entities.items():
            mapper = self._mapper_registry.get(entity_type)
            await mapper.delete(entities)

        for entity_type, entities in self._change_tracker.modified_entities.items():
            mapper = self._mapper_registry.get(entity_type)
            await mapper.update(entities)

        for entity_type, entities in self._change_tracker.deleted_entities.items():
            mapper = self._mapper_registry.get(entity_type)
            await mapper.delete(entities)

        await self._connection.commit()

        self._change_tracker = EntityChangeTracker()
