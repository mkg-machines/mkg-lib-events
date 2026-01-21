# mkg-lib-events

Event-Library fuer die MKG Platform. Definiert Event-Schemas (Pydantic), EventBridge-Integration, Event-Publisher und -Consumer Utilities.

## Installation

```bash
pip install mkg-lib-events
```

Fuer Entwicklung:

```bash
pip install mkg-lib-events[dev]
```

## Features

- **Event-Schemas**: Typsichere Pydantic-Models fuer alle Platform-Events
- **Event Registry**: Automatische Deserialisierung basierend auf `event_type`
- **EventBridge-Integration**: Einfache Konvertierung fuer AWS EventBridge
- **Tenant-Isolation**: `tenant_id` als Pflichtfeld in allen Events

## Verwendung

### Event erstellen

```python
from mkg_lib_events import EntityCreatedEvent

event = EntityCreatedEvent(
    tenant_id="tenant-123",
    entity_id="550e8400-e29b-41d4-a716-446655440000",
    entity_type="Article",
    attributes={"name": "Widget", "sku": "WDG-001"},
)
```

### Event serialisieren (fuer EventBridge)

```python
entry = event.to_eventbridge_entry(event_bus_name="mkg-events")
# Verwendung mit boto3:
# client.put_events(Entries=[entry])
```

### Event deserialisieren

```python
from mkg_lib_events import EventRegistry

event_data = {
    "event_type": "entity.created",
    "tenant_id": "tenant-123",
    "entity_id": "550e8400-e29b-41d4-a716-446655440000",
    "entity_type": "Article",
    "attributes": {"name": "Widget"},
}

event = EventRegistry.deserialize(event_data)
# event ist jetzt eine EntityCreatedEvent-Instanz
```

### Eigene Events registrieren

```python
from mkg_lib_events import BaseEvent, register_event
from pydantic import Field

@register_event("workflow.transitioned")
class WorkflowTransitionedEvent(BaseEvent):
    event_type: str = Field(default="workflow.transitioned")
    source: str = Field(default="mkg-extension-workflow")
    entity_id: str = Field(...)
    from_status: str = Field(...)
    to_status: str = Field(...)
```

## Event-Typen

### Entity Events

| Event Type | Klasse | Beschreibung |
|------------|--------|--------------|
| `entity.created` | `EntityCreatedEvent` | Entity wurde erstellt |
| `entity.updated` | `EntityUpdatedEvent` | Entity wurde aktualisiert |
| `entity.deleted` | `EntityDeletedEvent` | Entity wurde geloescht |

### Schema Events

| Event Type | Klasse | Beschreibung |
|------------|--------|--------------|
| `schema.created` | `SchemaCreatedEvent` | Schema wurde erstellt |
| `schema.updated` | `SchemaUpdatedEvent` | Schema wurde aktualisiert |

## Event-Struktur

Alle Events basieren auf `BaseEvent` mit folgenden Feldern:

| Feld | Typ | Beschreibung |
|------|-----|--------------|
| `event_id` | UUID | Eindeutige Event-ID (auto-generiert) |
| `event_type` | str | Event-Typ fuer Routing |
| `source` | str | Quell-Service |
| `tenant_id` | str | Mandanten-ID (Pflicht!) |
| `timestamp` | datetime | Erstellungszeitpunkt (UTC) |
| `version` | str | Schema-Version |
| `data` | dict | Event-spezifische Payload |
| `metadata` | EventMetadata | Tracing-Informationen |

## Entwicklung

### Setup

```bash
git clone git@github.com:mkg-machines/mkg-lib-events.git
cd mkg-lib-events
pip install -e ".[dev]"
```

### Tests ausfuehren

```bash
pytest
```

### Linting

```bash
ruff check .
ruff format .
```

### Type Checking

```bash
mypy src/
```

## Abhaengigkeiten

- `mkg-lib-core>=0.1.0` - Basis-Utilities
- `pydantic>=2.0` - Datenvalidierung
- `boto3>=1.34.0` - AWS SDK

## Lizenz

MIT
