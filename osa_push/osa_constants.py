STATUS_OPS = ('ACTIVE', 'DELETED', 'REJECTED', 'FINISHED', 'ABSTRACT')

WHERE_SCHEMA = {
    "notification_enabled": {"type": "boolean", "required": True},
    "channels": {"type": "string", "required": True},
    "role_id": {"type": "integer"},
    "team_id": {"type": "integer"}
}

DATA_SCHEMA = {
    "title": {"type": "string", "required": True},  # Titulo mensaje
    "alert": {"type": "string", "required": True},  # Descripcion
    "status": {"type": "string", "required": True},  # Estado
    "task": {"type": "string", "required": True},  # Titulo tarea
    "user": {"type": "integer", "required": True},  # Quien ejecuto
    "team_id": {"type": "integer", "required": True},  # Id
    "team": {"type": "integer", "required": True}  # Llave agrupar
}

KEYS_PUSH = ["where", "data"]
